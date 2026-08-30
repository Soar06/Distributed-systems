package rpc

import (
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/storage"
)

// End-to-end tests for a SHARDED cluster over real TCP (G1).
//
// The distinction from sim/ matters and is the reason this file exists. sim/
// proved the 2PC protocol against a simulated network with every shard in one
// process. These tests run several independent Raft groups over one real
// listener and one real connection per peer — the multiplexing described in
// learn/READING_LIST.md §14 — which is the configuration a deployment actually
// uses.
//
// Per RULES.md rule 3 the flows covered are: normal (money moves cross-shard),
// failure (a shard's leader killed mid-flight; a node hosting no replica of the
// target shard), concurrent (two clients against the same account), and retry
// (the same idempotency key delivered twice). Money conservation is asserted
// across every one of them.

// shardTestNode is one process-equivalent: a host with its replicas and its own
// coordinator view of the ring.
type shardTestNode struct {
	id     raft.NodeID
	host   *ShardHost
	client *ShardClientService
	coord  *shard.Coordinator
	trans  *Transport
	addr   string
}

// shardTestCluster is nNodes processes, each hosting a replica of every shard.
//
// Every node holding a replica of every shard is the simplest real topology and
// the one that exercises multiplexing hardest: each process runs nShards Raft
// groups over a single shared connection per peer.
type shardTestCluster struct {
	t      *testing.T
	nodes  map[raft.NodeID]*shardTestNode
	ids    []raft.NodeID
	shards []shard.ID
	ring   *shard.Ring
}

// startShardCluster builds nNodes processes, each hosting a replica of EVERY
// shard — the co-located topology CockroachDB uses, where a node carries many
// Ranges.
func startShardCluster(t *testing.T, nNodes, nShards int) *shardTestCluster {
	t.Helper()
	return startShardClusterPlaced(t, nNodes, nShards, false)
}

// startShardClusterDisjoint gives every shard its OWN nNodes nodes, so no
// process hosts two shards.
//
// This exists to separate two variables the co-located topology conflates. There,
// adding a shard adds a Raft group to every existing node — more tickers, more
// heartbeats, more goroutines on the same machines — so a throughput change could
// be capacity scaling OR per-node overhead. Disjoint placement adds hardware along
// with the shard, which is what "sharding adds capacity" actually claims.
func startShardClusterDisjoint(t *testing.T, nPerShard, nShards int) *shardTestCluster {
	t.Helper()
	return startShardClusterPlaced(t, nPerShard, nShards, true)
}

// startShardClusterTimed is startShardClusterDisjoint with explicit Raft timings.
//
// Needed by the throughput benchmark: with many Raft groups and many client
// goroutines on one dev box, LAN-tuned election timeouts fire because goroutines
// are not scheduled in time, not because a leader failed. That is a §5.2 timing
// violation induced by CPU starvation, and it makes a capacity measurement
// measure elections instead.
func startShardClusterTimed(t *testing.T, nPerShard, nShards int, cfg raft.Config) *shardTestCluster {
	t.Helper()
	return startShardClusterCfg(t, nPerShard, nShards, true, cfg)
}

func startShardClusterPlaced(t *testing.T, nNodes, nShards int, disjoint bool) *shardTestCluster {
	return startShardClusterCfg(t, nNodes, nShards, disjoint,
		raft.Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 40})
}

func startShardClusterCfg(t *testing.T, nNodes, nShards int, disjoint bool, cfg raft.Config) *shardTestCluster {
	return startShardClusterFull(t, nNodes, nShards, disjoint, cfg, true)
}

// startShardClusterFull optionally omits durable storage.
//
// durable=false is ONLY for isolating the disk as a variable in the throughput
// benchmark. It is never a mode the bank runs in: without persistence a node that
// restarts has forgotten its vote and can grant a second vote in a term it
// already voted in, which breaks Election Safety.
func startShardClusterFull(t *testing.T, nNodes, nShards int, disjoint bool,
	cfg raft.Config, durable bool) *shardTestCluster {
	t.Helper()
	dir := t.TempDir()

	var ids []raft.NodeID
	total := nNodes
	if disjoint {
		total = nNodes * nShards
	}
	for i := range total {
		ids = append(ids, raft.NodeID(fmt.Sprintf("n%d", i+1)))
	}
	var shardIDs []shard.ID
	for i := range nShards {
		shardIDs = append(shardIDs, shard.ID(fmt.Sprintf("shard-%d", i)))
	}
	ring := shard.NewRing(shardIDs, shard.DefaultVNodes)

	c := &shardTestCluster{
		t: t, nodes: make(map[raft.NodeID]*shardTestNode),
		ids: ids, shards: shardIDs, ring: ring,
	}

	// Bind listeners first so every node learns every address before starting.
	// Port 0 lets the OS choose, so parallel runs cannot collide.
	addrs := make(map[raft.NodeID]string, nNodes)
	type built struct {
		node     *shardTestNode
		replicas []*Replica
	}
	var pending []*built

	for nodeIdx, id := range ids {
		// Which shards does this node hold a replica of? Co-located: all of them.
		// Disjoint: exactly one, and its peer set is only the nodes sharing it.
		hostedShards := shardIDs
		peerSet := ids
		if disjoint {
			g := nodeIdx / nNodes
			hostedShards = shardIDs[g : g+1]
			peerSet = ids[g*nNodes : (g+1)*nNodes]
		}

		var seed int64
		for _, ch := range string(id) {
			seed = seed*31 + int64(ch)
		}

		trans := NewTransport(addrs, 50*time.Millisecond)
		var replicas []*Replica

		for i, sid := range hostedShards {
			// Each node gets its OWN directory. RaftState.Save fsyncs the containing
			// directory on every persist, so nodes sharing one directory serialize
			// their fsyncs against each other — which would make independent shards
			// contend on the single resource the whole benchmark exists to rule out.
			nodeDir := filepath.Join(dir, string(id))
			if err := os.MkdirAll(nodeDir, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", nodeDir, err)
			}
			base := filepath.Join(nodeDir, string(sid))
			applied, err := storage.OpenApplied(base + ".applied")
			if err != nil {
				t.Fatalf("open applied %s/%s: %v", id, sid, err)
			}
			t.Cleanup(func() { applied.Close() })

			machine := shard.NewMachine(sid, ledger.New())
			srv := raft.NewServerWith(id, peerSet, machine,
				trans.ForShard(string(sid)), cfg, seed+int64(i)*7919)
			if durable {
				srv.SetStorage(storage.OpenRaftState(base+".wal", applied))
			}

			replicas = append(replicas, &Replica{ShardID: sid, Raft: srv, Machine: machine})
		}

		groups := make(map[shard.ID]shard.Group, len(replicas))
		for _, rep := range replicas {
			groups[rep.ShardID] = NewNetworkGroup(rep.ShardID, rep)
		}
		coord := shard.NewCoordinator(ring, groups)
		client := NewShardClientService(string(id), ring, coord, "", addrs)

		n := &shardTestNode{id: id, client: client, coord: coord, trans: trans}
		pending = append(pending, &built{node: n, replicas: replicas})
		c.nodes[id] = n
	}

	// Register and bind, then fill in the shared address map so every transport
	// sees the real ports.
	for _, b := range pending {
		host, err := RegisterShards("127.0.0.1:0", b.replicas, b.node.trans, b.node.client, TLSConfig{})
		if err != nil {
			t.Fatalf("register shards for %s: %v", b.node.id, err)
		}
		b.node.host = host
		b.node.addr = host.Addr()
		b.node.client.Attach(host, false)
		addrs[b.node.id] = host.Addr()
	}

	for _, b := range pending {
		b.node.host.Start()
	}
	t.Cleanup(func() {
		for _, b := range pending {
			b.node.host.Stop()
			b.node.trans.Close()
		}
	})

	if !c.waitForLeaders(10 * time.Second) {
		t.Fatalf("not every shard elected a leader%s", c.view())
	}
	return c
}

// leaderFor returns the node leading a shard, or "" if none.
func (c *shardTestCluster) leaderFor(sid shard.ID) raft.NodeID {
	for _, id := range c.ids {
		if rep, ok := c.nodes[id].host.Replica(sid); ok && rep.Raft.Role() == raft.Leader {
			return id
		}
	}
	return ""
}

func (c *shardTestCluster) waitForLeaders(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, sid := range c.shards {
			if c.leaderFor(sid) == "" {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// coordinatorFor returns a coordinator on a node that leads the shard owning
// account, which is where a write must be driven from.
func (c *shardTestCluster) coordinatorFor(account ledger.AccountID) *shard.Coordinator {
	sid := c.ring.Lookup(string(account))
	if id := c.leaderFor(sid); id != "" {
		return c.nodes[id].coord
	}
	return nil
}

// open creates an account on whichever shard owns it, driven from that shard's
// leader, retrying while leadership is in flux.
//
// The retry is SETUP resilience, not a property under test. Under a full parallel
// -race run an election can be lost to scheduling delay between finding the
// leader and proposing — the same §5.2 timing-violated-by-the-machine effect the
// sharded throughput benchmark measured. When that happened here the account was
// never created, and a LATER assertion failed with "transaction aborted" for
// insufficient funds: a misleading symptom two steps removed from its cause.
//
// Seven call sites previously discarded this error, so any such failure surfaced
// only much later. Returning it is not enough on its own; the setup has to
// actually succeed.
func (c *shardTestCluster) open(account ledger.AccountID, amount ledger.Money) error {
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		coord := c.coordinatorFor(account)
		if coord == nil {
			lastErr = fmt.Errorf("no leader for the shard owning %s", account)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		res, err := coord.Transfer(shard.TxID("open-"+string(account)), ledger.Command{
			Op: ledger.OpOpenAccount, IdempotencyKey: "open-" + string(account),
			To: account, Amount: amount,
		})
		if err == nil && res.OK {
			return nil
		}
		if err == nil {
			// The ledger refused it — already open, say. Not a transient condition,
			// so retrying would only mask it.
			return fmt.Errorf("open %s: %s", account, res.Err)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("open %s: %w", account, lastErr)
}

// mustOpen opens an account and fails the test if it cannot.
//
// Used everywhere setup opens an account, so a setup failure is reported where it
// happens rather than as a puzzling assertion failure later on.
func (c *shardTestCluster) mustOpen(t *testing.T, account ledger.AccountID, amount ledger.Money) {
	t.Helper()
	if err := c.open(account, amount); err != nil {
		t.Fatalf("setup: %v%s", err, c.view())
	}
}

// totalMoney sums balances across every shard, reading each shard's leader.
func (c *shardTestCluster) totalMoney() ledger.Money {
	var total ledger.Money
	for _, sid := range c.shards {
		id := c.leaderFor(sid)
		if id == "" {
			continue
		}
		rep, _ := c.nodes[id].host.Replica(sid)
		for _, b := range rep.Machine.State.Balances() {
			total += b
		}
	}
	return total
}

func (c *shardTestCluster) balance(account ledger.AccountID) (ledger.Money, bool) {
	sid := c.ring.Lookup(string(account))
	id := c.leaderFor(sid)
	if id == "" {
		return 0, false
	}
	rep, _ := c.nodes[id].host.Replica(sid)
	return rep.Machine.State.Balance(account)
}

func (c *shardTestCluster) view() string {
	out := "\n  SHARD      LEADER  TERM  COMMIT  ACCOUNTS\n"
	for _, sid := range c.shards {
		id := c.leaderFor(sid)
		var term raft.Term
		var commit raft.Index
		var accounts int
		if id != "" {
			rep, _ := c.nodes[id].host.Replica(sid)
			term, commit = rep.Raft.CurrentTerm(), rep.Raft.CommitIndex()
			accounts = len(rep.Machine.State.Balances())
		}
		out += fmt.Sprintf("  %-10s %-7s %-5d %-7d %d\n", sid, id, term, commit, accounts)
	}
	return out
}

// crossShardPair returns two accounts owned by different shards.
func (c *shardTestCluster) crossShardPair() (ledger.AccountID, ledger.AccountID) {
	for i := range 200 {
		for j := i + 1; j < 200; j++ {
			a := ledger.AccountID(fmt.Sprintf("acct-%d", i))
			b := ledger.AccountID(fmt.Sprintf("acct-%d", j))
			if c.ring.Lookup(string(a)) != c.ring.Lookup(string(b)) {
				return a, b
			}
		}
	}
	return "", ""
}

// submitFollowingRedirect submits a write and follows one §8 redirect, which is
// exactly what the client contract tells a caller to do. Reports whether the
// write committed.
func submitFollowingRedirect(t *testing.T, addr string, args TxArgs) bool {
	t.Helper()

	call := func(at string) (TxReply, error) {
		client, err := rpc.Dial("tcp", at)
		if err != nil {
			return TxReply{}, err
		}
		defer client.Close()
		var reply TxReply
		err = client.Call("Bank.Submit", args, &reply)
		return reply, err
	}

	reply, err := call(addr)
	if err != nil {
		t.Logf("submit to %s: %v", addr, err)
		return false
	}
	if reply.OK {
		return true
	}
	if !reply.NotLeader || reply.LeaderAddr == "" {
		return false
	}

	// Retry at the named leader with the same idempotency key. Safe by
	// construction: that is what the key is for.
	retry, err := call(reply.LeaderAddr)
	if err != nil {
		t.Logf("retry at %s: %v", reply.LeaderAddr, err)
		return false
	}
	return retry.OK
}

// --- multiplexing ---------------------------------------------------------

// Several Raft groups must run independently over ONE listener per node, each
// electing its own leader. This is the structural claim of G1.
func TestShardsElectIndependentLeadersOverOneListener(t *testing.T) {
	c := startShardCluster(t, 3, 3)

	seen := make(map[shard.ID]raft.NodeID)
	for _, sid := range c.shards {
		id := c.leaderFor(sid)
		if id == "" {
			t.Fatalf("shard %s has no leader%s", sid, c.view())
		}
		seen[sid] = id
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 shards with leaders, got %d%s", len(seen), c.view())
	}

	// Every node exposes exactly one listener, and all three groups are reachable
	// through it. If multiplexing were broken, some group would be unreachable and
	// could not have elected a leader above.
	for _, id := range c.ids {
		if got := len(c.nodes[id].host.ShardIDs()); got != 3 {
			t.Fatalf("node %s hosts %d shards, want 3", id, got)
		}
	}
	t.Logf("three Raft groups per node, one listener each%s", c.view())
}

// A shard's log must grow only for writes routed to it. Multiplexing shares a
// transport, and this is the assertion that sharing it did not couple the groups.
func TestMultiplexedShardsCommitIndependently(t *testing.T) {
	c := startShardCluster(t, 3, 3)

	a, _ := c.crossShardPair()
	target := c.ring.Lookup(string(a))

	before := make(map[shard.ID]int, len(c.shards))
	for _, sid := range c.shards {
		id := c.leaderFor(sid)
		rep, _ := c.nodes[id].host.Replica(sid)
		before[sid] = len(rep.Raft.LogEntries())
	}

	if err := c.open(a, 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	grew := 0
	for _, sid := range c.shards {
		id := c.leaderFor(sid)
		rep, _ := c.nodes[id].host.Replica(sid)
		delta := len(rep.Raft.LogEntries()) - before[sid]
		if sid == target && delta == 0 {
			t.Fatalf("the owning shard %s did not grow its log%s", sid, c.view())
		}
		if sid != target && delta > 0 {
			// A non-owning shard growing would mean the write leaked across groups —
			// exactly the coupling multiplexing must not introduce.
			t.Fatalf("shard %s grew by %d entries for a write it does not own%s",
				sid, delta, c.view())
		}
		if delta > 0 {
			grew++
		}
	}
	if grew != 1 {
		t.Fatalf("%d shards grew, want exactly 1%s", grew, c.view())
	}
}

// --- cross-shard money over real TCP -------------------------------------

// The normal path: money moves between accounts on different shards, through
// 2PC, over real network transport, and the total is unchanged.
func TestCrossShardTransferOverRealTransport(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()
	if err := c.open(a, 10000); err != nil {
		t.Fatalf("open %s: %v", a, err)
	}
	if err := c.open(b, 1000); err != nil {
		t.Fatalf("open %s: %v", b, err)
	}
	total := c.totalMoney()

	coord := c.coordinatorFor(a)
	res, err := coord.Transfer("tx-real-1", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "tx-real-1",
		From: a, To: b, Amount: 2500,
	})
	if err != nil {
		t.Fatalf("cross-shard transfer over TCP: %v (res=%+v)%s", err, res, c.view())
	}

	if got, _ := c.balance(a); got != 7500 {
		t.Fatalf("%s = %s after transfer, want 75.00%s", a, got, c.view())
	}
	if got, _ := c.balance(b); got != 3500 {
		t.Fatalf("%s = %s after transfer, want 35.00%s", b, got, c.view())
	}
	if got := c.totalMoney(); got != total {
		t.Fatalf("money conservation violated over real transport: %s -> %s", total, got)
	}
}

// Retry path: the same idempotency key delivered twice must move money once.
func TestCrossShardTransferRetryIsIdempotent(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()
	c.mustOpen(t, a, 10000)
	c.mustOpen(t, b, 1000)
	total := c.totalMoney()

	coord := c.coordinatorFor(a)
	cmd := ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "tx-retry", From: a, To: b, Amount: 2000,
	}
	if _, err := coord.Transfer("tx-retry", cmd); err != nil {
		t.Fatalf("first transfer: %v", err)
	}

	// The same transaction id and the same request, delivered again — a network
	// retry. It must not move money a second time.
	coord.Transfer("tx-retry", cmd)

	if got, _ := c.balance(a); got != 8000 {
		t.Fatalf("%s = %s after a retried transfer, want 80.00 — the retry moved money "+
			"a second time%s", a, got, c.view())
	}
	if got, _ := c.balance(b); got != 3000 {
		t.Fatalf("%s = %s after a retried transfer, want 30.00%s", b, got, c.view())
	}
	if got := c.totalMoney(); got != total {
		t.Fatalf("money conservation violated by a retry: %s -> %s", total, got)
	}
}

// Concurrent path: several clients transferring from the same account at once.
// The ledger serializes them through the log, so the account may never go
// negative and the total may never change.
func TestConcurrentCrossShardTransfersConserveMoney(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrent cross-shard load; skipped under -short")
	}
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()

	// Funded well beyond the total being moved, deliberately.
	//
	// A cross-shard transfer RESERVES the debit at prepare time, so concurrent
	// transfers each hold their amount against the available balance until they
	// resolve. Funding alice with exactly n*amount made two of twelve fail with
	// "insufficient funds" — which is the ledger behaving CORRECTLY, not a defect:
	// reserved money is unavailable, and that is the property that stops the same
	// money being spent twice. This test is about the §8 redirect, so the funding
	// is set so that reservations can never be the limiting factor.
	c.mustOpen(t, a, 100000)
	c.mustOpen(t, b, 0)
	total := c.totalMoney()

	const n = 12
	var wg sync.WaitGroup
	var committed atomic.Int64

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("conc-%d", i)

			// Each client starts at a DIFFERENT node, as separate bank-app windows
			// hitting separate processes would, and follows the §8 redirect exactly
			// as the client contract prescribes — retrying at the address the
			// cluster names, with the SAME idempotency key.
			//
			// Driving the coordinators directly instead would make two thirds of
			// these requests land on a non-leader and simply fail, which measures
			// the harness rather than the cluster.
			if submitFollowingRedirect(t, c.nodes[c.ids[i%len(c.ids)]].addr, TxArgs{
				Op: "transfer", IdempotencyKey: key,
				From: string(a), To: string(b), Amount: 1000,
			}) {
				committed.Add(1)
			}
		}(i)
	}
	wg.Wait()

	balA, _ := c.balance(a)
	balB, _ := c.balance(b)

	// Every client that followed its redirect should have been served.
	if committed.Load() != n {
		t.Fatalf("only %d of %d concurrent transfers committed; with the §8 redirect "+
			"followed, a client should not be turned away%s", committed.Load(), n, c.view())
	}
	if balB != ledger.Money(n*1000) {
		t.Fatalf("%s = %s after %d transfers of 10.00, want %s%s",
			b, balB, n, ledger.Money(n*1000), c.view())
	}

	if balA < 0 {
		t.Fatalf("%s went negative (%s): the ledger did not serialize concurrent "+
			"withdrawals%s", a, balA, c.view())
	}
	if got := c.totalMoney(); got != total {
		t.Fatalf("money conservation violated under concurrent load: %s -> %s "+
			"(a=%s b=%s)%s", total, got, balA, balB, c.view())
	}
	t.Logf("after %d concurrent cross-shard transfers: %s=%s %s=%s, total unchanged at %s",
		n, a, balA, b, balB, total)
}

// Failure path: killing the leader of one shard must not stop another shard from
// committing. Independent groups have independent availability — that is the
// whole reason to shard.
func TestOneShardLeaderFailureDoesNotBlockAnother(t *testing.T) {
	if testing.Short() {
		t.Skip("involves an election; skipped under -short")
	}
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()
	c.mustOpen(t, a, 5000)
	c.mustOpen(t, b, 5000)
	total := c.totalMoney()

	victimShard := c.ring.Lookup(string(a))
	var survivor shard.ID
	for _, sid := range c.shards {
		if sid != victimShard {
			survivor = sid
			break
		}
	}

	// Stop the victim shard's leader replica — only that group's replica on that
	// node, leaving the node's other groups running. This is precisely what
	// multiplexing must permit: one group failing is not the process failing.
	victimNode := c.leaderFor(victimShard)
	rep, _ := c.nodes[victimNode].host.Replica(victimShard)
	rep.Raft.Stop()

	// The survivor shard must still elect and commit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.leaderFor(survivor) == "" {
		time.Sleep(10 * time.Millisecond)
	}
	if c.leaderFor(survivor) == "" {
		t.Fatalf("shard %s lost its leader because shard %s's leader was stopped%s",
			survivor, victimShard, c.view())
	}

	// And the victim shard elects a replacement from its surviving replicas.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.leaderFor(victimShard) == "" {
		time.Sleep(10 * time.Millisecond)
	}
	if c.leaderFor(victimShard) == "" {
		t.Fatalf("shard %s never elected a replacement leader%s", victimShard, c.view())
	}

	if got := c.totalMoney(); got != total {
		t.Fatalf("money changed across a leader failure: %s -> %s%s", total, got, c.view())
	}
	t.Logf("shard %s survived its leader being stopped; shard %s was unaffected%s",
		victimShard, survivor, c.view())
}

// --- client API routing ---------------------------------------------------

// A read for an account owned by a shard this node does not host must say so,
// rather than answering "not found" from a ledger that was never asked.
func TestReadForUnhostedShardReportsClearly(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 4200)

	// Build a client service whose host holds only one shard, then ask it for an
	// account owned by the other.
	owner := c.ring.Lookup(string(a))
	var other shard.ID
	for _, sid := range c.shards {
		if sid != owner {
			other = sid
			break
		}
	}

	node := c.nodes[c.ids[0]]
	rep, _ := node.host.Replica(other)
	partial, err := RegisterShards("127.0.0.1:0", []*Replica{rep}, node.trans, nil, TLSConfig{})
	if err != nil {
		t.Fatalf("register partial host: %v", err)
	}
	defer partial.server.Close()

	svc := NewShardClientService("partial", c.ring, node.coord, "", nil)
	svc.Attach(partial, false)

	var reply BalanceReply
	svc.Balance(BalanceArgs{Account: string(a)}, &reply)

	if reply.Found {
		t.Fatalf("a node hosting no replica of shard %s answered for %s", owner, a)
	}
	if reply.Err == "" {
		t.Fatal("a read for an unhosted shard returned no error; silently reporting " +
			"'not found' for an account that exists elsewhere is worse than an error")
	}
	t.Logf("unhosted-shard read reported: %s", reply.Err)
}

// The sharded client API must be reachable over the wire, not just in-process:
// this is what a bank app actually calls.
func TestShardClientAPIOverTheWire(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	if err := c.open(a, 7700); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Dial the node that leads the owning shard, so the local replica has the data.
	owner := c.ring.Lookup(string(a))
	leader := c.leaderFor(owner)
	client, err := rpc.Dial("tcp", c.nodes[leader].addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var reply BalanceReply
	if err := client.Call("Bank.Balance", BalanceArgs{Account: string(a)}, &reply); err != nil {
		t.Fatalf("Bank.Balance over the wire: %v", err)
	}
	if !reply.Found || reply.Balance != 7700 {
		t.Fatalf("balance over the wire = %d (found=%v), want 7700%s",
			reply.Balance, reply.Found, c.view())
	}

	var status ClusterStatusReply
	if err := client.Call("Bank.Status", struct{}{}, &status); err != nil {
		t.Fatalf("Bank.Status: %v", err)
	}
	if len(status.Shards) != 2 {
		t.Fatalf("status reported %d shards, want 2", len(status.Shards))
	}
	t.Logf("node %s reports %d shard replicas over the wire", status.NodeID, len(status.Shards))
}

// --- configuration validation --------------------------------------------

// Hosting the same shard twice in one process must be refused: it would silently
// run one shard's group twice while another goes unhosted.
func TestDuplicateShardRegistrationIsRejected(t *testing.T) {
	machine := shard.NewMachine("shard-0", ledger.New())
	srv := raft.NewServerWith("n1", []raft.NodeID{"n1"}, machine, nil, raft.DefaultConfig(), 1)
	rep := &Replica{ShardID: "shard-0", Raft: srv, Machine: machine}

	_, err := RegisterShards("127.0.0.1:0", []*Replica{rep, rep}, nil, nil, TLSConfig{})
	if err == nil {
		t.Fatal("registering the same shard twice was accepted")
	}
}

func TestParseShardAssignment(t *testing.T) {
	got, err := ParseShardAssignment("shard-0, shard-1 ,shard-2")
	if err != nil {
		t.Fatalf("ParseShardAssignment: %v", err)
	}
	if len(got) != 3 || got[0] != "shard-0" || got[2] != "shard-2" {
		t.Fatalf("parsed %v, want [shard-0 shard-1 shard-2]", got)
	}

	if _, err := ParseShardAssignment("shard-0,shard-0"); err == nil {
		t.Fatal("a duplicate shard in the assignment was accepted")
	}
	if _, err := ParseShardAssignment(""); err == nil {
		t.Fatal("an empty shard assignment was accepted")
	}
}
