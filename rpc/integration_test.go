package rpc

import (
	"fmt"
	"net/rpc"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/storage"
)

// End-to-end tests over real TCP, with real durable storage.
//
// These differ from sim/ in an important way: sim/ tests consensus against a
// simulated network, while these test the whole stack — RPC serialization, the
// client API, the ledger, and persistence — the way a bank app will actually use
// it. Per RULES.md rule 3, both matter: the paper's rules AND real-world behavior.

type testCluster struct {
	t       *testing.T
	ids     []raft.NodeID
	addrs   map[raft.NodeID]string
	servers map[raft.NodeID]*raft.Server
	rpcs    map[raft.NodeID]*Server
	states  map[raft.NodeID]*ledger.State
	wals    map[raft.NodeID]*storage.WAL
	trans   map[raft.NodeID]*Transport
}

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	dir := t.TempDir()

	c := &testCluster{
		t:       t,
		addrs:   make(map[raft.NodeID]string),
		servers: make(map[raft.NodeID]*raft.Server),
		rpcs:    make(map[raft.NodeID]*Server),
		states:  make(map[raft.NodeID]*ledger.State),
		wals:    make(map[raft.NodeID]*storage.WAL),
		trans:   make(map[raft.NodeID]*Transport),
	}

	// Bind listeners first so every node knows every address before starting.
	type pending struct {
		id  raft.NodeID
		srv *Server
	}
	for i := range n {
		id := raft.NodeID(fmt.Sprintf("n%d", i+1))
		c.ids = append(c.ids, id)
		// Port 0 lets the OS choose a free port, so parallel test runs cannot
		// collide.
		c.addrs[id] = "127.0.0.1:0"
	}

	cfg := raft.Config{ElectionTimeoutMin: 80, ElectionTimeoutMax: 160, HeartbeatInterval: 20}

	// Two passes: create listeners to learn real ports, then wire transports.
	listeners := make(map[raft.NodeID]*Server)
	for i, id := range c.ids {
		wal, err := storage.Open(filepath.Join(dir, string(id)+".wal"))
		if err != nil {
			t.Fatalf("wal %s: %v", id, err)
		}
		st := ledger.New()
		machine := ledger.NewMachine(st)

		tr := NewTransport(c.addrs, 300*time.Millisecond)
		srv := raft.NewServerWith(id, c.ids, machine, tr, cfg, int64(i+1)*7919)
		srv.SetStorage(storage.NewRaftState(wal))

		api := NewClientService(srv, machine, c.addrs)
		rs, err := Listen("127.0.0.1:0", srv, api)
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		c.addrs[id] = rs.Addr() // real bound address

		c.servers[id] = srv
		c.states[id] = st
		c.wals[id] = wal
		c.trans[id] = tr
		listeners[id] = rs
		_ = pending{}
	}
	c.rpcs = listeners

	// Every transport shares the same addrs map, which now holds real ports.
	for _, id := range c.ids {
		c.servers[id].Start()
	}

	t.Cleanup(func() {
		for _, id := range c.ids {
			c.servers[id].Stop()
			c.rpcs[id].Close()
			c.trans[id].Close()
			c.wals[id].Close()
		}
	})
	return c
}

// waitLeader returns the id and address of the elected leader.
func (c *testCluster) waitLeader(timeout time.Duration) (raft.NodeID, string) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if c.servers[id].Role() == raft.Leader {
				return id, c.addrs[id]
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %v", timeout)
	return "", ""
}

// call makes one client RPC to addr.
func call(t *testing.T, addr, method string, args, reply any) error {
	t.Helper()
	cl, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.Call(method, args, reply)
}

// --- Normal flow ----------------------------------------------------------

func TestEndToEndBanking(t *testing.T) {
	c := startCluster(t, 3)
	_, leaderAddr := c.waitLeader(3 * time.Second)

	// Follow leader redirects, exactly as a real client must (§8). The node that
	// was leader when we looked can be deposed before our write arrives; that is
	// normal, and the redirect is the supported way to handle it.
	mustTx := func(args TxArgs) TxReply {
		t.Helper()
		addr := leaderAddr
		for attempt := range 5 {
			var r TxReply
			if err := call(t, addr, "Bank.Submit", args, &r); err != nil {
				t.Fatalf("rpc: %v", err)
			}
			if r.OK {
				return r
			}
			if r.NotLeader && r.LeaderAddr != "" {
				addr = r.LeaderAddr
				leaderAddr = r.LeaderAddr
				continue
			}
			if r.NotLeader {
				// No leader known yet; wait for the election to settle and retry
				// with the same idempotency key.
				time.Sleep(100 * time.Millisecond)
				_, addr = c.waitLeader(2 * time.Second)
				leaderAddr = addr
				continue
			}
			t.Fatalf("tx %+v failed on attempt %d: %s", args, attempt, r.Err)
		}
		t.Fatalf("tx %+v never reached a leader", args)
		return TxReply{}
	}

	mustTx(TxArgs{Op: "open", IdempotencyKey: "o1", To: "alice", Amount: 10000})
	mustTx(TxArgs{Op: "open", IdempotencyKey: "o2", To: "bob", Amount: 5000})
	mustTx(TxArgs{Op: "deposit", IdempotencyKey: "d1", To: "alice", Amount: 2500})
	r := mustTx(TxArgs{Op: "transfer", IdempotencyKey: "t1", From: "alice", To: "bob", Amount: 2000})

	if r.Balance != 10500 {
		t.Fatalf("alice = %d, want 10500", r.Balance)
	}

	// Linearizable read through the leader.
	var br BalanceReply
	if err := call(t, leaderAddr, "Bank.Balance",
		BalanceArgs{Account: "bob", Linearizable: true}, &br); err != nil {
		t.Fatalf("balance rpc: %v", err)
	}
	if !br.Found || br.Balance != 7000 {
		t.Fatalf("bob = %d (found=%v), want 7000", br.Balance, br.Found)
	}

	// Every node's ledger must agree, and every node's books must balance.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		agreed := true
		for _, id := range c.ids {
			if b, _ := c.states[id].Balance("bob"); b != 7000 {
				agreed = false
			}
		}
		if agreed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range c.ids {
		b, _ := c.states[id].Balance("bob")
		if b != 7000 {
			t.Fatalf("%s has bob = %s, want 70.00 — replicas disagree", id, b)
		}
		if err := c.states[id].VerifyDoubleEntry(); err != nil {
			t.Fatalf("%s failed the double-entry audit: %v", id, err)
		}
	}
}

// A follower must reject a write and point at the leader (§8).
func TestFollowerRedirectsToLeader(t *testing.T) {
	c := startCluster(t, 3)
	leaderID, leaderAddr := c.waitLeader(3 * time.Second)

	var followerAddr string
	for _, id := range c.ids {
		if id != leaderID {
			followerAddr = c.addrs[id]
			break
		}
	}

	var r TxReply
	if err := call(t, followerAddr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "x", To: "z", Amount: 100}, &r); err != nil {
		t.Fatalf("rpc: %v", err)
	}
	if !r.NotLeader {
		t.Fatal("follower accepted a write instead of redirecting")
	}
	if r.LeaderAddr != leaderAddr {
		t.Fatalf("redirected to %q, want the leader at %q", r.LeaderAddr, leaderAddr)
	}
}

// --- Retry flow -----------------------------------------------------------

// The real-world case idempotency exists for: a client that resends because it
// did not hear back must not be charged twice.
func TestRetriedWriteAppliesOnce(t *testing.T) {
	c := startCluster(t, 3)
	_, leaderAddr := c.waitLeader(3 * time.Second)

	call(t, leaderAddr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "acct", Amount: 10000}, &TxReply{})

	withdraw := TxArgs{Op: "withdraw", IdempotencyKey: "retry-me", From: "acct", Amount: 3000}

	for range 5 {
		var r TxReply
		if err := call(t, leaderAddr, "Bank.Submit", withdraw, &r); err != nil {
			t.Fatalf("rpc: %v", err)
		}
		if !r.OK {
			t.Fatalf("retry rejected: %s", r.Err)
		}
	}

	var br BalanceReply
	call(t, leaderAddr, "Bank.Balance", BalanceArgs{Account: "acct", Linearizable: true}, &br)
	if br.Balance != 7000 {
		t.Fatalf("balance = %d, want 7000 — the retry was applied more than once", br.Balance)
	}
}

// --- Concurrent flow ------------------------------------------------------

// NOW.md's bank-app scenario: several windows hitting the SAME account at once.
// Whatever the interleaving, the account must never go negative and exactly the
// available funds may be withdrawn.
func TestConcurrentWithdrawalsSameAccount(t *testing.T) {
	c := startCluster(t, 3)
	_, leaderAddr := c.waitLeader(3 * time.Second)

	call(t, leaderAddr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "shared", Amount: 10000}, &TxReply{})

	const clients = 15
	const amount = 1000 // only 10 can succeed

	var wg sync.WaitGroup
	replies := make([]TxReply, clients)
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call(t, leaderAddr, "Bank.Submit", TxArgs{
				Op:             "withdraw",
				IdempotencyKey: fmt.Sprintf("w%d", i),
				From:           "shared",
				Amount:         amount,
			}, &replies[i])
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for _, r := range replies {
		if r.OK {
			succeeded++
		}
	}
	if succeeded != 10 {
		t.Fatalf("%d withdrawals succeeded, want exactly 10 — the ledger over- or under-paid", succeeded)
	}

	var br BalanceReply
	call(t, leaderAddr, "Bank.Balance", BalanceArgs{Account: "shared", Linearizable: true}, &br)
	if br.Balance != 0 {
		t.Fatalf("final balance = %d, want 0", br.Balance)
	}
	if br.Balance < 0 {
		t.Fatal("account went negative — concurrent withdrawals overdrew it")
	}
}

// --- Failure flow ---------------------------------------------------------

// Kill the leader over real TCP and confirm the cluster keeps serving clients.
func TestLeaderFailoverOverRealNetwork(t *testing.T) {
	c := startCluster(t, 3)
	oldLeader, oldAddr := c.waitLeader(3 * time.Second)

	call(t, oldAddr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "acct", Amount: 5000}, &TxReply{})

	// Kill the leader: stop its Raft loop and close its listener, as a process
	// death would.
	c.servers[oldLeader].Stop()
	c.rpcs[oldLeader].Close()

	// A new leader must emerge among the survivors.
	var newAddr string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && newAddr == "" {
		for _, id := range c.ids {
			if id != oldLeader && c.servers[id].Role() == raft.Leader {
				newAddr = c.addrs[id]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newAddr == "" {
		t.Fatal("no new leader after killing the leader")
	}

	// And the surviving cluster must still accept writes, with the pre-failover
	// balance intact.
	var r TxReply
	if err := call(t, newAddr, "Bank.Submit",
		TxArgs{Op: "deposit", IdempotencyKey: "after", To: "acct", Amount: 1000}, &r); err != nil {
		t.Fatalf("rpc to new leader: %v", err)
	}
	if !r.OK {
		t.Fatalf("new leader rejected the write: %s", r.Err)
	}
	if r.Balance != 6000 {
		t.Fatalf("balance = %d, want 6000 — committed state was lost in failover", r.Balance)
	}
}

// --- The Phase 1 demo: more nodes ≠ more write throughput ----------------

// NOW.md names this as an explicit Phase 1 goal: show hands-on that adding nodes
// buys fault tolerance, NOT write throughput, because every write still funnels
// through one leader. Write scaling comes from sharding (Phase 2), not replicas.
func TestMoreNodesDoNotIncreaseWriteThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput demo in -short mode")
	}

	measure := func(n int) (float64, int) {
		c := startCluster(t, n)
		_, leaderAddr := c.waitLeader(5 * time.Second)

		call(t, leaderAddr, "Bank.Submit",
			TxArgs{Op: "open", IdempotencyKey: "o", To: "acct", Amount: 100000000}, &TxReply{})

		const writes = 40
		start := time.Now()
		ok := 0
		for i := range writes {
			var r TxReply
			err := call(t, leaderAddr, "Bank.Submit", TxArgs{
				Op:             "deposit",
				IdempotencyKey: fmt.Sprintf("bench-%d-%d", n, i),
				To:             "acct",
				Amount:         1,
			}, &r)
			if err == nil && r.OK {
				ok++
			}
		}
		elapsed := time.Since(start)
		return float64(ok) / elapsed.Seconds(), ok
	}

	tps3, ok3 := measure(3)
	tps5, ok5 := measure(5)

	if ok3 == 0 || ok5 == 0 {
		t.Fatalf("no writes completed (3-node: %d, 5-node: %d)", ok3, ok5)
	}

	t.Logf("write throughput: 3 nodes = %.1f tx/s, 5 nodes = %.1f tx/s (%.0f%% of 3-node)",
		tps3, tps5, tps5/tps3*100)

	// The claim being demonstrated: adding nodes does not INCREASE throughput.
	// Five nodes should be no faster than three — in fact usually slightly slower,
	// since the leader waits for a majority of a larger cluster and does more
	// work per entry. A generous margin keeps this from being a flaky
	// timing-sensitive assertion while still catching the misconception.
	if tps5 > tps3*1.5 {
		t.Fatalf("5 nodes were %.0f%% faster than 3 — that contradicts Raft: "+
			"writes funnel through one leader, so replicas add fault tolerance, not throughput",
			(tps5/tps3-1)*100)
	}
}
