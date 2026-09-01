package sim

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Re-replication tests (READING_LIST.md §21).
//
// The property under test is not "the count went back to 3". It is that a healed
// replica holds THE SAME COMMITTED LOG as the replicas it joined — Raft's Log
// Matching Property (Figure 3) — and that healing is refused whenever it could
// not honestly deliver that.

// waitFor polls until cond holds, so tests assert on a settled cluster rather
// than on a fixed sleep that is either flaky or slow.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", within, what)
}

// committedLog returns a node's log entries excluding the index-0 sentinel.
func committedLog(t *testing.T, g *ShardGroup, id raft.NodeID) []raft.LogEntry {
	t.Helper()
	srv := g.Nodes[id]
	if srv == nil {
		t.Fatalf("%s holds no replica", id)
	}
	all := srv.LogEntries()
	commit := srv.CommitIndex()

	var out []raft.LogEntry
	for _, e := range all {
		if e.Index >= 1 && e.Index <= commit {
			out = append(out, e)
		}
	}
	return out
}

// assertLogsMatch asserts Figure 3's Log Matching Property across two replicas:
// entries at the same index must have the same term AND the same command.
func assertLogsMatch(t *testing.T, g *ShardGroup, a, b raft.NodeID) {
	t.Helper()
	la, lb := committedLog(t, g, a), committedLog(t, g, b)

	n := len(la)
	if len(lb) < n {
		n = len(lb)
	}
	if n == 0 {
		t.Fatalf("no committed entries to compare between %s and %s", a, b)
	}

	for i := range n {
		if la[i].Index != lb[i].Index {
			t.Fatalf("index mismatch at position %d: %s has %d, %s has %d",
				i, a, la[i].Index, b, lb[i].Index)
		}
		if la[i].Term != lb[i].Term {
			t.Fatalf("LOG MATCHING VIOLATED at index %d: %s term=%d, %s term=%d",
				la[i].Index, a, la[i].Term, b, lb[i].Term)
		}
		if string(la[i].Command) != string(lb[i].Command) {
			t.Fatalf("LOG MATCHING VIOLATED at index %d: same index+term, different command\n  %s: %q\n  %s: %q",
				la[i].Index, a, la[i].Command, b, lb[i].Command)
		}
	}
}

// openAndFund opens an account through the coordinator.
func openAndFund(t *testing.T, sc *ShardCluster, acct string, amount int64) {
	t.Helper()
	if err := sc.Open(ledger.AccountID(acct), ledger.Money(amount)); err != nil {
		t.Fatalf("opening %s: %v", acct, err)
	}
}

// deposit puts money into an account through the shard's own Raft group.
//
// The idempotency key is required (ledger enforces it) and is what makes a
// retried deposit safe — the same key applied twice does not double-credit.
func deposit(sc *ShardCluster, acct string, amount int64, key string) error {
	sid := sc.Coordinator.ShardFor(ledger.AccountID(acct))
	_, _, err := sc.Groups[sid].Propose(shard.Command{
		Op: shard.OpSingle,
		Ledger: ledger.Command{
			Op: ledger.OpDeposit, IdempotencyKey: key,
			To: ledger.AccountID(acct), Amount: ledger.Money(amount),
		},
	}, 3*time.Second)
	return err
}

// balanceOn reads ONE replica's own ledger, not the group's.
//
// That distinction is the point of these tests: asking the group would route to
// the leader and tell us nothing about whether the healed replica actually
// received the data.
func balanceOn(t *testing.T, g *ShardGroup, id raft.NodeID, acct string) int64 {
	t.Helper()
	sm := g.SMs[id]
	if sm == nil {
		t.Fatalf("%s holds no state machine", id)
	}
	bal, _ := sm.State.Balance(ledger.AccountID(acct))
	return int64(bal)
}

// ---------------------------------------------------------------- normal path

// A shard that loses one replica but keeps a majority heals onto a spare, and
// the new replica ends up with the same committed log as the survivors.
func TestHealedReplicaMatchesTheSurvivingLog(t *testing.T) {
	sc, err := NewPlacedCluster(1, 4, 3, 20260830)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]

	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	openAndFund(t, sc, "dave", 10_000)

	holders := sc.Holders(sid)
	if len(holders) != 3 {
		t.Fatalf("expected RF=3, got %d holders: %v", len(holders), holders)
	}

	// Kill a FOLLOWER, so a majority plainly survives and the leader is untouched.
	leaderID := g.leader()
	var victim raft.NodeID
	for _, h := range holders {
		if h != leaderID {
			victim = h
			break
		}
	}
	sc.Groups[sid].Net.Crash(victim)

	// The spare is the one machine of four holding no replica.
	var spare raft.NodeID
	held := map[raft.NodeID]bool{}
	for _, h := range holders {
		held[h] = true
	}
	for i := range 4 {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if !held[id] {
			spare = id
		}
	}
	if spare == "" {
		t.Fatal("no spare machine in a 4-machine RF=3 cluster")
	}

	if err := sc.AddReplica(sid, spare); err != nil {
		t.Fatalf("AddReplica: %v", err)
	}

	// The new replica must catch up through the ordinary Raft path.
	waitFor(t, 10*time.Second, "the healed replica to catch up", func() bool {
		return balanceOn(t, g, spare, "dave") == 10_000
	})

	// The property that actually matters: identical committed logs, not merely
	// an identical balance. A balance check says something agrees; Log Matching
	// says the replica is genuinely a copy.
	assertLogsMatch(t, g, leaderID, spare)

	if got := balanceOn(t, g, spare, "dave"); got != 10_000 {
		t.Fatalf("healed replica has dave=%d, want 10000", got)
	}
}

// After healing, the shard survives a FURTHER failure — which is the entire
// point of restoring the replication factor.
func TestHealingRestoresFaultTolerance(t *testing.T) {
	sc, err := NewPlacedCluster(1, 4, 3, 20260831)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]
	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	openAndFund(t, sc, "erin", 500)

	holders := sc.Holders(sid)
	leaderID := g.leader()
	var victim raft.NodeID
	for _, h := range holders {
		if h != leaderID {
			victim = h
			break
		}
	}

	held := map[raft.NodeID]bool{}
	for _, h := range holders {
		held[h] = true
	}
	var spare raft.NodeID
	for i := range 4 {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if !held[id] {
			spare = id
		}
	}

	g.Net.Crash(victim)
	if err := sc.AddReplica(sid, spare); err != nil {
		t.Fatalf("AddReplica: %v", err)
	}
	waitFor(t, 10*time.Second, "catch-up", func() bool {
		return balanceOn(t, g, spare, "erin") == 500
	})
	if err := sc.RemoveReplica(sid, victim); err != nil {
		t.Fatalf("RemoveReplica: %v", err)
	}

	// Back to 3 healthy replicas. Losing one more must still leave a working
	// shard — before healing, this second failure would have broken quorum.
	remaining := sc.Holders(sid)
	if len(remaining) != 3 {
		t.Fatalf("after healing expected 3 holders, got %v", remaining)
	}

	newLeader := g.leader()
	var second raft.NodeID
	for _, h := range remaining {
		if h != newLeader {
			second = h
			break
		}
	}
	g.Net.Crash(second)

	waitFor(t, 10*time.Second, "a leader after the second failure", func() bool {
		return g.leader() != ""
	})

	err = deposit(sc, "erin", 100, "post-heal")
	if err != nil {
		t.Fatalf("shard should still commit after healing + one more failure: %v", err)
	}
}

// --------------------------------------------------------------- failure path

// The refusal that keeps the system honest: a shard below majority must NOT be
// healed, because there is no majority to copy committed entries from.
func TestBelowMajorityShardIsRefusedNotFabricated(t *testing.T) {
	sc, err := NewPlacedCluster(1, 4, 3, 20260832)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]
	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	openAndFund(t, sc, "dave", 10_000)

	holders := sc.Holders(sid)
	held := map[raft.NodeID]bool{}
	for _, h := range holders {
		held[h] = true
	}
	var spare raft.NodeID
	for i := range 4 {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if !held[id] {
			spare = id
		}
	}

	// Kill TWO of three: quorum is gone, so no leader can exist.
	g.Net.Crash(holders[0])
	g.Net.Crash(holders[1])

	// NOT waiting for g.leader() to go empty. A partitioned leader keeps reporting
	// Leader because Role() is its own local view — it has not yet heard a higher
	// term, and it never will while isolated. That is the phantom-leader case the
	// group's leader() doc already describes, and it is exactly why the safety of
	// this path cannot rest on a liveness check.
	//
	// The real guarantee is stronger and is what gets asserted: with two of three
	// replicas gone, the configuration change CANNOT COMMIT, so re-replication
	// fails whether or not some node still believes it leads.
	err = sc.AddReplica(sid, spare)
	if err == nil {
		t.Fatal("BUG: re-replicated a shard that has no majority. The new replica " +
			"could only have been filled with invented state, which would silently " +
			"zero every balance in the shard.")
	}

	// And the data must still exist — healing being refused does not mean it was
	// discarded.
	//
	// Checked across ALL of the shard's replicas, including the crashed ones. A
	// crashed node in this simulator is unreachable, not wiped: its state machine
	// still holds what it applied, which is the concrete sense in which dave's
	// money is "still there, just unreadable". Requiring one PARTICULAR survivor
	// to have it would instead be asserting that every replica is always fully
	// caught up, which Raft never promises for a follower.
	found := false
	for _, h := range sc.Holders(sid) {
		if balanceOn(t, g, h, "dave") == 10_000 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("dave's balance is on no replica at all: the data was actually lost, " +
			"not merely unreachable")
	}
}

// Healing must not run against a shard whose replica set is already complete.
func TestHealingARepeatedTargetIsRejected(t *testing.T) {
	sc, err := NewPlacedCluster(1, 4, 3, 20260833)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]
	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	existing := sc.Holders(sid)[0]
	if err := sc.AddReplica(sid, existing); err == nil {
		t.Fatal("adding a replica to a machine that already holds one must be rejected")
	}
}

// ------------------------------------------------------------------ retry path

// §6 allows only one configuration change at a time. A duplicate heal request —
// the same repair delivered twice — must not produce a double-added replica.
func TestRepeatedHealRequestDoesNotDoubleAdd(t *testing.T) {
	sc, err := NewPlacedCluster(1, 5, 3, 20260834)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]
	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	openAndFund(t, sc, "bob", 700)

	holders := sc.Holders(sid)
	held := map[raft.NodeID]bool{}
	for _, h := range holders {
		held[h] = true
	}
	var spare raft.NodeID
	for i := range 5 {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if !held[id] {
			spare = id
			break
		}
	}

	if err := sc.AddReplica(sid, spare); err != nil {
		t.Fatalf("first AddReplica: %v", err)
	}
	// The retry: same shard, same target.
	if err := sc.AddReplica(sid, spare); err == nil {
		t.Fatal("a repeated heal onto the same machine must be rejected, not applied twice")
	}

	count := 0
	for _, h := range sc.Holders(sid) {
		if h == spare {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("%s appears %d times in the replica set, want exactly 1", spare, count)
	}
}

// -------------------------------------------------------------- concurrent path

// Healing while writes are in flight must not lose or duplicate money. The
// committed balance has to reflect exactly the transfers that succeeded.
func TestHealingDuringConcurrentWritesConservesMoney(t *testing.T) {
	sc, err := NewPlacedCluster(1, 4, 3, 20260835)
	if err != nil {
		t.Fatal(err)
	}
	sc.Start()
	defer sc.Stop()

	sid := sc.Ring.Shards()[0]
	g := sc.Groups[sid]
	waitFor(t, 5*time.Second, "a leader", func() bool { return g.leader() != "" })

	openAndFund(t, sc, "carol", 1_000)

	holders := sc.Holders(sid)
	held := map[raft.NodeID]bool{}
	for _, h := range holders {
		held[h] = true
	}
	var spare raft.NodeID
	for i := range 4 {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if !held[id] {
			spare = id
		}
	}

	const deposits = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := int64(0)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range deposits {
			err := deposit(sc, "carol", 10, fmt.Sprintf("dep-%d", i))
			if err == nil {
				mu.Lock()
				accepted += 10
				mu.Unlock()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Heal in the middle of the write stream.
	time.Sleep(30 * time.Millisecond)
	if err := sc.AddReplica(sid, spare); err != nil {
		t.Fatalf("AddReplica during writes: %v", err)
	}

	wg.Wait()

	want := 1_000 + accepted
	waitFor(t, 10*time.Second, "the healed replica to converge", func() bool {
		return balanceOn(t, g, spare, "carol") == want
	})

	// Every replica must agree, and agree with what was actually accepted — no
	// invented money, none lost.
	for _, h := range sc.Holders(sid) {
		if got := balanceOn(t, g, h, "carol"); got != want {
			t.Fatalf("replica %s has carol=%d, want %d (accepted=%d)", h, got, want, accepted)
		}
	}

	leaderID := g.leader()
	if leaderID != "" {
		assertLogsMatch(t, g, leaderID, spare)
	}
}
