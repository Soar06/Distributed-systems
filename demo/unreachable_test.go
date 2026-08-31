package demo

import (
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Reading a shard whose replicas are down or lagging.
//
// Found by using the demo rather than by a test: after killing several nodes the
// dashboard happily showed balances for a shard whose every replica was crashed,
// and counted them in the bank's total. The numbers came from
// ShardGroup.Machine(), which falls back to IDs[0] when there is no leader —
// whichever node happens to be first, crashed or not, current or not.
//
// That is the degraded-quorum blindness G5 removed from the backend,
// reintroduced in the view. A total that silently includes unreachable money is
// worse than one that visibly drops: the first looks correct and is not.

// Every replica down: the shard must report unreachable and contribute nothing.
func TestFullyCrashedShardIsUnreachableNotStale(t *testing.T) {
	c := newTestCluster(t, 2, 3)

	if _, err := c.Open("alice", 10000); err != nil {
		t.Fatalf("open: %v", err)
	}

	before := c.Snapshot()
	// Find the shard holding alice, and kill all of it.
	var target string
	for _, s := range before.Shards {
		if _, ok := s.Accounts["alice"]; ok {
			target = s.ID
		}
	}
	if target == "" {
		t.Fatal("alice is on no shard")
	}
	for _, s := range before.Shards {
		if s.ID != target {
			continue
		}
		for _, n := range s.Nodes {
			if err := c.Kill(raft.NodeID(n.ID)); err != nil {
				t.Fatalf("kill %s: %v", n.ID, err)
			}
		}
	}

	v := c.Snapshot()
	for _, s := range v.Shards {
		if s.ID != target {
			continue
		}
		if !s.Unreachable {
			t.Fatal("a shard with every replica crashed is not marked unreachable")
		}
		if len(s.Accounts) != 0 {
			t.Fatalf("a fully crashed shard reported balances %v — read from a dead "+
				"node. Showing a crashed replica's ledger as the shard's balance is "+
				"exactly the degraded state that looks healthy from outside", s.Accounts)
		}
	}

	if v.Total != 0 {
		t.Fatalf("total money = %d with the only funded shard unreachable, want 0 — "+
			"a total that silently includes money nobody can reach looks correct and "+
			"is not", v.Total)
	}
	if v.Healthy {
		t.Fatal("the cluster reports healthy with a shard unreachable")
	}
}

// One live replica: readable (staler, but real) and NOT ready to commit.
//
// The distinction matters. "Cannot commit" and "cannot read at all" are different
// failures calling for different responses, and collapsing them would make a
// recoverable shard look destroyed.
func TestPartiallyCrashedShardIsReadableButNotReady(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if _, err := c.Open("alice", 7500); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Wait until every replica has applied the account, so the survivor genuinely
	// HAS the data.
	//
	// Without this wait the test is wrong rather than the code: a follower that has
	// not yet applied the entry legitimately does not hold the account, and
	// reporting no accounts from it is correct. Observed directly — the surviving
	// replica sat at applied=1 while the others were at 2, so an empty read was the
	// honest answer.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		caught := true
		for _, n := range c.Snapshot().Shards[0].Nodes {
			if n.Applied < 2 {
				caught = false
			}
		}
		if caught {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	nodes := c.Snapshot().Shards[0].Nodes
	// Kill two of three: quorum is gone, one replica still answers.
	for i := 0; i < 2; i++ {
		if err := c.Kill(raft.NodeID(nodes[i].ID)); err != nil {
			t.Fatalf("kill: %v", err)
		}
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := c.Snapshot().Shards[0]
		if s.Ready {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if s.Unreachable {
			t.Fatal("a shard with one live replica is marked unreachable; it can " +
				"still be read, just not committed to")
		}
		if len(s.Accounts) == 0 {
			t.Fatal("a shard with one live replica reported no accounts; a stale read " +
				"is honest, refusing to read at all is not")
		}
		if s.Reason == "" {
			t.Fatal("a not-ready shard gives no reason")
		}
		return
	}
	t.Fatal("the shard never reported not-ready after losing its majority")
}

// The view must read from the most advanced LIVE replica, not from whichever is
// first in the id list.
func TestSnapshotReadsFromTheFreshestLiveReplica(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if _, err := c.Open("alice", 1000); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Kill one follower, then commit — it now lags the others.
	var victim string
	for _, n := range c.Snapshot().Shards[0].Nodes {
		if n.Role != "Leader" {
			victim = n.ID
			break
		}
	}
	if err := c.Kill(raft.NodeID(victim)); err != nil {
		t.Fatalf("kill: %v", err)
	}
	for i := range 3 {
		c.Transact("deposit", string(rune('a'+i))+"-dep", "", "alice", 500)
	}

	// Revive it: it is live but behind until it catches up.
	if err := c.Revive(raft.NodeID(victim)); err != nil {
		t.Fatalf("revive: %v", err)
	}

	// Whatever moment we sample, the reported balance must never be BELOW what a
	// committed write already established — reading from a lagging replica when a
	// more current one is live would show money going backwards.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v := c.Snapshot()
		if bal, ok := v.Shards[0].Accounts["alice"]; ok && bal < 2500 {
			t.Fatalf("reported balance %d is below the 2500 already committed — the "+
				"view read from a lagging replica while a current one was live", bal)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
