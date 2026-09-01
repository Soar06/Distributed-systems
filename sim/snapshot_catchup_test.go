package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// InstallSnapshot end to end: a follower behind the leader's compacted prefix
// must be caught up by a snapshot (§7 — a G3 gap found during the G5 audit).
//
// The gap had two halves. InstallSnapshot was implemented as a RECEIVER but
// nothing ever sent one, so a lagging follower could never converge. And the
// missing send path was not merely absent: replicateTo computed a NEGATIVE slice
// index for a follower below the snapshot boundary and panicked the leader.
// A follower falling behind a compacted leader is routine — it crashes and
// restarts, or is briefly partitioned — so an ordinary event crashed the process.
//
// Per RULES.md rule 3: normal (a lagging follower converges), failure (the peer
// is down while the leader compacts, then returns), and duplicate delivery (the
// simulator may deliver the install twice, which must be a no-op).

// compactReachable compacts every node that is not crashed.
func compactReachable(t *testing.T, c *Cluster, threshold int, skip raft.NodeID) int {
	t.Helper()
	n := 0
	for _, id := range c.IDs {
		if id == skip {
			continue
		}
		did, err := c.Nodes[id].MaybeCompact(threshold)
		if err != nil {
			t.Fatalf("compact %s: %v", id, err)
		}
		if did {
			n++
		}
	}
	return n
}

// The core flow: a node that was down while the cluster compacted must be caught
// up by a snapshot when it returns.
func TestCrashedFollowerCatchesUpBySnapshotAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	c := NewClusterWithStorage(t, 3, 7, dir)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	// Crash a follower, so it misses everything that follows.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			victim = id
			break
		}
	}
	c.Net.Crash(victim)

	const writes = 30
	for i := range writes {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("catchup-%d", i)), 5*time.Second); !ok {
			t.Fatalf("submit %d failed%s", i, c.View())
		}
	}

	// The surviving majority commits and then compacts past what the victim holds.
	var survivors []raft.NodeID
	for _, id := range c.IDs {
		if id != victim {
			survivors = append(survivors, id)
		}
	}
	if !c.WaitForCommit(writes, 5*time.Second, survivors...) {
		t.Fatalf("the surviving majority did not commit %d entries%s", writes, c.View())
	}
	if n := compactReachable(t, c, 5, victim); n == 0 {
		t.Fatalf("nothing compacted after %d writes%s", writes, c.View())
	}

	before := len(c.SMs[victim].AppliedCopy())
	if before >= writes {
		t.Fatalf("the crashed node applied %d entries; it was not actually down", before)
	}

	// Bring it back. Its nextIndex is below the leader's snapshot boundary, so
	// AppendEntries cannot carry what it needs.
	c.Net.Restore(victim)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.SMs[victim].AppliedCopy()) >= writes {
			t.Logf("follower %s went from %d to %d entries via InstallSnapshot",
				victim, before, writes)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("follower %s never caught up: it holds %d of %d entries. The leader "+
		"compacted past its nextIndex, so AppendEntries cannot carry what it needs "+
		"and a snapshot must be sent%s",
		victim, len(c.SMs[victim].AppliedCopy()), writes, c.View())
}

// The leader must not crash when a follower is below its snapshot boundary.
//
// This is the panic itself, exercised through a partition rather than by reaching
// into internals: a partitioned node's nextIndex is walked back by failed
// consistency checks until it falls below the compacted prefix.
func TestCompactedLeaderSurvivesPartitionedFollower(t *testing.T) {
	dir := t.TempDir()
	c := NewClusterWithStorage(t, 3, 11, dir)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			victim = id
			break
		}
	}

	// Isolate one node, commit past it, compact.
	c.Net.Partition([]raft.NodeID{victim})
	for i := range 25 {
		c.SubmitWithRetry(t, []byte(fmt.Sprintf("part-%d", i)), 5*time.Second)
	}
	compactReachable(t, c, 5, victim)

	// Heal. The leader now replicates to a follower whose nextIndex is below the
	// snapshot boundary — the case that panicked.
	c.Net.Heal()

	// The cluster must keep accepting writes rather than dying.
	if _, ok := c.SubmitWithRetry(t, []byte("after-heal"), 5*time.Second); !ok {
		t.Fatalf("the cluster stopped accepting writes after healing a partition "+
			"against a compacted log%s", c.View())
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.CheckAll(); err == nil && len(c.SMs[victim].AppliedCopy()) >= 26 {
			t.Logf("cluster converged after healing; all Figure 3 properties hold%s", c.View())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := c.CheckAll(); err != nil {
		t.Fatalf("safety violated after snapshot catch-up: %v%s", err, c.View())
	}
	t.Fatalf("victim holds %d entries, want at least 26%s",
		len(c.SMs[victim].AppliedCopy()), c.View())
}

// Duplicate delivery of an install must be a no-op.
//
// The simulator can deliver any RPC twice, and a snapshot is no exception. A
// second install of the same snapshot must not rewind a state machine that has
// moved on since.
func TestDuplicateSnapshotDeliveryIsHarmless(t *testing.T) {
	dir := t.TempDir()
	c := NewClusterWithStorage(t, 3, 13, dir)
	c.Net.SetDuplicateRate(1.0) // every RPC delivered twice
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(5 * time.Second); !ok {
		t.Fatalf("no leader%s", c.View())
	}

	const writes = 20
	for i := range writes {
		c.SubmitWithRetry(t, []byte(fmt.Sprintf("dup-%d", i)), 5*time.Second)
	}
	if !c.WaitForCommit(writes, 8*time.Second) {
		t.Fatalf("cluster did not commit %d entries with every RPC duplicated%s",
			writes, c.View())
	}

	for _, id := range c.IDs {
		c.Nodes[id].MaybeCompact(5)
	}

	// Every safety property must still hold: duplicate installs must not have
	// applied anything twice or rewound anyone.
	if err := c.CheckAll(); err != nil {
		t.Fatalf("safety violated with duplicated snapshot delivery: %v%s", err, c.View())
	}
}
