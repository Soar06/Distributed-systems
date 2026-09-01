package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Membership changes against a live cluster (§6, dissertation §4.1) — G6.
//
// raft/membership_test.go proves the rules in isolation. These prove the cluster
// KEEPS WORKING through a reconfiguration, which is the entire point: without
// membership changes, replacing one dead machine means stopping every node,
// editing a config, and starting again — full-cluster downtime to recover from a
// single-node failure.
//
// Per RULES.md rule 3: normal (add a node, remove a node), failure (remove the
// leader), and safety asserted throughout — all five Figure 3 properties must
// hold across every reconfiguration, because a botched change is exactly how two
// leaders get elected in one term.

// waitForConfig waits until every reachable node agrees on a configuration size.
func waitForConfig(t *testing.T, c *Cluster, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		agreed := true
		for _, id := range c.IDs {
			if len(c.Nodes[id].CurrentConfiguration().Servers) != want {
				agreed = false
				break
			}
		}
		if agreed {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// Removing a server must leave the cluster serving, with safety intact.
func TestClusterKeepsCommittingWhileRemovingAServer(t *testing.T) {
	c := NewCluster(4, 21)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	// Real traffic before the change.
	for i := range 5 {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("before-%d", i)), 5*time.Second); !ok {
			t.Fatalf("submit before-%d failed%s", i, c.View())
		}
	}
	if !c.WaitForCommit(5, 5*time.Second) {
		t.Fatalf("pre-change writes did not commit%s", c.View())
	}

	// Remove a follower.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			victim = id
			break
		}
	}
	if _, err := c.Nodes[leader].RemoveServer(victim); err != nil {
		t.Fatalf("RemoveServer(%s): %v", victim, err)
	}

	// The cluster must keep accepting writes THROUGH the change.
	for i := range 5 {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("during-%d", i)), 5*time.Second); !ok {
			t.Fatalf("the cluster stopped accepting writes during a reconfiguration%s", c.View())
		}
	}

	// The remaining members must converge on the smaller configuration.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cfg := c.Nodes[leader].CurrentConfiguration()
		if len(cfg.Servers) == 3 && !cfg.Contains(victim) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cfg := c.Nodes[leader].CurrentConfiguration()
	if len(cfg.Servers) != 3 || cfg.Contains(victim) {
		t.Fatalf("leader configuration is %v, want 3 servers without %s", cfg.Servers, victim)
	}

	// Safety across the whole episode. A botched membership change is exactly how
	// two leaders get elected in one term, so this is the assertion that matters.
	if err := c.CheckAll(); err != nil {
		t.Fatalf("safety violated across a membership change: %v%s", err, c.View())
	}
	t.Logf("removed %s; cluster kept committing and all Figure 3 properties hold%s",
		victim, c.View())
}

// Adding a server must not stall the cluster, and the new member must be
// replicated to.
func TestClusterKeepsCommittingWhileAddingAServer(t *testing.T) {
	// Five nodes registered on the network, but the cluster starts as four: the
	// fifth is the one being added. Registering it up front mirrors reality, where
	// the new machine is running and reachable before it joins.
	c := NewCluster(5, 22)

	// Start the cluster as a 4-server configuration.
	four := []raft.NodeID{c.IDs[0], c.IDs[1], c.IDs[2], c.IDs[3]}
	newcomer := c.IDs[4]
	for _, id := range c.IDs {
		c.Nodes[id].SetConfigurationForTest(four)
	}

	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeaderAmong(four, 5*time.Second)
	if !ok {
		t.Fatalf("no leader among the initial four%s", c.View())
	}

	for i := range 5 {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("pre-%d", i)), 5*time.Second); !ok {
			t.Fatalf("submit pre-%d failed%s", i, c.View())
		}
	}

	if _, err := c.Nodes[leader].AddServer(newcomer); err != nil {
		t.Fatalf("AddServer(%s): %v", newcomer, err)
	}

	// Writes must keep flowing through the change.
	for i := range 5 {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("post-%d", i)), 5*time.Second); !ok {
			t.Fatalf("the cluster stopped accepting writes while adding a server%s", c.View())
		}
	}

	// The newcomer must actually receive the log, not merely appear in a list.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.SMs[newcomer].AppliedCopy()) >= 10 {
			if err := c.CheckAll(); err != nil {
				t.Fatalf("safety violated after adding a server: %v%s", err, c.View())
			}
			t.Logf("added %s; it caught up to %d entries and safety holds%s",
				newcomer, len(c.SMs[newcomer].AppliedCopy()), c.View())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the added server %s holds %d entries, want at least 10 — it is in the "+
		"configuration but is not being replicated to%s",
		newcomer, len(c.SMs[newcomer].AppliedCopy()), c.View())
}

// Removing the CURRENT LEADER must work: it serves until the change commits,
// then steps down, and the survivors elect a replacement.
func TestRemovingTheLeaderTransfersLeadership(t *testing.T) {
	c := NewCluster(3, 23)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	for i := range 3 {
		c.SubmitWithRetry(t, []byte(fmt.Sprintf("x-%d", i)), 5*time.Second)
	}

	if _, err := c.Nodes[leader].RemoveServer(leader); err != nil {
		t.Fatalf("RemoveServer(self): %v", err)
	}

	// It must step down once the change commits, and someone else must take over.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if c.Nodes[leader].SteppedDownAfterRemoval() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !c.Nodes[leader].SteppedDownAfterRemoval() {
		t.Fatalf("the removed leader %s never stepped down%s", leader, c.View())
	}

	// A survivor must become leader and keep serving.
	var survivors []raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			survivors = append(survivors, id)
		}
	}
	if _, ok := c.WaitForLeaderAmong(survivors, 8*time.Second); !ok {
		t.Fatalf("the surviving members never elected a replacement leader%s", c.View())
	}

	if err := c.CheckAll(); err != nil {
		t.Fatalf("safety violated after removing the leader: %v%s", err, c.View())
	}
	t.Logf("leader %s removed itself, stepped down, and the survivors took over%s",
		leader, c.View())
}

// Election Safety must hold across a reconfiguration under chaos.
//
// This is the property single-server changes exist to preserve: two disjoint
// majorities must never exist, so two leaders must never be elected in one term.
func TestSafetyHoldsAcrossReconfigurationUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos run; skipped under -short")
	}

	c := NewCluster(5, 24)
	c.Net.SetDropRate(0.05)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(8 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	for i := range 10 {
		c.SubmitWithRetry(t, []byte(fmt.Sprintf("chaos-%d", i)), 8*time.Second)
	}

	// Remove one server while the network drops messages.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			victim = id
			break
		}
	}
	if _, err := c.Nodes[leader].RemoveServer(victim); err != nil {
		t.Logf("RemoveServer refused (leadership may have moved): %v", err)
	}

	for i := range 10 {
		c.SubmitWithRetry(t, []byte(fmt.Sprintf("after-%d", i)), 8*time.Second)
	}

	// Whatever happened, safety must hold.
	if err := c.CheckAll(); err != nil {
		t.Fatalf("safety violated across a reconfiguration under 5%% message loss: "+
			"%v%s", err, c.View())
	}
	if !waitForConfig(t, c, 4, 3*time.Second) {
		t.Logf("nodes have not all converged on the 4-server configuration yet%s", c.View())
	}
	t.Logf("reconfiguration under message loss: all Figure 3 properties hold%s", c.View())
}
