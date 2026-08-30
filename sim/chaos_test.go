package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Chaos tests: Raft under deliberately injected failure.
//
// Methodology per learn/READING_LIST.md §5. Each test states a steady-state
// hypothesis (Figure 3 holds, no committed entry is lost), injects a real-world
// fault, and asserts the hypothesis survived. Required by RULES.md rule 3.
//
// Every run is seeded, so a failure is reproducible from the seed in its name.

// checkAll fails the test with a rendered cluster view if any safety property is
// violated. The view is what makes a failure diagnosable rather than merely
// detected.
func checkAll(t *testing.T, c *Cluster) {
	t.Helper()
	if err := c.CheckAll(); err != nil {
		t.Fatalf("%v\n%s%s", err, c.View(), c.View().LogsString())
	}
}

// --- Leader election ------------------------------------------------------

func TestElectsSingleLeader(t *testing.T) {
	c := NewCluster(3, 1)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(2 * time.Second)
	if !ok {
		t.Fatalf("no leader elected within 2s%s", c.View())
	}
	t.Logf("elected %s%s", leader, c.View())
	checkAll(t, c)
}

func TestElectsLeaderInFiveNodeCluster(t *testing.T) {
	c := NewCluster(5, 2)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatalf("no leader elected in 5-node cluster%s", c.View())
	}
	checkAll(t, c)
}

// A leader must remain stable: heartbeats should stop followers from starting
// pointless elections. Term churn under no faults means the timing inequality
// (§5.2) is being violated.
func TestLeaderRemainsStable(t *testing.T) {
	c := NewCluster(3, 3)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	termAfterElection := c.Nodes[leader].CurrentTerm()

	time.Sleep(500 * time.Millisecond)

	if got := c.Nodes[leader].Role(); got != raft.Leader {
		t.Fatalf("leader %s changed role to %v while healthy%s", leader, got, c.View())
	}
	if got := c.Nodes[leader].CurrentTerm(); got != termAfterElection {
		t.Fatalf("term churned %d -> %d with no faults: heartbeats are not suppressing elections%s",
			termAfterElection, got, c.View())
	}
	checkAll(t, c)
}

// --- Replication ----------------------------------------------------------

func TestReplicatesToAllFollowers(t *testing.T) {
	c := NewCluster(3, 4)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}

	// Submitted through leadership changes: an election between finding the leader
	// and submitting is legitimate Raft behaviour, and asserting it did not happen
	// is asserting a property Raft does not offer.
	for i := range 5 {
		if _, ok := c.SubmitWithRetry(t, []byte(fmt.Sprintf("cmd%d", i)), 5*time.Second); !ok {
			t.Fatalf("could not submit cmd%d to any leader%s", i, c.View())
		}
	}

	if !c.WaitForCommit(5, 5*time.Second) {
		t.Fatalf("not all nodes applied 5 commands%s%s", c.View(), c.View().LogsString())
	}
	checkAll(t, c)
}

// A non-leader must reject writes and redirect (NOW.md): all writes go through
// the single current leader.
func TestFollowerRejectsSubmit(t *testing.T) {
	c := NewCluster(3, 5)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}

	for _, id := range c.IDs {
		if id == leader {
			continue
		}
		if _, _, ok := c.Nodes[id].Submit([]byte("should be rejected")); ok {
			t.Fatalf("follower %s accepted a write; only the leader may accept writes", id)
		}
	}
}

// --- Chaos Monkey: kill the leader ---------------------------------------

// The original Chaos Monkey fault: terminate an instance. Killing the leader
// mid-operation is the canonical Raft failure, and NOW.md names it explicitly.
func TestChaosMonkey_KillLeaderElectsNewOne(t *testing.T) {
	c := NewCluster(3, 6)
	c.Start()
	defer c.Stop()

	old, ok := c.WaitForLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	for i := range 3 {
		c.Nodes[old].Submit([]byte(fmt.Sprintf("before%d", i)))
	}
	c.WaitForCommit(3, 2*time.Second)
	c.RecordHistory()

	// Chaos: the leader dies.
	c.Net.Crash(old)
	c.Nodes[old].Stop()

	// Steady-state hypothesis: the remaining majority elects a new leader.
	deadline := time.Now().Add(3 * time.Second)
	var newLeader raft.NodeID
	for time.Now().Before(deadline) {
		for id := range c.Leaders() {
			if id != old {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatalf("no new leader after killing %s%s", old, c.View())
	}

	// And the new leader can still accept writes — the cluster is still usable.
	if _, _, ok := c.Nodes[newLeader].Submit([]byte("after-failover")); !ok {
		t.Fatalf("new leader %s cannot accept writes", newLeader)
	}

	survivors := []raft.NodeID{}
	for _, id := range c.IDs {
		if id != old {
			survivors = append(survivors, id)
		}
	}
	if !c.WaitForCommit(4, 2*time.Second, survivors...) {
		t.Fatalf("survivors did not commit the post-failover write%s%s", c.View(), c.View().LogsString())
	}

	t.Logf("failover %s -> %s%s", old, newLeader, c.View())
	checkAll(t, c)
}

// Committed entries must survive a leader crash. This is the "no lost money"
// requirement in its rawest form.
func TestChaosMonkey_CommittedEntriesSurviveLeaderCrash(t *testing.T) {
	c := NewCluster(5, 7)
	c.Start()
	defer c.Stop()

	old, ok := c.WaitForLeader(2 * time.Second)
	if !ok {
		t.Fatal("no leader elected")
	}
	for i := range 4 {
		c.Nodes[old].Submit([]byte(fmt.Sprintf("durable%d", i)))
	}
	if !c.WaitForCommit(4, 2*time.Second) {
		t.Fatalf("initial replication failed%s", c.View())
	}
	c.RecordHistory()

	c.Net.Crash(old)
	c.Nodes[old].Stop()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.Leaders()) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Every surviving node must still hold all four committed commands.
	for _, id := range c.IDs {
		if id == old {
			continue
		}
		applied := c.SMs[id].AppliedCopy()
		if len(applied) < 4 {
			t.Fatalf("%s lost committed entries: applied %v%s", id, applied, c.View())
		}
		for i := range 4 {
			want := fmt.Sprintf("durable%d", i)
			if applied[i] != want {
				t.Fatalf("%s applied %q at position %d, want %q — committed data changed",
					id, applied[i], i+1, want)
			}
		}
	}
	checkAll(t, c)
}

// --- Chaos Gorilla: partition the network --------------------------------

// A minority partition must not be able to elect a leader or commit anything.
// This is CAP made concrete: the minority side sacrifices availability to
// preserve consistency.
func TestChaosGorilla_MinorityPartitionCannotCommit(t *testing.T) {
	c := NewCluster(5, 8)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}

	// Split 3 | 2. The minority side can talk among themselves but not to the
	// majority.
	majority := []raft.NodeID{"n1", "n2", "n3"}
	minority := []raft.NodeID{"n4", "n5"}
	c.Net.Partition(majority, minority)

	time.Sleep(600 * time.Millisecond)

	// The minority must not have a leader that can commit. Its nodes may cycle
	// as candidates forever — that is correct, and is exactly why a minority
	// partition is unavailable rather than inconsistent.
	for _, id := range minority {
		s := c.Nodes[id]
		if s.Role() == raft.Leader {
			// A leader here would be a split brain unless it cannot commit.
			if _, _, ok := s.Submit([]byte("split-brain")); ok {
				time.Sleep(200 * time.Millisecond)
				if s.CommitIndex() > 0 && len(c.SMs[id].AppliedCopy()) > 0 {
					t.Fatalf("minority node %s committed an entry: split brain%s%s",
						id, c.View(), c.View().LogsString())
				}
			}
		}
	}
	checkAll(t, c)
}

// The majority side must keep working through a partition — that is the whole
// point of tolerating f failures in a 2f+1 cluster.
func TestChaosGorilla_MajorityPartitionKeepsCommitting(t *testing.T) {
	c := NewCluster(5, 9)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}

	majority := []raft.NodeID{"n1", "n2", "n3"}
	minority := []raft.NodeID{"n4", "n5"}
	c.Net.Partition(majority, minority)

	// Wait for the majority side to settle on a leader of its own.
	var maj raft.NodeID
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range majority {
			if c.Nodes[id].Role() == raft.Leader {
				maj = id
			}
		}
		if maj != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if maj == "" {
		t.Fatalf("majority side elected no leader%s", c.View())
	}

	if _, _, ok := c.Nodes[maj].Submit([]byte("majority-write")); !ok {
		t.Fatalf("majority leader %s rejected a write", maj)
	}
	if !c.WaitForCommit(1, 2*time.Second, majority...) {
		t.Fatalf("majority could not commit during partition%s%s", c.View(), c.View().LogsString())
	}
	checkAll(t, c)
}

// Heal the partition and the isolated nodes must catch up, converging on the
// majority's history rather than their own.
func TestChaosGorilla_HealedPartitionConverges(t *testing.T) {
	c := NewCluster(5, 10)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(2 * time.Second); !ok {
		t.Fatal("no leader elected")
	}

	majority := []raft.NodeID{"n1", "n2", "n3"}
	minority := []raft.NodeID{"n4", "n5"}
	c.Net.Partition(majority, minority)

	var maj raft.NodeID
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range majority {
			if c.Nodes[id].Role() == raft.Leader {
				maj = id
			}
		}
		if maj != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if maj == "" {
		t.Fatalf("majority side elected no leader%s", c.View())
	}

	for i := range 3 {
		c.Nodes[maj].Submit([]byte(fmt.Sprintf("during-partition%d", i)))
	}
	c.WaitForCommit(3, 2*time.Second, majority...)
	c.RecordHistory()

	// Heal.
	c.Net.Heal()

	if !c.WaitForCommit(3, 4*time.Second) {
		t.Fatalf("nodes did not converge after healing%s%s", c.View(), c.View().LogsString())
	}
	t.Logf("converged after heal%s", c.View())
	checkAll(t, c)
}

// --- Latency Monkey: lossy and duplicating network ------------------------

// Raft must make progress despite packet loss — RPCs are retried by the next
// heartbeat round.
func TestLatencyMonkey_ProgressUnderPacketLoss(t *testing.T) {
	c := NewCluster(3, 11)
	c.Net.SetDropRate(0.2)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader elected under 20%% packet loss%s", c.View())
	}

	for i := range 3 {
		c.Nodes[leader].Submit([]byte(fmt.Sprintf("lossy%d", i)))
	}

	// A majority must still converge, even if a straggler lags.
	if !c.WaitForCommit(3, 5*time.Second, leader) {
		t.Fatalf("leader could not commit under packet loss%s%s", c.View(), c.View().LogsString())
	}
	checkAll(t, c)
}

// Duplicated RPCs must not double-apply commands. This is the retry flow rule 3
// requires, exercised at the cluster level rather than the handler level.
func TestLatencyMonkey_DuplicatedRPCsDoNotDoubleApply(t *testing.T) {
	c := NewCluster(3, 12)
	c.Net.SetDuplicateRate(0.5)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatalf("no leader elected%s", c.View())
	}

	const n = 4
	for i := range n {
		c.Nodes[leader].Submit([]byte(fmt.Sprintf("once%d", i)))
	}
	if !c.WaitForCommit(n, 3*time.Second) {
		t.Fatalf("did not converge%s%s", c.View(), c.View().LogsString())
	}

	time.Sleep(300 * time.Millisecond)

	// Each command must appear exactly once on every node.
	for _, id := range c.IDs {
		applied := c.SMs[id].AppliedCopy()
		counts := make(map[string]int)
		for _, cmd := range applied {
			counts[cmd]++
		}
		for i := range n {
			cmd := fmt.Sprintf("once%d", i)
			if counts[cmd] != 1 {
				t.Fatalf("%s applied %q %d times, want exactly 1 — duplicate RPC double-applied%s",
					id, cmd, counts[cmd], c.View().LogsString())
			}
		}
	}
	checkAll(t, c)
}

// --- Combined chaos -------------------------------------------------------

// The full Simian Army run: repeated random faults while writes continue.
// Whatever happens, Figure 3 must hold at every checkpoint.
func TestSimianArmy_RepeatedFaultsPreserveSafety(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long chaos run in -short mode")
	}

	const seed = 13
	c := NewCluster(5, seed)
	c.Net.SetDropRate(0.05)
	c.Net.SetDuplicateRate(0.1)
	c.Start()
	defer c.Stop()

	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatalf("no initial leader (seed %d)%s", seed, c.View())
	}

	writes := 0
	for round := range 6 {
		// Write whatever the current leader will accept.
		for id := range c.Leaders() {
			if _, _, ok := c.Nodes[id].Submit([]byte(fmt.Sprintf("r%d", round))); ok {
				writes++
			}
		}

		// Inject a fault: alternate crashing a node and partitioning.
		victim := c.IDs[round%len(c.IDs)]
		if round%2 == 0 {
			c.Net.Crash(victim)
		} else {
			c.Net.Partition([]raft.NodeID{c.IDs[0], c.IDs[1], c.IDs[2]},
				[]raft.NodeID{c.IDs[3], c.IDs[4]})
		}

		time.Sleep(250 * time.Millisecond)
		c.RecordHistory()
		if err := c.CheckAll(); err != nil {
			t.Fatalf("seed %d round %d: %v%s%s", seed, round, err, c.View(), c.View().LogsString())
		}

		// Recover.
		c.Net.Restore(victim)
		c.Net.Heal()
		time.Sleep(250 * time.Millisecond)
		c.RecordHistory()
		if err := c.CheckAll(); err != nil {
			t.Fatalf("seed %d round %d after heal: %v%s%s", seed, round, err, c.View(), c.View().LogsString())
		}
	}

	t.Logf("survived 6 chaos rounds, %d writes accepted%s", writes, c.View())
	checkAll(t, c)
}

// --- Latency: the timing class of bug the old sim could not see ----------

// The simulator delivered RPCs instantly and synchronously, so an entire class
// of timing bug was invisible: head-of-line blocking, shared-connection aborts,
// and an RPC timeout set above the election timeout all require real delay to
// manifest.
//
// This runs the cluster with realistic one-way latency and asserts the same
// Figure 3 properties still hold.
func TestSafetyUnderRealisticLatency(t *testing.T) {
	c := NewCluster(5, 21)
	// 2-8ms one-way, well inside the 60-120ms election timeout — a healthy LAN.
	c.Net.SetLatency(2*time.Millisecond, 8*time.Millisecond)
	c.Start()
	defer c.Stop()

	leader, ok := c.WaitForLeader(5 * time.Second)
	if !ok {
		t.Fatalf("no leader elected under realistic latency%s", c.View())
	}

	for i := range 10 {
		c.Nodes[leader].Submit([]byte(fmt.Sprintf("lat%d", i)))
	}
	if !c.WaitForCommit(10, 5*time.Second) {
		t.Fatalf("did not converge under latency%s%s", c.View(), c.View().LogsString())
	}
	checkAll(t, c)
}

// With latency ABOVE the election timeout, the cluster should struggle to keep a
// stable leader — the §5.2 inequality broadcastTime << electionTimeout is
// violated. Safety must hold regardless: elections may churn, but no two leaders
// in one term and no divergent logs.
func TestSafetyHoldsWhenTimingInequalityIsViolated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow timing test in -short mode")
	}
	c := NewCluster(3, 22)
	// 40-90ms one-way against a 60-120ms election timeout: a round trip can
	// exceed the timeout, which is exactly the misconfiguration that used to be
	// shipped as a hardcoded 500ms RPC timeout against a 150-300ms election.
	c.Net.SetLatency(40*time.Millisecond, 90*time.Millisecond)
	c.Start()
	defer c.Stop()

	// Availability is NOT asserted here — losing it is the expected consequence.
	// Safety is.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.CheckElectionSafety(); err != nil {
			t.Fatalf("%v%s", err, c.View())
		}
		if err := c.CheckLogMatching(); err != nil {
			t.Fatalf("%v%s%s", err, c.View(), c.View().LogsString())
		}
		time.Sleep(50 * time.Millisecond)
	}

	terms := make(map[raft.NodeID]raft.Term)
	for _, id := range c.IDs {
		terms[id] = c.Nodes[id].CurrentTerm()
	}
	t.Logf("under a violated timing inequality, terms churned to %v — safety held%s",
		terms, c.View())
	checkAll(t, c)
}
