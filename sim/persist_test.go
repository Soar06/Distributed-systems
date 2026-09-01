package sim

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/storage"
)

// Crash-and-restart tests. These were impossible before persistence existed:
// without durable state a restarted node is indistinguishable from a fresh one,
// and the bugs persistence prevents cannot be demonstrated.

// A restarted node must remember its term, its vote, and its log. Forgetting a
// vote is the classic path to two leaders in one term.
func TestRestartPreservesTermVoteAndLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "n1.wal")

	walPath := path
	st := storage.OpenRaftState(walPath, nil)

	peers := []raft.NodeID{"n1", "n2", "n3"}
	s := raft.NewServerWith("n1", peers, &CountingSM{}, nil, raft.DefaultConfig(), 1)
	s.SetStorage(st)

	// Grant a vote and accept some entries, as a live follower would.
	s.RequestVote(raft.RequestVoteArgs{Term: 5, CandidateID: "n2"})
	s.AppendEntries(raft.AppendEntriesArgs{
		Term: 5, LeaderID: "n2", PrevLogIndex: 0,
		Entries: []raft.LogEntry{
			{Term: 5, Index: 1, Command: []byte("a")},
			{Term: 5, Index: 2, Command: []byte("b")},
		},
	})

	wantTerm := s.CurrentTerm()
	wantLog := s.LogEntries()

	// "Restart": a brand-new Server object reading the same WAL.
	walPath2 := path

	s2 := raft.NewServerWith("n1", peers, &CountingSM{}, nil, raft.DefaultConfig(), 1)
	s2.SetStorage(storage.OpenRaftState(walPath2, nil))
	if err := s2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got := s2.CurrentTerm(); got != wantTerm {
		t.Fatalf("term after restart = %d, want %d — a forgotten term can re-elect", got, wantTerm)
	}
	gotLog := s2.LogEntries()
	if len(gotLog) != len(wantLog) {
		t.Fatalf("log length after restart = %d, want %d", len(gotLog), len(wantLog))
	}
	for i := range wantLog {
		if gotLog[i].Term != wantLog[i].Term || string(gotLog[i].Command) != string(wantLog[i].Command) {
			t.Fatalf("log entry %d changed across restart: %q/t%d vs %q/t%d",
				i, gotLog[i].Command, gotLog[i].Term, wantLog[i].Command, wantLog[i].Term)
		}
	}
}

// THE Election Safety scenario. A node votes, crashes, and restarts. It must
// still refuse to vote for a different candidate in the same term — otherwise two
// candidates can each collect a majority and both become leader.
func TestRestartedNodeDoesNotVoteTwiceInSameTerm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "voter.wal")

	walPath := path
	peers := []raft.NodeID{"n1", "n2", "n3"}

	s := raft.NewServerWith("n1", peers, &CountingSM{}, nil, raft.DefaultConfig(), 1)
	s.SetStorage(storage.OpenRaftState(walPath, nil))

	first := s.RequestVote(raft.RequestVoteArgs{Term: 7, CandidateID: "n2"})
	if !first.VoteGranted {
		t.Fatal("first vote should be granted")
	}

	// Crash and restart.
	walPath2 := path
	s2 := raft.NewServerWith("n1", peers, &CountingSM{}, nil, raft.DefaultConfig(), 1)
	s2.SetStorage(storage.OpenRaftState(walPath2, nil))
	if err := s2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// A different candidate asks for a vote in the SAME term.
	second := s2.RequestVote(raft.RequestVoteArgs{Term: 7, CandidateID: "n3"})
	if second.VoteGranted {
		t.Fatal("Election Safety violated: restarted node voted twice in term 7 — " +
			"the vote was not durable before the reply")
	}
}

// Committed entries must survive a full-cluster restart. This is the "the bank
// does not forget your deposit when the power goes out" test.
func TestClusterSurvivesFullRestart(t *testing.T) {
	dir := t.TempDir()

	c := NewClusterWithStorage(t, 3, 42, dir)
	c.Start()

	if _, ok := c.WaitForLeader(3 * time.Second); !ok {
		t.Fatalf("no leader elected%s", c.View())
	}
	// Submitted through leadership changes rather than to one node found earlier:
	// an election between finding the leader and submitting is legitimate Raft
	// behaviour, and under load it happens often enough to matter.
	for _, cmd := range []string{"tx1", "tx2", "tx3"} {
		if _, ok := c.SubmitWithRetry(t, []byte(cmd), 5*time.Second); !ok {
			t.Fatalf("could not submit %s to any leader%s", cmd, c.View())
		}
	}
	if !c.WaitForCommit(3, 5*time.Second) {
		t.Fatalf("initial commit failed%s%s", c.View(), c.View().LogsString())
	}
	c.Stop()
	c.CloseStorage()

	// Every node restarts from disk.
	c2 := NewClusterWithStorage(t, 3, 99, dir) // different seed: recovery must not depend on it
	if err := c2.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	defer c2.CloseStorage()

	// Logs must have survived, before any election happens.
	//
	// Note: the log also contains blank no-op entries, one per leader election
	// (§8). Those carry a nil command and are skipped here — what matters is that
	// the real commands survived, in order.
	for _, id := range c2.IDs {
		log := c2.Nodes[id].LogEntries()

		var cmds []string
		for _, e := range log[1:] { // skip the sentinel
			if e.Command != nil {
				cmds = append(cmds, string(e.Command))
			}
		}
		want := []string{"tx1", "tx2", "tx3"}
		if len(cmds) != len(want) {
			t.Fatalf("%s has %d real commands after restart, want %d%s",
				id, len(cmds), len(want), c2.View().LogsString())
		}
		for i := range want {
			if cmds[i] != want[i] {
				t.Fatalf("%s command %d = %q, want %q", id, i, cmds[i], want[i])
			}
		}
	}

	// And the cluster must come back to life and re-apply them.
	c2.Start()
	defer c2.Stop()

	if _, ok := c2.WaitForLeader(3 * time.Second); !ok {
		t.Fatalf("no leader after restart%s", c2.View())
	}
	if !c2.WaitForCommit(3, 3*time.Second) {
		t.Fatalf("entries not re-applied after restart%s%s", c2.View(), c2.View().LogsString())
	}
	checkAll(t, c2)
}

// A node that crashes and rejoins must catch up from the leader rather than
// diverging.
func TestCrashedNodeRejoinsAndCatchesUp(t *testing.T) {
	dir := t.TempDir()
	c := NewClusterWithStorage(t, 3, 7, dir)
	c.Start()
	defer c.Stop()
	defer c.CloseStorage()

	leader, ok := c.WaitForLeader(3 * time.Second)
	if !ok {
		t.Fatalf("no leader%s", c.View())
	}

	// Pick a victim that is not the leader.
	var victim raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			victim = id
			break
		}
	}

	c.Net.Crash(victim)

	// The remaining majority keeps working while the victim is down.
	for _, cmd := range []string{"while-down-1", "while-down-2"} {
		c.Nodes[leader].Submit([]byte(cmd))
	}
	survivors := []raft.NodeID{}
	for _, id := range c.IDs {
		if id != victim {
			survivors = append(survivors, id)
		}
	}
	if !c.WaitForCommit(2, 3*time.Second, survivors...) {
		t.Fatalf("majority could not commit with one node down%s", c.View())
	}

	// The victim rejoins and must catch up.
	c.Net.Restore(victim)

	if !c.WaitForCommit(2, 5*time.Second, victim) {
		t.Fatalf("rejoined node did not catch up%s%s", c.View(), c.View().LogsString())
	}
	checkAll(t, c)
}
