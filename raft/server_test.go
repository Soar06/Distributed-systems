package raft

import (
	"fmt"
	"testing"
)

// Tests for the Figure 2 RPC receiver rules and the Figure 3 safety properties.
//
// Per Agents/RULES.md rule 3, each feature is exercised across multiple flows —
// normal, failure, concurrent, and retry — and asserted against the paper's rules
// rather than only against end results.

// countingSM is a trivial deterministic state machine. Using it here (rather than
// the ledger) is what keeps raft/ testable independently of the banking domain,
// per context/DESIGN.md §7.
type countingSM struct {
	applied []string
}

func (c *countingSM) Apply(cmd []byte) any {
	c.applied = append(c.applied, string(cmd))
	return len(c.applied)
}

func newTestServer(sm StateMachine) *Server {
	return NewServer("n1", []NodeID{"n1", "n2", "n3"}, sm)
}

// entries builds a run of entries starting at index start, all in term term.
func entries(start Index, term Term, cmds ...string) []LogEntry {
	out := make([]LogEntry, len(cmds))
	for i, c := range cmds {
		out[i] = LogEntry{Term: term, Index: start + Index(i), Command: []byte(c)}
	}
	return out
}

// --- AppendEntries: normal flow -------------------------------------------

func TestAppendEntries_AppendsToEmptyLog(t *testing.T) {
	s := newTestServer(&countingSM{})

	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n2",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: entries(1, 1, "a", "b"),
	})

	if !reply.Success {
		t.Fatal("expected success appending to empty log at prevLogIndex 0")
	}
	if got := s.lastIndex(); got != 2 {
		t.Fatalf("lastIndex = %d, want 2", got)
	}
	if got := s.CurrentTerm(); got != 1 {
		t.Fatalf("currentTerm = %d, want 1 (should adopt leader's term)", got)
	}
}

func TestAppendEntries_HeartbeatKeepsLogIntact(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a")})

	// Heartbeat: same prevLog, no entries.
	reply := s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 1, PrevLogTerm: 1})

	if !reply.Success {
		t.Fatal("heartbeat should succeed when prevLog matches")
	}
	if got := s.lastIndex(); got != 1 {
		t.Fatalf("heartbeat changed the log: lastIndex = %d, want 1", got)
	}
}

// --- AppendEntries: failure flows -----------------------------------------

func TestAppendEntries_RejectsStaleTerm(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.AppendEntries(AppendEntriesArgs{Term: 5, PrevLogIndex: 0})

	// A deposed leader still broadcasting at an old term must be rejected.
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 3, PrevLogIndex: 0, Entries: entries(1, 3, "stale"),
	})

	if reply.Success {
		t.Fatal("rule 1 violated: accepted AppendEntries with term < currentTerm")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 so the stale leader steps down", reply.Term)
	}
	if got := s.lastIndex(); got != 0 {
		t.Fatalf("stale leader mutated the log: lastIndex = %d, want 0", got)
	}
}

func TestAppendEntries_RejectsGapInLog(t *testing.T) {
	s := newTestServer(&countingSM{})

	// prevLogIndex 5 does not exist — the follower is behind and must refuse
	// rather than create a hole.
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 5, PrevLogTerm: 1, Entries: entries(6, 1, "x"),
	})

	if reply.Success {
		t.Fatal("rule 2 violated: accepted entries past a gap")
	}
	if got := s.lastIndex(); got != 0 {
		t.Fatalf("log should be untouched, lastIndex = %d", got)
	}
}

func TestAppendEntries_RejectsPrevLogTermMismatch(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a")})

	// Right index, wrong term: this follower's history diverges from the leader's.
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 2, PrevLogIndex: 1, PrevLogTerm: 99, Entries: entries(2, 2, "b"),
	})

	if reply.Success {
		t.Fatal("rule 2 violated: accepted entries with mismatched prevLogTerm")
	}
}

// --- AppendEntries: conflict truncation (rule 3) --------------------------

func TestAppendEntries_TruncatesConflictingSuffix(t *testing.T) {
	s := newTestServer(&countingSM{})
	// Follower has three entries from a leader in term 1.
	s.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a", "b", "c"),
	})

	// A new leader in term 2 overwrites from index 2. Rule 3 requires deleting
	// the conflicting entry AND everything after it — index 3 must not survive.
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 2, "B"),
	})

	if !reply.Success {
		t.Fatal("expected success: prevLogIndex 1 matches")
	}
	if got := s.lastIndex(); got != 2 {
		t.Fatalf("rule 3 violated: lastIndex = %d, want 2 (index 3 must be deleted)", got)
	}
	e, _ := s.entryAt(2)
	if string(e.Command) != "B" || e.Term != 2 {
		t.Fatalf("entry 2 = %q term %d, want \"B\" term 2", e.Command, e.Term)
	}
}

func TestAppendEntries_KeepsMatchingPrefix(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a", "b")})

	// Same entries resent plus a new one: the matching prefix must NOT be
	// truncated, or committed entries could be lost.
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a", "b", "c"),
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if got := s.lastIndex(); got != 3 {
		t.Fatalf("lastIndex = %d, want 3", got)
	}
}

// --- AppendEntries: retry flow (rule 3's no-double-process requirement) ---

func TestAppendEntries_DuplicateDeliveryIsIdempotent(t *testing.T) {
	sm := &countingSM{}
	s := newTestServer(sm)

	args := AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0,
		Entries:      entries(1, 1, "a", "b"),
		LeaderCommit: 2,
	}

	// The network may duplicate an RPC. Delivering it three times must leave the
	// same log and must not apply commands more than once.
	s.AppendEntries(args)
	s.AppendEntries(args)
	s.AppendEntries(args)

	if got := s.lastIndex(); got != 2 {
		t.Fatalf("duplicate delivery corrupted the log: lastIndex = %d, want 2", got)
	}
	if len(sm.applied) != 2 {
		t.Fatalf("commands applied %d times, want 2 — duplicate RPC double-processed",
			len(sm.applied))
	}
}

func TestAppendEntries_ReorderedStaleRetryDoesNotTruncate(t *testing.T) {
	sm := &countingSM{}
	s := newTestServer(sm)
	s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a", "b", "c")})

	// An older AppendEntries arrives late (networks reorder). Its entries already
	// match, so rule 3 must not fire and truncate the newer tail.
	s.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "a")})

	if got := s.lastIndex(); got != 3 {
		t.Fatalf("late duplicate truncated the log: lastIndex = %d, want 3", got)
	}
}

// --- Commit and apply -----------------------------------------------------

func TestAppendEntries_AppliesCommittedInOrder(t *testing.T) {
	sm := &countingSM{}
	s := newTestServer(sm)

	s.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0,
		Entries:      entries(1, 1, "first", "second", "third"),
		LeaderCommit: 3,
	})

	want := []string{"first", "second", "third"}
	if len(sm.applied) != len(want) {
		t.Fatalf("applied %v, want %v", sm.applied, want)
	}
	for i := range want {
		if sm.applied[i] != want[i] {
			t.Fatalf("applied out of order: got %v, want %v", sm.applied, want)
		}
	}
	if s.LastApplied() != 3 {
		t.Fatalf("lastApplied = %d, want 3", s.LastApplied())
	}
}

func TestAppendEntries_CommitIndexNeverExceedsLocalLog(t *testing.T) {
	sm := &countingSM{}
	s := newTestServer(sm)

	// The leader's commitIndex is 99, but it has only sent us one entry. Rule 5's
	// min() must clamp: committing entries we do not hold would break State
	// Machine Safety.
	s.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0,
		Entries:      entries(1, 1, "only"),
		LeaderCommit: 99,
	})

	if got := s.CommitIndex(); got != 1 {
		t.Fatalf("commitIndex = %d, want 1 — must clamp to last new entry", got)
	}
	if len(sm.applied) != 1 {
		t.Fatalf("applied %d commands, want 1", len(sm.applied))
	}
}

// --- RequestVote: normal and failure flows --------------------------------

func TestRequestVote_GrantsToUpToDateCandidate(t *testing.T) {
	s := newTestServer(&countingSM{})

	reply := s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})

	if !reply.VoteGranted {
		t.Fatal("expected vote granted to up-to-date candidate in a new term")
	}
	if s.votedFor == nil || *s.votedFor != "n2" {
		t.Fatalf("votedFor = %v, want n2", s.votedFor)
	}
}

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.RequestVote(RequestVoteArgs{Term: 5, CandidateID: "n2"})

	reply := s.RequestVote(RequestVoteArgs{Term: 3, CandidateID: "n3"})

	if reply.VoteGranted {
		t.Fatal("rule 1 violated: granted vote for term < currentTerm")
	}
}

// TestRequestVote_OneVotePerTerm is the Election Safety property (Figure 3):
// at most one leader can be elected in a given term. It holds because a server
// grants at most one vote per term.
func TestRequestVote_OneVotePerTerm(t *testing.T) {
	s := newTestServer(&countingSM{})

	first := s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})
	second := s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n3"})

	if !first.VoteGranted {
		t.Fatal("first candidate should get the vote")
	}
	if second.VoteGranted {
		t.Fatal("Election Safety violated: granted two votes in the same term")
	}
}

func TestRequestVote_RepeatedRequestFromSameCandidateGranted(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})

	// A candidate retrying after a lost reply must still get its vote, or an
	// election can stall purely because of a dropped packet.
	reply := s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})

	if !reply.VoteGranted {
		t.Fatal("retry from the same candidate in the same term must be granted")
	}
}

// TestRequestVote_RejectsCandidateWithStaleLog covers §5.4.1 — the check that
// makes Leader Completeness hold.
func TestRequestVote_RejectsCandidateWithStaleLog(t *testing.T) {
	s := newTestServer(&countingSM{})
	// Voter has entries through index 3, term 2.
	s.AppendEntries(AppendEntriesArgs{Term: 2, PrevLogIndex: 0, Entries: entries(1, 2, "a", "b", "c")})

	cases := []struct {
		name         string
		lastLogIndex Index
		lastLogTerm  Term
		want         bool
		why          string
	}{
		{"lower last term", 5, 1, false, "later term wins regardless of length"},
		{"same term, shorter log", 2, 2, false, "same term, longer log wins"},
		{"same term, equal log", 3, 2, true, "equally up-to-date is sufficient"},
		{"same term, longer log", 4, 2, true, "longer log at same term is more up-to-date"},
		{"higher term, shorter log", 1, 3, true, "later term wins even if shorter"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh voter each time so votedFor does not carry over.
			v := newTestServer(&countingSM{})
			v.AppendEntries(AppendEntriesArgs{Term: 2, PrevLogIndex: 0, Entries: entries(1, 2, "a", "b", "c")})

			reply := v.RequestVote(RequestVoteArgs{
				Term: 9, CandidateID: "n2",
				LastLogIndex: tc.lastLogIndex, LastLogTerm: tc.lastLogTerm,
			})
			if reply.VoteGranted != tc.want {
				t.Fatalf("voteGranted = %v, want %v (%s)", reply.VoteGranted, tc.want, tc.why)
			}
		})
	}
}

// --- Term rules across roles ----------------------------------------------

func TestHigherTermAlwaysCausesStepDown(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		t.Run(role.String(), func(t *testing.T) {
			s := newTestServer(&countingSM{})
			s.role = role
			s.currentTerm = 2

			s.AppendEntries(AppendEntriesArgs{Term: 7, LeaderID: "n2", PrevLogIndex: 0})

			if s.Role() != Follower {
				t.Fatalf("role = %v, want Follower after seeing a higher term", s.Role())
			}
			if s.CurrentTerm() != 7 {
				t.Fatalf("currentTerm = %d, want 7", s.CurrentTerm())
			}
		})
	}
}

// A candidate that receives AppendEntries from a leader in the SAME term must
// convert to follower (Figure 2, "Candidates") — that leader already won.
func TestCandidateStepsDownOnSameTermLeader(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.role = Candidate
	s.currentTerm = 3

	s.AppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "n2", PrevLogIndex: 0})

	if s.Role() != Follower {
		t.Fatalf("role = %v, want Follower: a same-term leader has already won", s.Role())
	}
}

func TestNewTermClearsVote(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})

	// A new term must clear votedFor, otherwise no election could ever be won
	// after the first one.
	reply := s.RequestVote(RequestVoteArgs{Term: 2, CandidateID: "n3"})

	if !reply.VoteGranted {
		t.Fatal("vote should be granted in a new term after votedFor is cleared")
	}
}

// --- Log Matching (Figure 3) ----------------------------------------------

// If two logs contain an entry with the same index and term, the logs are
// identical in all entries up through that index. Here two followers receive
// different histories and are then brought into agreement by a new leader; the
// property must hold afterwards.
func TestLogMatching_AfterConflictResolution(t *testing.T) {
	a := newTestServer(&countingSM{})
	b := newTestServer(&countingSM{})

	// Divergent histories from different term-1/term-2 leaders.
	a.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "x", "y", "z")})
	b.AppendEntries(AppendEntriesArgs{Term: 2, PrevLogIndex: 0, Entries: entries(1, 2, "x")})

	// A term-3 leader replicates its own history to both.
	authoritative := entries(1, 3, "x", "y")
	for _, s := range []*Server{a, b} {
		reply := s.AppendEntries(AppendEntriesArgs{
			Term: 3, PrevLogIndex: 0, Entries: authoritative,
		})
		if !reply.Success {
			t.Fatalf("%s rejected the authoritative log", s.ID())
		}
	}

	assertLogMatching(t, a, b)
}

// assertLogMatching checks Figure 3's Log Matching property between two servers.
func assertLogMatching(t *testing.T, a, b *Server) {
	t.Helper()
	la, lb := a.LogEntries(), b.LogEntries()

	limit := min(len(la), len(lb))
	for i := range limit {
		if la[i].Index != lb[i].Index {
			t.Fatalf("index mismatch at slot %d: %d vs %d", i, la[i].Index, lb[i].Index)
		}
		if la[i].Term != lb[i].Term {
			continue // different terms at this index: property says nothing beyond here
		}
		// Same index and term ⇒ every preceding entry must be identical.
		for j := 0; j <= i; j++ {
			if la[j].Term != lb[j].Term || string(la[j].Command) != string(lb[j].Command) {
				t.Fatalf("Log Matching violated: entry %d/term %d agrees but entry %d differs (%q term %d vs %q term %d)",
					la[i].Index, la[i].Term, j,
					la[j].Command, la[j].Term, lb[j].Command, lb[j].Term)
			}
		}
	}
}

// --- State Machine Safety (Figure 3) --------------------------------------

// If a server has applied an entry at a given index, no other server will ever
// apply a different entry for that same index. Both followers here are fed the
// same committed prefix through different delivery patterns (one batch, one
// split with a duplicate) and must apply identical sequences.
func TestStateMachineSafety_SameIndexSameCommand(t *testing.T) {
	smA, smB := &countingSM{}, &countingSM{}
	a, b := newTestServer(smA), newTestServer(smB)

	// a: one batch, committed at once.
	a.AppendEntries(AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0,
		Entries: entries(1, 1, "op1", "op2", "op3"), LeaderCommit: 3,
	})

	// b: split delivery with a duplicated middle RPC and incremental commits.
	b.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 0, Entries: entries(1, 1, "op1"), LeaderCommit: 1})
	b.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 1, "op2"), LeaderCommit: 2})
	b.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 1, "op2"), LeaderCommit: 2}) // duplicate
	b.AppendEntries(AppendEntriesArgs{Term: 1, PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(3, 1, "op3"), LeaderCommit: 3})

	if len(smA.applied) != len(smB.applied) {
		t.Fatalf("applied different counts: %v vs %v", smA.applied, smB.applied)
	}
	for i := range smA.applied {
		if smA.applied[i] != smB.applied[i] {
			t.Fatalf("State Machine Safety violated at index %d: %q vs %q",
				i+1, smA.applied[i], smB.applied[i])
		}
	}
	assertLogMatching(t, a, b)
}

// --- Leader Append-Only (Figure 3) ----------------------------------------

// A leader never overwrites or deletes entries in its own log. A server acting as
// leader that receives a stale AppendEntries must not truncate anything.
func TestLeaderAppendOnly_StaleRPCDoesNotTruncate(t *testing.T) {
	s := newTestServer(&countingSM{})
	s.AppendEntries(AppendEntriesArgs{Term: 5, PrevLogIndex: 0, Entries: entries(1, 5, "a", "b", "c")})
	s.role = Leader

	before := s.LogEntries()
	s.AppendEntries(AppendEntriesArgs{
		Term: 2, PrevLogIndex: 0, Entries: entries(1, 2, "evil"),
	})
	after := s.LogEntries()

	if len(before) != len(after) {
		t.Fatalf("Leader Append-Only violated: log length %d -> %d", len(before), len(after))
	}
	for i := range before {
		if string(before[i].Command) != string(after[i].Command) {
			t.Fatalf("Leader Append-Only violated: entry %d changed %q -> %q",
				i, before[i].Command, after[i].Command)
		}
	}
}

// --- Concurrency ----------------------------------------------------------

// Rule 3 requires a concurrent flow. Raft servers receive RPCs from several peers
// at once; the mutex must keep state coherent. Run with -race.
func TestConcurrentRPCsDoNotCorruptState(t *testing.T) {
	s := newTestServer(&countingSM{})

	const goroutines = 8
	const perGoroutine = 50
	done := make(chan struct{})

	for g := range goroutines {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range perGoroutine {
				switch (g + i) % 3 {
				case 0:
					s.AppendEntries(AppendEntriesArgs{
						Term: Term(1 + i%3), LeaderID: "n2", PrevLogIndex: 0,
						Entries: entries(1, Term(1+i%3), fmt.Sprintf("g%d-%d", g, i)),
					})
				case 1:
					s.RequestVote(RequestVoteArgs{
						Term: Term(1 + i%3), CandidateID: NodeID(fmt.Sprintf("n%d", g)),
					})
				default:
					_ = s.Role()
					_ = s.CurrentTerm()
					_ = s.LogEntries()
				}
			}
		}(g)
	}
	for range goroutines {
		<-done
	}

	// Invariants that must hold no matter how the operations interleaved.
	if s.CommitIndex() > s.lastIndex() {
		t.Fatalf("commitIndex %d exceeds lastIndex %d", s.CommitIndex(), s.lastIndex())
	}
	if s.LastApplied() > s.CommitIndex() {
		t.Fatalf("lastApplied %d exceeds commitIndex %d", s.LastApplied(), s.CommitIndex())
	}
	log := s.LogEntries()
	for i, e := range log {
		if e.Index != Index(i) {
			t.Fatalf("log corrupted: slot %d holds index %d", i, e.Index)
		}
	}
}
