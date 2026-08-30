package raft

import (
	"testing"
	"time"
)

// Cluster membership change tests (§6, dissertation §4.1) — G6.
//
// Per RULES.md rule 3: normal (add and remove a server), failure (removing the
// leader, removing the last server, a malformed entry), concurrent (two changes
// at once must be refused), and the safety rules the paper is specific about —
// a change takes effect on APPEND rather than on commit, and only one server may
// change at a time.
//
// The property underneath all of it: two disjoint majorities must never exist.
// Single-server changes guarantee that by a counting argument — any majority of
// the old configuration and any majority of the new overlap in at least one
// server, and one server never votes twice in a term.

func newMemberServer(t *testing.T, id NodeID, peers ...NodeID) *Server {
	t.Helper()
	s := NewServer(id, peers, &noopHealthSM{})
	s.SetStorage(&memStorage{})
	return s
}

// makeLeader puts a server into the leader role without running an election, so
// membership can be tested independently of election timing.
func makeLeader(t *testing.T, s *Server, term Term) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Leader
	s.currentTerm = term
	s.leaderID = s.id
	s.nextIndex = make(map[NodeID]Index, len(s.peers))
	s.matchIndex = make(map[NodeID]Index, len(s.peers))
	next := s.lastIndex() + 1
	for _, p := range s.peers {
		s.nextIndex[p] = next
		s.matchIndex[p] = 0
	}
	s.matchIndex[s.id] = s.lastIndex()
}

// --- encoding -------------------------------------------------------------

func TestConfigurationRoundTrips(t *testing.T) {
	in := Configuration{Servers: []NodeID{"n1", "n2", "n3"}}
	out, err := decodeConfig(encodeConfig(in))
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}
	if len(out.Servers) != 3 {
		t.Fatalf("decoded %d servers, want 3", len(out.Servers))
	}
	for i := range in.Servers {
		if out.Servers[i] != in.Servers[i] {
			t.Fatalf("server %d = %q, want %q", i, out.Servers[i], in.Servers[i])
		}
	}
}

// A configuration entry must be distinguishable from an application command, or
// the state machine would try to apply it as banking data.
func TestConfigEntriesAreDistinguishableFromCommands(t *testing.T) {
	cfg := encodeConfig(Configuration{Servers: []NodeID{"n1"}})
	if !isConfigEntry(cfg) {
		t.Fatal("a configuration entry was not recognised as one")
	}
	for _, cmd := range [][]byte{
		[]byte("deposit"), {}, {0x01, 0x02}, []byte("\x00transfer"),
	} {
		if isConfigEntry(cmd) {
			t.Fatalf("application command %q was mistaken for a configuration entry", cmd)
		}
	}
}

// A truncated or hostile entry must be refused rather than allocating from a
// declared length.
func TestMalformedConfigEntryIsRefused(t *testing.T) {
	good := encodeConfig(Configuration{Servers: []NodeID{"n1", "n2"}})

	for _, cut := range []int{1, 5, 9, len(good) - 1} {
		if _, err := decodeConfig(good[:cut]); err == nil {
			t.Fatalf("a configuration entry truncated to %d of %d bytes was accepted",
				cut, len(good))
		}
	}
	if _, err := decodeConfig([]byte("not a config")); err == nil {
		t.Fatal("a non-configuration command was decoded as one")
	}
}

// --- single-server rule ---------------------------------------------------

// The counting argument that makes this design safe only holds for a difference
// of one, so a bigger jump must be refused rather than attempted.
func TestConfigurationDiffersByOne(t *testing.T) {
	base := Configuration{Servers: []NodeID{"n1", "n2", "n3"}}

	cases := []struct {
		name string
		next Configuration
		want bool
	}{
		{"add one", Configuration{Servers: []NodeID{"n1", "n2", "n3", "n4"}}, true},
		{"remove one", Configuration{Servers: []NodeID{"n1", "n2"}}, true},
		{"add two", Configuration{Servers: []NodeID{"n1", "n2", "n3", "n4", "n5"}}, false},
		{"remove two", Configuration{Servers: []NodeID{"n1"}}, false},
		{"same size", Configuration{Servers: []NodeID{"n1", "n2", "n4"}}, false},
		{"swap one for one, size+1", Configuration{Servers: []NodeID{"n1", "n2", "n4", "n5"}}, false},
	}
	for _, tc := range cases {
		if got := base.differsByOne(tc.next); got != tc.want {
			t.Fatalf("%s: differsByOne = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// --- adding a server ------------------------------------------------------

func TestAddServerAppendsConfigEntryAndTakesEffectImmediately(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	before := len(s.LogEntries())
	idx, err := s.AddServer("n4")
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if idx == 0 {
		t.Fatal("AddServer returned index 0")
	}
	if got := len(s.LogEntries()); got != before+1 {
		t.Fatalf("log grew by %d, want 1", got-before)
	}

	// §6: the change takes effect on APPEND, not on commit. The entry is not
	// committed here — nothing has replicated — and the configuration must have
	// changed anyway.
	cfg := s.CurrentConfiguration()
	if !cfg.Contains("n4") {
		t.Fatalf("configuration is %v after AddServer; §6 requires a server to use "+
			"the latest configuration in its log REGARDLESS of commit status. Waiting "+
			"for commit means voting under the old configuration while the leader "+
			"counts under the new one", cfg.Servers)
	}
	if s.CommitIndex() >= idx {
		t.Fatal("the test did not actually leave the entry uncommitted")
	}

	// Quorum must have grown with the cluster: 3 of 4, not 2 of 3.
	if got := cfg.Majority(); got != 3 {
		t.Fatalf("majority = %d for a 4-server configuration, want 3", got)
	}
}

// Adding a server must give it leader bookkeeping, or it is never replicated to.
func TestAddedServerGetsReplicationState(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	if _, err := s.AddServer("n4"); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	s.mu.Lock()
	_, hasNext := s.nextIndex["n4"]
	_, hasMatch := s.matchIndex["n4"]
	s.mu.Unlock()

	if !hasNext || !hasMatch {
		t.Fatal("the added server has no nextIndex/matchIndex, so the leader will " +
			"never replicate to it and it can never catch up")
	}
}

func TestAddExistingServerIsRejected(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	if _, err := s.AddServer("n2"); err == nil {
		t.Fatal("adding a server already in the configuration was accepted")
	}
}

// --- removing a server ----------------------------------------------------

func TestRemoveServerDropsItFromTheConfiguration(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	if _, err := s.RemoveServer("n3"); err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	cfg := s.CurrentConfiguration()
	if cfg.Contains("n3") {
		t.Fatalf("configuration still holds n3: %v", cfg.Servers)
	}
	if got := cfg.Majority(); got != 2 {
		t.Fatalf("majority = %d for a 2-server configuration, want 2", got)
	}

	// The removed server's bookkeeping must be gone, or it keeps counting toward
	// quorum despite no longer being a member.
	s.mu.Lock()
	_, stillTracked := s.matchIndex["n3"]
	s.mu.Unlock()
	if stillTracked {
		t.Fatal("a removed server is still tracked in matchIndex, so it would still " +
			"count toward the commit majority")
	}
}

// A cluster of zero can never elect a leader, so the removal could never commit.
func TestRemovingTheLastServerIsRefused(t *testing.T) {
	s := newMemberServer(t, "solo", "solo")
	makeLeader(t, s, 1)

	if _, err := s.RemoveServer("solo"); err != ErrLastServer {
		t.Fatalf("removing the last server returned %v, want ErrLastServer", err)
	}
}

func TestRemoveAbsentServerIsRejected(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	if _, err := s.RemoveServer("n9"); err == nil {
		t.Fatal("removing a server that is not in the configuration was accepted")
	}
}

// --- removing the leader --------------------------------------------------

// A leader removing itself must keep serving until the change COMMITS.
//
// This is the one place membership does not act on append. Stepping down early
// would strand the very entry that removes it, leaving the cluster with a
// configuration nobody can complete.
func TestLeaderRemovingItselfKeepsServingUntilCommit(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	idx, err := s.RemoveServer("n1")
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}

	// Still the leader: the entry has not committed.
	if got := s.Role(); got != Leader {
		t.Fatalf("role = %v immediately after proposing self-removal, want Leader — "+
			"stepping down now strands the entry that removes it", got)
	}
	if s.SteppedDownAfterRemoval() {
		t.Fatal("reported stepped-down before the removal committed")
	}

	// Commit it.
	s.mu.Lock()
	s.commitIndex = idx
	s.checkSelfRemovalLocked()
	s.mu.Unlock()

	if got := s.Role(); got != Follower {
		t.Fatalf("role = %v after the self-removal committed, want Follower", got)
	}
	if !s.SteppedDownAfterRemoval() {
		t.Fatal("a leader removed by a committed configuration did not record it")
	}
}

// --- one change at a time -------------------------------------------------

// Two concurrent changes can compose into a difference of TWO, which breaks the
// overlap argument even though each change alone is safe.
func TestSecondConfigChangeIsRefusedWhileOneIsInFlight(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	if _, err := s.AddServer("n4"); err != nil {
		t.Fatalf("first AddServer: %v", err)
	}
	if _, err := s.AddServer("n5"); err != ErrConfigChangeInFlight {
		t.Fatalf("a second change while one was uncommitted returned %v, want "+
			"ErrConfigChangeInFlight — two changes can compose into a difference of "+
			"two, and the safety argument only covers one", err)
	}
}

// Once the first change commits, another is allowed.
func TestConfigChangeAllowedAfterPreviousCommits(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	idx, err := s.AddServer("n4")
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	s.mu.Lock()
	s.commitIndex = idx
	s.mu.Unlock()

	if _, err := s.AddServer("n5"); err != nil {
		t.Fatalf("a change after the previous one committed was refused: %v", err)
	}
}

// --- leader-only ----------------------------------------------------------

func TestNonLeaderCannotChangeConfiguration(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")

	if _, err := s.AddServer("n4"); err != ErrNotLeader {
		t.Fatalf("a follower's AddServer returned %v, want ErrNotLeader", err)
	}
	if _, err := s.RemoveServer("n2"); err != ErrNotLeader {
		t.Fatalf("a follower's RemoveServer returned %v, want ErrNotLeader", err)
	}
}

// A server already removed must not keep reconfiguring the cluster it left.
func TestRemovedLeaderCannotChangeConfiguration(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	// Force a configuration this server is not part of.
	s.mu.Lock()
	s.useConfigurationLocked(Configuration{Servers: []NodeID{"n2", "n3"}})
	s.role = Leader
	s.mu.Unlock()

	if _, err := s.AddServer("n4"); err != ErrNotInConfiguration {
		t.Fatalf("a leader outside the configuration returned %v, want "+
			"ErrNotInConfiguration", err)
	}
}

// --- followers adopt from the log ----------------------------------------

// A follower learns membership the same way it learns everything else, and must
// adopt it on APPEND.
func TestFollowerAdoptsConfigurationFromAppendedEntries(t *testing.T) {
	s := newMemberServer(t, "n2", "n1", "n2", "n3")

	cfg := Configuration{Servers: []NodeID{"n1", "n2", "n3", "n4"}}
	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n1", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Command: encodeConfig(cfg)}},
		// Deliberately NOT committed: the follower must adopt it anyway.
		LeaderCommit: 0,
	})
	if !reply.Success {
		t.Fatalf("AppendEntries rejected: %+v", reply)
	}

	got := s.CurrentConfiguration()
	if !got.Contains("n4") {
		t.Fatalf("follower configuration is %v after appending a config entry; §6 "+
			"requires adopting the latest configuration in the log regardless of "+
			"commit status", got.Servers)
	}
}

// The NEWEST configuration in the log wins, not the first one found.
func TestFollowerAdoptsTheNewestConfiguration(t *testing.T) {
	s := newMemberServer(t, "n2", "n1", "n2", "n3")

	first := encodeConfig(Configuration{Servers: []NodeID{"n1", "n2", "n3", "n4"}})
	second := encodeConfig(Configuration{Servers: []NodeID{"n1", "n2", "n3"}})

	reply := s.AppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n1", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: first},
			{Term: 1, Index: 2, Command: second},
		},
	})
	if !reply.Success {
		t.Fatalf("AppendEntries rejected: %+v", reply)
	}

	if got := s.CurrentConfiguration(); got.Contains("n4") {
		t.Fatalf("follower adopted the OLDER configuration %v; the newest entry in "+
			"the log must win", got.Servers)
	}
}

// A restarting server must re-learn membership from its log, not from the flags
// it was started with.
func TestRestoreAdoptsConfigurationFromLog(t *testing.T) {
	store := &memStorage{}

	s := NewServer("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})
	s.SetStorage(store)
	makeLeader(t, s, 1)
	if _, err := s.AddServer("n4"); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	// Restart with the ORIGINAL peer list, as a process restarted from its flags
	// would.
	restarted := NewServer("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})
	restarted.SetStorage(store)
	if err := restarted.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	cfg := restarted.CurrentConfiguration()
	if !cfg.Contains("n4") {
		t.Fatalf("a restarted server came back with %v, the configuration in its "+
			"FLAGS, ignoring the change in its own log. It would count quorum against "+
			"a membership that no longer exists", cfg.Servers)
	}
}

// --- the disruptive server ------------------------------------------------

// A removed server must not be able to depose a healthy leader.
//
// Removed from the configuration, it stops receiving heartbeats, times out,
// increments its term, and campaigns. Its higher term would force the real leader
// to step down even though the caller is not a member any more — indefinitely.
//
// The answer (dissertation §4.2.3) is to ignore RequestVote received within the
// minimum election timeout of hearing from a current leader.
func TestVoteIsRefusedWhileALeaderIsReachingUs(t *testing.T) {
	s := newMemberServer(t, "n2", "n1", "n2", "n3")

	// A leader is currently reaching us.
	s.AppendEntries(AppendEntriesArgs{Term: 5, LeaderID: "n1"})
	if got := s.CurrentTerm(); got != 5 {
		t.Fatalf("term = %d after AppendEntries, want 5", got)
	}

	// A departed server campaigns at a much higher term.
	reply := s.RequestVote(RequestVoteArgs{
		Term: 99, CandidateID: "removed-node", LastLogIndex: 0, LastLogTerm: 0,
	})

	if reply.VoteGranted {
		t.Fatal("granted a vote to a disruptive candidate while a leader was reaching us")
	}
	if got := s.CurrentTerm(); got != 5 {
		t.Fatalf("currentTerm = %d after refusing the vote, want 5 — adopting the "+
			"term IS the damage: the real leader steps down on the next exchange even "+
			"though the vote was refused", got)
	}
}

// The check must not stop a legitimate election once the leader really is gone.
func TestVoteIsGrantedOnceTheLeaderStopsReachingUs(t *testing.T) {
	s := newMemberServer(t, "n2", "n1", "n2", "n3")

	s.AppendEntries(AppendEntriesArgs{Term: 5, LeaderID: "n1"})

	// Age the contact past the minimum election timeout.
	s.mu.Lock()
	s.lastLeaderContact = time.Now().Add(-10 * s.minElectionTimeout())
	s.mu.Unlock()

	reply := s.RequestVote(RequestVoteArgs{
		Term: 6, CandidateID: "n3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !reply.VoteGranted {
		t.Fatal("refused a legitimate candidate after the leader stopped reaching " +
			"us; the check must not prevent elections, only spurious ones")
	}
}

// A candidate must still be able to win when no leader is known at all.
func TestVoteIsGrantedWhenNoLeaderIsKnown(t *testing.T) {
	s := newMemberServer(t, "n2", "n1", "n2", "n3")

	reply := s.RequestVote(RequestVoteArgs{
		Term: 1, CandidateID: "n1", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !reply.VoteGranted {
		t.Fatal("refused the first candidate of a fresh cluster, which has no leader")
	}
}
