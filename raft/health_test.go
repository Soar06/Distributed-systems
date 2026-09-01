package raft

import (
	"testing"
	"time"
)

// Health and readiness tests (G5).
//
// The property under test is the one the whole feature exists for: a Raft node
// that has LOST QUORUM must report itself not ready, even though it still has a
// role, still has a term, still answers RPCs, and — if it was the leader — still
// believes it leads.
//
// That gap is the degraded-quorum blind spot (learn/READING_LIST.md §16). Every
// superficial signal stays green while nothing can commit, and for a bank that
// means writes which appear accepted and are not.
//
// Per RULES.md rule 3: normal (a healthy leader and follower are ready), failure
// (a partitioned leader, a follower that has lost its leader, a stopped node),
// and the boundary cases (single-node clusters, a candidate mid-election).

const testWindow = 200 * time.Millisecond

// A leader in contact with a majority is ready.
func TestLeaderWithQuorumIsReady(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 4
	s.leaderID = "n1"
	s.lastContact = map[NodeID]time.Time{
		"n2": time.Now(),
		"n3": time.Now(),
	}
	s.mu.Unlock()

	h := s.Health(testWindow)
	if !h.Ready {
		t.Fatalf("a leader in contact with both peers is not ready: %+v", h)
	}
	if h.QuorumContact != 3 {
		t.Fatalf("QuorumContact = %d, want 3 (self plus two peers)", h.QuorumContact)
	}
	if h.QuorumNeeded != 2 {
		t.Fatalf("QuorumNeeded = %d, want 2 for a 3-node cluster", h.QuorumNeeded)
	}
	if h.NotReadyReason != "" {
		t.Fatalf("a ready node carried a reason: %q", h.NotReadyReason)
	}
}

// THE case this feature exists for: a leader that has lost contact with its
// peers must report NOT ready, even though it still holds the role.
//
// Leadership is not the same as quorum. A partitioned leader keeps its role and
// its term until it hears otherwise, so "am I the leader?" answers yes while "can
// I commit?" answers no. Only the second question matters to a client.
func TestPartitionedLeaderIsNotReady(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 4
	s.leaderID = "n1"
	// Contact recorded, but long enough ago to be outside the window — a peer that
	// stopped answering.
	stale := time.Now().Add(-10 * testWindow)
	s.lastContact = map[NodeID]time.Time{"n2": stale, "n3": stale}
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a leader that has heard from NO peer reports ready: %+v — this is "+
			"exactly the degraded-quorum blind spot: it still holds the role, still "+
			"has a term, and cannot commit a thing", h)
	}
	if h.QuorumContact != 1 {
		t.Fatalf("QuorumContact = %d, want 1 (self only)", h.QuorumContact)
	}
	if h.NotReadyReason == "" {
		t.Fatal("not ready with no reason given; whoever is paged has to guess")
	}
	// The role must still report Leader: readiness is a separate question, and
	// flattening the two would lose the information an operator needs.
	if h.Role != Leader {
		t.Fatalf("role = %v, want Leader — readiness must not rewrite the role", h.Role)
	}
	t.Logf("partitioned leader correctly not ready: %s", h.NotReadyReason)
}

// A leader that can still see exactly a bare majority is ready: that is the
// definition, and being stricter would take a working cluster out of service.
func TestLeaderWithBareMajorityIsReady(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1", "n2", "n3", "n4", "n5"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 2
	s.leaderID = "n1"
	stale := time.Now().Add(-10 * testWindow)
	// Two live peers plus self is 3 of 5 — exactly a majority.
	s.lastContact = map[NodeID]time.Time{
		"n2": time.Now(), "n3": time.Now(),
		"n4": stale, "n5": stale,
	}
	s.mu.Unlock()

	h := s.Health(testWindow)
	if !h.Ready {
		t.Fatalf("a leader with a bare majority (3 of 5) is not ready: %+v — this "+
			"cluster can commit, and taking it out of service would be the outage", h)
	}
	if h.QuorumContact != 3 || h.QuorumNeeded != 3 {
		t.Fatalf("contact=%d needed=%d, want 3 and 3", h.QuorumContact, h.QuorumNeeded)
	}
}

// One below a majority is not ready.
func TestLeaderOneBelowMajorityIsNotReady(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1", "n2", "n3", "n4", "n5"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 2
	s.leaderID = "n1"
	stale := time.Now().Add(-10 * testWindow)
	// One live peer plus self is 2 of 5 — one short.
	s.lastContact = map[NodeID]time.Time{
		"n2": time.Now(), "n3": stale, "n4": stale, "n5": stale,
	}
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a leader with 2 of 5 reports ready: %+v — it cannot commit", h)
	}
	if h.QuorumContact != 2 {
		t.Fatalf("QuorumContact = %d, want 2", h.QuorumContact)
	}
}

// A single-node cluster is its own majority.
func TestSingleNodeLeaderIsReady(t *testing.T) {
	s := NewServer("solo", []NodeID{"solo"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 1
	s.leaderID = "solo"
	s.mu.Unlock()

	h := s.Health(testWindow)
	if !h.Ready {
		t.Fatalf("a single-node cluster is not ready: %+v — it is its own majority", h)
	}
	if h.QuorumNeeded != 1 {
		t.Fatalf("QuorumNeeded = %d, want 1", h.QuorumNeeded)
	}
}

// A follower in contact with a leader is ready to serve stale-tolerant reads.
func TestFollowerHearingFromLeaderIsReady(t *testing.T) {
	s := NewServer("n2", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Follower
	s.leaderID = "n1"
	s.currentTerm = 3
	s.resetElectionTimerLocked()
	s.mu.Unlock()

	h := s.Health(testWindow)
	if !h.Ready {
		t.Fatalf("a follower in contact with its leader is not ready: %+v", h)
	}
	if h.QuorumContact != 0 {
		t.Fatalf("QuorumContact = %d on a follower, want 0 — only a leader tracks "+
			"peer contact", h.QuorumContact)
	}
}

// A follower that has heard nothing is not ready: it is about to start an
// election, and answering from it means answering from a node that has lost touch.
func TestFollowerWithNoLeaderContactIsNotReady(t *testing.T) {
	s := NewServer("n2", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Follower
	s.leaderID = "n1"
	s.currentTerm = 3
	// Election deadline already passed.
	s.electionDeadline = time.Now().Add(-time.Second)
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a follower past its election deadline reports ready: %+v", h)
	}
	if h.NotReadyReason == "" {
		t.Fatal("not ready with no reason")
	}
}

// A follower that knows no leader at all is not ready.
func TestFollowerWithNoKnownLeaderIsNotReady(t *testing.T) {
	s := NewServer("n2", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a follower with no known leader reports ready: %+v", h)
	}
	if h.NotReadyReason != "no known leader" {
		t.Fatalf("reason = %q, want %q", h.NotReadyReason, "no known leader")
	}
}

// A candidate mid-election is not serving.
func TestCandidateIsNotReady(t *testing.T) {
	s := NewServer("n2", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Candidate
	s.currentTerm = 7
	s.resetElectionTimerLocked()
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a candidate reports ready: %+v", h)
	}
	if h.NotReadyReason != "election in progress" {
		t.Fatalf("reason = %q", h.NotReadyReason)
	}
}

// A stopped node is never ready, whatever role it held.
//
// The phantom-quorum bug taught this lesson at the RPC layer: a shut-down node
// that keeps answering is worse than one that is plainly gone.
func TestStoppedNodeIsNotReady(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 5
	s.leaderID = "n1"
	s.lastContact = map[NodeID]time.Time{"n2": time.Now(), "n3": time.Now()}
	s.stopped = true
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Ready {
		t.Fatalf("a STOPPED leader with full quorum contact reports ready: %+v", h)
	}
	if h.NotReadyReason != "server is stopped" {
		t.Fatalf("reason = %q", h.NotReadyReason)
	}
}

// Health must report the counters an operator needs, not just readiness.
func TestHealthReportsProgressCounters(t *testing.T) {
	s := NewServer("n1", []NodeID{"n1"}, &noopHealthSM{})

	s.mu.Lock()
	s.role = Leader
	s.currentTerm = 9
	s.leaderID = "n1"
	for i := range 5 {
		s.log = append(s.log, LogEntry{Term: 9, Index: Index(i + 1), Command: []byte("x")})
	}
	s.commitIndex = 5
	s.lastApplied = 3
	s.mu.Unlock()

	h := s.Health(testWindow)
	if h.Term != 9 {
		t.Fatalf("Term = %d, want 9", h.Term)
	}
	if h.CommitIndex != 5 || h.LastApplied != 3 {
		t.Fatalf("commit=%d applied=%d, want 5 and 3", h.CommitIndex, h.LastApplied)
	}
	if h.LogLength != 5 {
		t.Fatalf("LogLength = %d, want 5 (excluding the sentinel)", h.LogLength)
	}
	// The commit/applied gap is the signal that the state machine is falling
	// behind, which no other metric shows.
	if gap := int64(h.CommitIndex) - int64(h.LastApplied); gap != 2 {
		t.Fatalf("apply lag = %d, want 2", gap)
	}
}

// A leader elected in a new term must not inherit contact timestamps from the
// old one.
//
// Carrying them over would let a leader elected inside a minority partition
// report quorum from timestamps recorded BEFORE the partition — the readiness
// signal would be a memory of a majority that no longer exists.
func TestNewLeaderDoesNotInheritStaleContact(t *testing.T) {
	s := NewServerWith("n1", []NodeID{"n1", "n2", "n3"}, &noopHealthSM{}, nil, DefaultConfig(), 1)

	// Pretend a previous term left full contact recorded.
	s.mu.Lock()
	s.lastContact = map[NodeID]time.Time{"n2": time.Now(), "n3": time.Now()}
	s.role = Candidate
	s.currentTerm = 2
	s.votedFor = ptrNodeID("n1")
	s.mu.Unlock()

	// Win the election.
	s.becomeLeader(2)

	h := s.Health(testWindow)
	if h.QuorumContact != 1 {
		t.Fatalf("a newly elected leader reports contact with %d servers before "+
			"exchanging a single heartbeat: stale contact was inherited, so a leader "+
			"in a minority partition would report a majority that no longer exists",
			h.QuorumContact)
	}
	if h.Ready {
		t.Fatal("a newly elected leader reports ready before hearing from any peer")
	}
}

func ptrNodeID(id NodeID) *NodeID { return &id }

// noopHealthSM is a state machine that does nothing.
type noopHealthSM struct{}

func (noopHealthSM) Apply([]byte) any { return nil }
