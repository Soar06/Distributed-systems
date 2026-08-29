package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Server is one Raft node: one Go process's worth of consensus state.
//
// Concurrency: a single mutex guards all mutable state. This is deliberate for
// Phase 1 — readability over performance, since races in consensus code are the
// hardest class of bug to find (context/DESIGN.md §2). Every exported method
// takes the lock; unexported helpers assume it is already held.
type Server struct {
	mu sync.Mutex

	id    NodeID
	peers []NodeID // the full cluster, INCLUDING this server

	// --- Persistent state on all servers (Figure 2) ---
	// Must be durable before responding to RPCs.
	currentTerm Term
	votedFor    *NodeID
	log         []LogEntry

	// --- Volatile state on all servers (Figure 2) ---
	commitIndex Index
	lastApplied Index

	// --- Volatile state on leaders (Figure 2), reinitialized after election ---
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	role Role

	// sm is the replicated application. Committed entries are applied to it in
	// log order, exactly once, on every server.
	sm StateMachine

	// --- role loop machinery (loop.go) ---

	cfg       Config
	transport Transport
	rnd       *rand.Rand

	// electionDeadline is when this server will start an election if it has not
	// heard from a leader. heartbeatDeadline is when a leader next sends
	// heartbeats.
	electionDeadline  time.Time
	heartbeatDeadline time.Time

	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewServer creates a follower with an empty log, using default timings and no
// transport. Suitable for testing the RPC receiver rules in isolation; use
// NewServerWith to run the role loop.
//
// peers must include id itself: majority calculations are defined over the full
// cluster (§5.2). All servers start as followers with term 0 (Figure 2, "State").
func NewServer(id NodeID, peers []NodeID, sm StateMachine) *Server {
	return NewServerWith(id, peers, sm, nil, DefaultConfig(), 1)
}

// NewServerWith creates a server with an explicit transport, timing config, and
// random seed.
//
// The seed is explicit so that a chaos run is reproducible: an unreproducible
// consensus bug is close to impossible to fix (learn/READING_LIST.md §5).
func NewServerWith(id NodeID, peers []NodeID, sm StateMachine, tr Transport, cfg Config, seed int64) *Server {
	return &Server{
		id:    id,
		peers: peers,
		// log[0] is the sentinel so real entries start at index 1, matching the
		// paper's 1-based log.
		log:        []LogEntry{{Term: 0, Index: 0}},
		role:       Follower,
		nextIndex:  make(map[NodeID]Index),
		matchIndex: make(map[NodeID]Index),
		sm:         sm,
		cfg:        cfg,
		transport:  tr,
		rnd:        newRand(seed),
	}
}

// ID returns this server's identifier.
func (s *Server) ID() NodeID { return s.id }

// Role returns the server's current role.
func (s *Server) Role() Role {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role
}

// CurrentTerm returns the server's current term.
func (s *Server) CurrentTerm() Term {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

// CommitIndex returns the highest log index known to be committed.
func (s *Server) CommitIndex() Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitIndex
}

// LastApplied returns the highest log index applied to the state machine.
func (s *Server) LastApplied() Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// LogEntries returns a copy of the log, sentinel included. For tests and for the
// cluster dashboard (NOW.md Phase 4); a copy so callers cannot mutate consensus
// state.
func (s *Server) LogEntries() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.log))
	copy(out, s.log)
	return out
}

// becomeFollower converts to follower at the given term. Caller must hold s.mu.
//
// This implements the "All Servers" rule from Figure 2: if an RPC request or
// response contains term T > currentTerm, set currentTerm = T and convert to
// follower. That rule is unconditional and applies in every role — it is the most
// commonly missed line in Figure 2, so it lives in one place here.
func (s *Server) becomeFollower(term Term) {
	s.role = Follower
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = nil // a new term means the vote has not been cast yet
	}
}

// observeTerm applies the "All Servers" term rule for any incoming term.
// Reports whether the server stepped down. Caller must hold s.mu.
func (s *Server) observeTerm(t Term) bool {
	if t > s.currentTerm {
		s.becomeFollower(t)
		return true
	}
	return false
}

// AppendEntries handles the AppendEntries RPC (Figure 2, §5.3).
//
// Receiver implementation, in the paper's order:
//  1. Reply false if term < currentTerm.
//  2. Reply false if the log has no entry at prevLogIndex whose term matches
//     prevLogTerm.
//  3. If an existing entry conflicts with a new one, delete it and all that follow.
//  4. Append any new entries not already in the log.
//  5. If leaderCommit > commitIndex, set commitIndex =
//     min(leaderCommit, index of last new entry).
func (s *Server) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rule 1: reject a stale leader without touching our log.
	if args.Term < s.currentTerm {
		return AppendEntriesReply{Term: s.currentTerm, Success: false}
	}

	// All Servers rule: a term at least as large means this leader is current, so
	// step down. Note >= rather than >: a candidate that receives AppendEntries
	// from a leader in the SAME term must also convert to follower (Figure 2,
	// "Candidates"), since that leader already won this term's election.
	s.observeTerm(args.Term)
	if s.role == Candidate && args.Term == s.currentTerm {
		s.role = Follower
	}

	// A valid leader is alive, so do not challenge it: reset the election timer
	// (Figure 2, "Followers"). This happens even if the log check below fails —
	// a log inconsistency means we are behind, not that the leader is invalid,
	// and starting an election here would disrupt a healthy cluster.
	s.resetElectionTimerLocked()

	// Rule 2: log consistency check.
	if !s.matchesPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		return AppendEntriesReply{Term: s.currentTerm, Success: false}
	}

	// Rules 3 and 4.
	s.appendFrom(args.PrevLogIndex, args.Entries)

	// Rule 5. The min() matters: the leader's commitIndex may be ahead of what it
	// has actually sent us, and committing entries we do not hold would break
	// State Machine Safety.
	if args.LeaderCommit > s.commitIndex {
		lastNew := args.PrevLogIndex + Index(len(args.Entries))
		s.commitIndex = min(args.LeaderCommit, lastNew)
		s.applyCommitted()
	}

	return AppendEntriesReply{Term: s.currentTerm, Success: true}
}

// RequestVote handles the RequestVote RPC (Figure 2, §5.2).
//
// Receiver implementation:
//  1. Reply false if term < currentTerm.
//  2. If votedFor is null or candidateId, and the candidate's log is at least as
//     up-to-date as the receiver's log, grant vote (§5.2, §5.4).
func (s *Server) RequestVote(args RequestVoteArgs) RequestVoteReply {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rule 1.
	if args.Term < s.currentTerm {
		return RequestVoteReply{Term: s.currentTerm, VoteGranted: false}
	}

	// All Servers rule: a higher term makes us a follower and clears votedFor,
	// which is what allows us to vote in this newer term.
	s.observeTerm(args.Term)

	// Rule 2. Both halves are required: the votedFor check gives at most one vote
	// per term (Election Safety), and the up-to-date check stops a candidate that
	// is missing committed entries from winning (Leader Completeness).
	alreadyVoted := s.votedFor != nil && *s.votedFor != args.CandidateID
	if alreadyVoted || !s.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return RequestVoteReply{Term: s.currentTerm, VoteGranted: false}
	}

	cand := args.CandidateID
	s.votedFor = &cand

	// "...without receiving AppendEntries RPC from current leader or granting
	// vote to candidate" — granting a vote also defers our own election, giving
	// the candidate we just endorsed time to win.
	s.resetElectionTimerLocked()

	return RequestVoteReply{Term: s.currentTerm, VoteGranted: true}
}

// applyCommitted applies newly committed entries to the state machine in log
// order. Caller must hold s.mu.
//
// Figure 2, "All Servers": if commitIndex > lastApplied, increment lastApplied
// and apply log[lastApplied]. Advancing one index at a time, in order, is what
// gives State Machine Safety: no server ever applies a different entry at a given
// index than another server did.
func (s *Server) applyCommitted() {
	for s.commitIndex > s.lastApplied {
		s.lastApplied++
		e, ok := s.entryAt(s.lastApplied)
		if !ok {
			// Cannot happen: commitIndex never exceeds the last log index.
			s.lastApplied--
			return
		}
		if s.sm != nil && e.Command != nil {
			s.sm.Apply(e.Command)
		}
	}
}
