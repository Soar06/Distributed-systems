// Package raft implements the Raft consensus algorithm from Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm (Extended Version)",
// tech report published 2014-05-20, https://raft.github.io/raft.pdf
//
// Section and figure references throughout cite that document. The paper wins on
// any disagreement between it and this code.
//
// This package knows nothing about banking. The state machine it replicates is an
// interface (see StateMachine) so that Raft can be tested against a trivial state
// machine, independently of the ledger domain — see context/DESIGN.md §7.
package raft

// Role is a server's current state in the Raft protocol (Figure 4).
//
// Followers only respond to requests from other servers. A follower that receives
// no communication becomes a candidate and starts an election. A candidate that
// receives votes from a majority of the full cluster becomes leader. Leaders
// typically operate until they fail.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// NodeID identifies one server in the cluster. In Phase 1 this is a host:port
// string from the static peer list; nothing in this package depends on that
// format, so real hostnames work unchanged later (LATER.md).
type NodeID string

// Term is a Raft term number. Terms act as a logical clock: they increase
// monotonically and let servers detect stale leaders and stale RPCs (§5.1).
type Term uint64

// Index is a position in the replicated log. The paper's log is 1-based: the
// first real entry has Index 1. Index 0 means "no entry" — it is the initial
// value of commitIndex and lastApplied, and the index of the sentinel entry.
type Index uint64

// LogEntry is one entry in the replicated log.
//
// Per §2, the log is a series of commands the state machine executes in order —
// the authoritative record from which state is derived. It is not diagnostic
// logging. Each entry holds the command and the term when the entry was received
// by the leader (Figure 2, "State").
type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte // opaque to raft; interpreted only by the StateMachine
}

// StateMachine is the deterministic application Raft replicates.
//
// Apply is called exactly once per committed entry, in log order, on every
// server. It MUST be deterministic: the same sequence of commands must produce
// the same state and the same results on every node. No wall-clock time, no
// randomness, no map-iteration order (context/DESIGN.md §6).
type StateMachine interface {
	Apply(cmd []byte) any
}

// IndexedStateMachine is an optional extension: a state machine that also wants
// to know the log index of the entry being applied.
//
// This lets a caller that proposed entry N find out what entry N actually did —
// "the entry replicated" and "the operation succeeded" are different questions,
// and a 2PC prepare that votes NO replicates perfectly well.
type IndexedStateMachine interface {
	StateMachine
	ApplyAt(index Index, cmd []byte) any
}

// Persistent state on all servers (Figure 2).
//
// The paper requires this be updated on stable storage BEFORE responding to
// RPCs. Violating that is a silent correctness bug that only appears after a
// crash, so it is modelled as its own type to keep the durability boundary
// explicit rather than scattered across fields.
type PersistentState struct {
	// CurrentTerm is the latest term this server has seen. Initialized to 0 on
	// first boot, increases monotonically.
	CurrentTerm Term

	// VotedFor is the candidate that received this server's vote in the current
	// term, or nil if none. Reset to nil whenever CurrentTerm advances.
	VotedFor *NodeID

	// Log holds the entries. Log[0] is a zero-value sentinel so that Go's
	// 0-based slice indexing lines up with the paper's 1-based log; real entries
	// start at Log[1]. This removes an off-by-one from every prevLogIndex
	// comparison (context/DESIGN.md §2).
	Log []LogEntry
}

// AppendEntriesArgs are the arguments to the AppendEntries RPC (Figure 2).
//
// Invoked by the leader to replicate log entries (§5.3); also used as a
// heartbeat (§5.2), in which case Entries is empty.
type AppendEntriesArgs struct {
	Term         Term   // leader's term
	LeaderID     NodeID // so follower can redirect clients
	PrevLogIndex Index  // index of log entry immediately preceding new ones
	PrevLogTerm  Term   // term of PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit Index // leader's commitIndex
}

// AppendEntriesReply is the result of an AppendEntries RPC (Figure 2).
type AppendEntriesReply struct {
	Term Term // currentTerm, for leader to update itself

	// Success is true if the follower contained an entry matching PrevLogIndex
	// and PrevLogTerm.
	Success bool
}

// RequestVoteArgs are the arguments to the RequestVote RPC (Figure 2).
//
// Invoked by candidates to gather votes (§5.2).
type RequestVoteArgs struct {
	Term         Term   // candidate's term
	CandidateID  NodeID // candidate requesting vote
	LastLogIndex Index  // index of candidate's last log entry (§5.4)
	LastLogTerm  Term   // term of candidate's last log entry (§5.4)
}

// RequestVoteReply is the result of a RequestVote RPC (Figure 2).
type RequestVoteReply struct {
	Term        Term // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}
