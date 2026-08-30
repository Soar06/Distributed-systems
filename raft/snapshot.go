package raft

import (
	"errors"
	"fmt"
	"time"
)

var (
	// errNoSnapshotToSend means nextIndex fell below the snapshot boundary while no
	// snapshot exists. Structurally impossible; counted rather than ignored.
	errNoSnapshotToSend = errors.New("raft: follower needs a snapshot but none exists")

	// errTransportCannotSnapshot means the transport does not implement
	// SnapshotTransport, so a lagging follower cannot be caught up at all.
	errTransportCannotSnapshot = errors.New(
		"raft: transport does not support InstallSnapshot; a follower behind the " +
			"compacted prefix cannot be caught up")
)

// Snapshotting and log compaction (§7).
//
// The problem, stated by the paper: "the log grows longer, it occupies more space
// and takes more time to replay." Measured here before this existed:
// storage.RaftState.Save rewrites the ENTIRE state on every persist, which is
// O(n²) over a run — 481x write amplification at 800 entries. That is a wall
// rather than a slope, and it also makes restart time unbounded, which is why
// membership changes (G6) depend on this landing first.
//
// §7's mechanism: each server snapshots its own state machine independently,
// records the index and term of the last entry the snapshot includes, and
// discards the log through that point. A follower that has fallen behind the
// leader's discarded prefix is caught up with InstallSnapshot rather than
// AppendEntries.
//
// THE DANGEROUS PART, and the first thing tested: a snapshot must capture
// everything the state machine holds, not merely the balances. shard.Machine also
// holds 2PC transaction records and the ledger holds fund reservations. A
// compacted node that lost those would forget an unretractable promise — exactly
// the durability property the 2PC work established — and would free money it had
// already committed to another transaction.

// Snapshotter is an optional StateMachine extension: a state machine that can
// serialize and restore its whole state.
//
// A state machine that does not implement it simply never gets compacted, which
// is correct but unbounded. Both ledger.Machine and shard.Machine implement it.
//
// The contract that makes this safe is the same one Apply already lives under:
// Snapshot must capture ALL state derived from the log, and Restore must produce
// a state machine indistinguishable from one that replayed the whole log. If
// those two disagree, compaction silently changes the state machine — a class of
// bug that no amount of Raft correctness can catch, because the log is still
// perfect.
type Snapshotter interface {
	// Snapshot returns a serialized copy of the entire state machine.
	Snapshot() ([]byte, error)

	// RestoreSnapshot replaces the state machine's contents with a snapshot.
	RestoreSnapshot(data []byte) error
}

// SnapshotStorage is an optional Storage extension for persisting snapshots.
//
// Kept separate from Storage so that a storage backend without snapshot support
// still satisfies the base interface, and so the raft -> storage dependency stays
// one-directional.
type SnapshotStorage interface {
	// SaveSnapshot durably records a snapshot and the log position it covers.
	// It must not return until the data would survive a power loss.
	SaveSnapshot(lastIncludedIndex uint64, lastIncludedTerm uint64, data []byte) error

	// LoadSnapshot returns the most recent snapshot, or ok=false if none exists.
	LoadSnapshot() (lastIncludedIndex uint64, lastIncludedTerm uint64, data []byte, ok bool, err error)
}

// InstallSnapshotArgs is the §7 RPC, sent when a follower needs entries the
// leader has already discarded.
//
// [project decision] The snapshot is sent whole rather than in the paper's
// offset-chunked form. Figure 13 chunks because a snapshot may be very large; at
// this project's scale a snapshot fits comfortably in one message, and chunking
// adds partial-transfer state that would need its own correctness argument. The
// field names still mirror Figure 13 so the code reads against the paper, and
// Done is retained as an explicit marker that this is the whole thing.
type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Data              []byte
	Done              bool
}

// InstallSnapshotReply carries the follower's term, so a stale leader steps down.
type InstallSnapshotReply struct {
	Term Term
}

// snapshotState is what a server knows about its own discarded log prefix.
//
// lastIncludedIndex/Term are load-bearing beyond bookkeeping: they are what let
// AppendEntries still answer a prevLogIndex/prevLogTerm check for the entry
// immediately before the snapshot. Without them, compaction would make the log
// unmatched at its own boundary and replication would stall permanently.
type snapshotState struct {
	lastIncludedIndex Index
	lastIncludedTerm  Term
	data              []byte
}

// MaybeCompact takes a snapshot and discards the log through lastApplied, if the
// log has grown past threshold entries.
//
// Called after applying, so compaction happens where new entries arrive rather
// than on a timer: the cost this exists to bound is a function of log SIZE, not
// of elapsed time.
//
// Returns whether a snapshot was taken.
func (s *Server) MaybeCompact(threshold int) (bool, error) {
	s.mu.Lock()

	snapper, ok := s.sm.(Snapshotter)
	store, hasStore := s.storage.(SnapshotStorage)
	if !ok || !hasStore || s.sm == nil {
		// No snapshot support: correct, but the log grows without bound. Reported
		// as "did not compact" rather than as an error, since it is a configuration
		// choice and not a failure.
		s.mu.Unlock()
		return false, nil
	}

	// Compact only up to what has been APPLIED, never merely committed. A snapshot
	// is a picture of the state machine, so it can only cover entries the state
	// machine has actually seen. Discarding an entry that is committed but not yet
	// applied would lose it entirely: it is gone from the log and absent from the
	// snapshot.
	upto := s.lastApplied
	if upto <= s.baseIndex() || len(s.log)-1 < threshold {
		s.mu.Unlock()
		return false, nil
	}

	e, found := s.entryAt(upto)
	if !found {
		s.mu.Unlock()
		return false, fmt.Errorf("raft: cannot compact to %d: entry missing", upto)
	}
	lastIncludedIndex, lastIncludedTerm := e.Index, e.Term

	// Serialize while holding the lock, so the snapshot is a consistent picture of
	// the state machine at exactly lastApplied. Releasing first would let another
	// entry apply and produce a snapshot that matches no log position at all.
	data, err := snapper.Snapshot()
	if err != nil {
		s.mu.Unlock()
		return false, fmt.Errorf("raft: snapshot state machine: %w", err)
	}
	s.mu.Unlock()

	// Persist BEFORE truncating. If the process dies between the two, the log is
	// still whole and replay reconstructs the same state — the snapshot is merely
	// redundant. Truncating first and crashing before the write would destroy
	// entries that exist nowhere else.
	if err := store.SaveSnapshot(uint64(lastIncludedIndex), uint64(lastIncludedTerm), data); err != nil {
		return false, fmt.Errorf("raft: save snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check under the reacquired lock: the log may have been truncated by an
	// AppendEntries from a new leader while the snapshot was being written.
	if lastIncludedIndex <= s.baseIndex() || lastIncludedIndex > s.lastIndex() {
		return false, nil
	}
	if cur, ok := s.entryAt(lastIncludedIndex); !ok || cur.Term != lastIncludedTerm {
		// The entry at that index is no longer the one we snapshotted, so the
		// snapshot describes a log that no longer exists here. Leave the log alone.
		return false, nil
	}

	s.discardThrough(lastIncludedIndex, lastIncludedTerm)
	s.snapshot = &snapshotState{
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
		data:              data,
	}

	// Persist the now-shorter log. This is where the win lands: every subsequent
	// Save writes the compacted log, not the whole history.
	return true, s.persistLocked()
}

// discardThrough replaces the log prefix up to and including idx with a sentinel
// carrying that index and term. Caller must hold s.mu.
func (s *Server) discardThrough(idx Index, term Term) {
	tail := s.log[s.slot(idx)+1:]
	compacted := make([]LogEntry, 0, len(tail)+1)
	compacted = append(compacted, LogEntry{Term: term, Index: idx})
	compacted = append(compacted, tail...)
	s.log = compacted
}

// InstallSnapshot is the receiver for §7's RPC, sent when a follower needs
// entries the leader has already discarded.
//
// Figure 13's receiver rules, in order.
func (s *Server) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	s.mu.Lock()

	// 1. Reply immediately if term < currentTerm.
	if args.Term < s.currentTerm {
		reply := InstallSnapshotReply{Term: s.currentTerm}
		s.mu.Unlock()
		return reply
	}

	// The unconditional Figure 2 rule: a higher term always means step down.
	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.votedFor = nil
		s.role = Follower
	}
	s.leaderID = args.LeaderID
	s.resetElectionTimerLocked()

	// A snapshot we already cover adds nothing. Applying it anyway would rewind
	// a state machine that is AHEAD of it, which loses committed entries.
	if args.LastIncludedIndex <= s.baseIndex() || args.LastIncludedIndex <= s.lastApplied {
		reply := InstallSnapshotReply{Term: s.currentTerm}
		s.mu.Unlock()
		return reply
	}

	snapper, canRestore := s.sm.(Snapshotter)
	if !canRestore {
		// A state machine that cannot restore a snapshot must not silently accept
		// one: the log would be discarded while the state machine kept its old
		// contents, which is exactly the divergence State Machine Safety forbids.
		reply := InstallSnapshotReply{Term: s.currentTerm}
		s.mu.Unlock()
		return reply
	}

	if err := snapper.RestoreSnapshot(args.Data); err != nil {
		s.snapshotErrs++
		s.lastSnapshotErr = err
		reply := InstallSnapshotReply{Term: s.currentTerm}
		s.mu.Unlock()
		return reply
	}

	// If an existing entry has the same index and term as the snapshot's last
	// entry, retain the log that follows it (Figure 13 rule 6); otherwise discard
	// the whole log, because nothing after a diverged point can be trusted.
	if e, ok := s.entryAt(args.LastIncludedIndex); ok && e.Term == args.LastIncludedTerm {
		s.discardThrough(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		s.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	// The state machine now reflects everything through LastIncludedIndex, so both
	// markers move with it. Leaving lastApplied behind would re-apply entries the
	// snapshot already contains.
	s.lastApplied = args.LastIncludedIndex
	if s.commitIndex < args.LastIncludedIndex {
		s.commitIndex = args.LastIncludedIndex
	}
	s.snapshot = &snapshotState{
		lastIncludedIndex: args.LastIncludedIndex,
		lastIncludedTerm:  args.LastIncludedTerm,
		data:              args.Data,
	}

	if store, ok := s.storage.(SnapshotStorage); ok {
		if err := store.SaveSnapshot(uint64(args.LastIncludedIndex),
			uint64(args.LastIncludedTerm), args.Data); err != nil {
			s.snapshotErrs++
			s.lastSnapshotErr = err
		}
	}
	err := s.persistLocked()
	if err != nil {
		s.snapshotErrs++
		s.lastSnapshotErr = err
	}

	reply := InstallSnapshotReply{Term: s.currentTerm}
	s.mu.Unlock()
	return reply
}

// SnapshotInfo reports the current snapshot boundary, for tests and diagnostics.
func (s *Server) SnapshotInfo() (lastIncludedIndex Index, lastIncludedTerm Term, has bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return 0, 0, false
	}
	return s.snapshot.lastIncludedIndex, s.snapshot.lastIncludedTerm, true
}

// SnapshotFailures reports how many snapshot operations failed, with the most
// recent error. A non-zero count means compaction is not keeping up, or a
// follower could not be caught up.
func (s *Server) SnapshotFailures() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotErrs, s.lastSnapshotErr
}

// sendSnapshotTo catches up a follower that has fallen behind this leader's
// compacted prefix (§7).
//
// Called from replicateTo when nextIndex[peer] is at or below the snapshot
// boundary: those entries no longer exist in the log, so AppendEntries cannot
// carry them.
func (s *Server) sendSnapshotTo(peer NodeID, term Term) {
	s.mu.Lock()

	if s.role != Leader || s.currentTerm != term {
		s.mu.Unlock()
		return
	}
	if s.snapshot == nil {
		// nextIndex is below baseIndex but there is no snapshot to send. That should
		// be impossible — baseIndex only moves when a snapshot is taken or
		// installed — so it is counted rather than ignored: a "this cannot happen"
		// branch that silently does nothing is how a follower never converges and
		// nobody notices.
		s.snapshotErrs++
		s.lastSnapshotErr = errNoSnapshotToSend
		s.mu.Unlock()
		return
	}

	st, ok := s.transport.(SnapshotTransport)
	if !ok {
		// The transport predates snapshotting. Report it: this follower cannot be
		// caught up at all, and pretending otherwise leaves a permanently lagging
		// replica that still counts toward cluster size.
		s.snapshotErrs++
		s.lastSnapshotErr = errTransportCannotSnapshot
		s.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              s.currentTerm,
		LeaderID:          s.id,
		LastIncludedIndex: s.snapshot.lastIncludedIndex,
		LastIncludedTerm:  s.snapshot.lastIncludedTerm,
		Data:              s.snapshot.data,
		// Sent whole rather than chunked, per the project decision recorded on
		// InstallSnapshotArgs. Done marks that explicitly so a future chunked
		// implementation has a field to key off rather than a convention.
		Done: true,
	}
	s.mu.Unlock()

	reply, err := st.SendInstallSnapshot(peer, args)
	if err != nil {
		return // unreachable; the next heartbeat retries
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// All Servers rule, applied to a response: a higher term means step down.
	if reply.Term > s.currentTerm {
		s.becomeFollower(reply.Term)
		s.mustPersistLocked()
		return
	}
	if s.role != Leader || s.currentTerm != term {
		return
	}

	// The follower now holds everything through LastIncludedIndex, so replication
	// resumes from the entry after it. Without this the leader would send the same
	// snapshot every heartbeat forever.
	if args.LastIncludedIndex > s.matchIndex[peer] {
		s.matchIndex[peer] = args.LastIncludedIndex
	}
	if next := args.LastIncludedIndex + 1; next > s.nextIndex[peer] {
		s.nextIndex[peer] = next
	}

	// A snapshot install can advance the commit index, exactly like a successful
	// AppendEntries: this peer's matchIndex just moved.
	s.advanceCommitIndexLocked()

	// Contact is recorded on a successful install for the same reason it is
	// recorded on a successful AppendEntries (health.go): this peer answered and
	// accepted our authority, so it counts toward quorum.
	if s.lastContact == nil {
		s.lastContact = make(map[NodeID]time.Time)
	}
	s.lastContact[peer] = time.Now()
}
