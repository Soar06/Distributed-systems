package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Persistence for the Figure 2 persistent state: currentTerm, votedFor, log[].
//
// The paper requires this reach stable storage BEFORE the server responds to an
// RPC. Every mutation of persistent state in server.go and loop.go is therefore
// followed by persistLocked() before the reply is returned.

// Storage is the durability interface Raft depends on. storage.WAL implements it;
// tests use an in-memory version that can simulate a crash.
type Storage interface {
	// Save durably records the full persistent state. It must not return until
	// the data would survive a power loss.
	Save(state []byte) error

	// Load returns the most recently saved state, or nil if none exists.
	Load() ([]byte, error)
}

// AppliedStorage is an optional extension for durably recording how far the state
// machine has been applied.
//
// Figure 2 makes commitIndex and lastApplied volatile, and a node re-learns its
// commit index from the leader. But a state machine holding state a client has
// already been told about — a committed transfer, a 2PC promise — cannot simply
// start empty and wait: between restart and the leader's next AppendEntries, the
// node would answer reads from nothing. Recording lastApplied lets Restore replay
// the log deterministically and come back with the same state it had.
// The index is a plain uint64 rather than raft.Index so that storage
// implementations need not import this package — the dependency stays
// one-directional, raft -> storage, never the reverse.
type AppliedStorage interface {
	SaveApplied(index uint64) error
	LoadApplied() (uint64, error)
}

// encodeState serializes persistent state.
//
// Format: [8 term][1 hasVote][2 voteLen][vote][8 entryCount], then per entry
// [8 term][8 index][4 cmdLen][cmd]. Fixed-width and little-endian throughout so
// decoding cannot depend on platform specifics.
func encodeState(term Term, votedFor *NodeID, log []LogEntry) []byte {
	var b bytes.Buffer

	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], uint64(term))
	b.Write(scratch[:])

	if votedFor == nil {
		b.WriteByte(0)
		binary.LittleEndian.PutUint16(scratch[:2], 0)
		b.Write(scratch[:2])
	} else {
		b.WriteByte(1)
		v := []byte(*votedFor)
		binary.LittleEndian.PutUint16(scratch[:2], uint16(len(v)))
		b.Write(scratch[:2])
		b.Write(v)
	}

	binary.LittleEndian.PutUint64(scratch[:], uint64(len(log)))
	b.Write(scratch[:])

	for _, e := range log {
		binary.LittleEndian.PutUint64(scratch[:], uint64(e.Term))
		b.Write(scratch[:])
		binary.LittleEndian.PutUint64(scratch[:], uint64(e.Index))
		b.Write(scratch[:])
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(e.Command)))
		b.Write(scratch[:4])
		b.Write(e.Command)
	}
	return b.Bytes()
}

var errShortState = errors.New("raft: truncated persisted state")

// decodeState is the inverse of encodeState.
func decodeState(data []byte) (Term, *NodeID, []LogEntry, error) {
	r := bytes.NewReader(data)

	readU64 := func() (uint64, error) {
		var buf [8]byte
		if _, err := r.Read(buf[:]); err != nil {
			return 0, errShortState
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	}

	termRaw, err := readU64()
	if err != nil {
		return 0, nil, nil, err
	}

	hasVote, err := r.ReadByte()
	if err != nil {
		return 0, nil, nil, errShortState
	}
	var voteLenBuf [2]byte
	if _, err := r.Read(voteLenBuf[:]); err != nil {
		return 0, nil, nil, errShortState
	}
	voteLen := binary.LittleEndian.Uint16(voteLenBuf[:])

	var votedFor *NodeID
	if hasVote == 1 {
		v := make([]byte, voteLen)
		if _, err := r.Read(v); err != nil {
			return 0, nil, nil, errShortState
		}
		id := NodeID(v)
		votedFor = &id
	}

	count, err := readU64()
	if err != nil {
		return 0, nil, nil, err
	}

	log := make([]LogEntry, 0, count)
	for range count {
		et, err := readU64()
		if err != nil {
			return 0, nil, nil, err
		}
		ei, err := readU64()
		if err != nil {
			return 0, nil, nil, err
		}
		var lenBuf [4]byte
		if _, err := r.Read(lenBuf[:]); err != nil {
			return 0, nil, nil, errShortState
		}
		cmdLen := binary.LittleEndian.Uint32(lenBuf[:])

		var cmd []byte
		if cmdLen > 0 {
			cmd = make([]byte, cmdLen)
			if _, err := r.Read(cmd); err != nil {
				return 0, nil, nil, errShortState
			}
		}
		log = append(log, LogEntry{Term: Term(et), Index: Index(ei), Command: cmd})
	}

	return Term(termRaw), votedFor, log, nil
}

// persistLocked writes the current persistent state durably.
// Caller must hold s.mu.
//
// A failure here is fatal to correctness: continuing after a failed persist would
// mean responding to an RPC on the strength of state that may not survive a
// crash, which is exactly what Figure 2 forbids. The error is surfaced so the
// caller can decide, and callers in this package treat it as unrecoverable.
func (s *Server) persistLocked() error {
	if s.storage == nil {
		return nil // in-memory only; used by tests that do not exercise durability
	}
	return s.storage.Save(encodeState(s.currentTerm, s.votedFor, s.log))
}

// mustPersistLocked persists and panics on failure.
//
// Panicking is deliberate. If durable storage is failing, the node cannot
// participate safely: a vote or log entry it has acknowledged might vanish on
// restart. Crashing is the correct behavior — Raft tolerates a crashed node, but
// it does not tolerate a node that lies about what it has durably recorded.
func (s *Server) mustPersistLocked() {
	if err := s.persistLocked(); err != nil {
		panic(fmt.Sprintf("raft: cannot persist state, refusing to continue: %v", err))
	}
}

// Restore loads persistent state from storage, replacing in-memory state.
// Called once at startup, before the role loop starts.
//
// Note what is NOT restored: commitIndex and lastApplied are volatile and reset
// to 0 (Figure 2). The node re-learns its commit index from the leader and
// re-applies entries to rebuild the state machine — which is safe precisely
// because applying the same log in the same order is deterministic.
func (s *Server) Restore() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.storage == nil {
		return nil
	}
	data, err := s.storage.Load()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil // nothing persisted yet
	}

	term, votedFor, log, err := decodeState(data)
	if err != nil {
		return err
	}
	if len(log) == 0 {
		// A persisted log always includes the sentinel; an empty one means the
		// record was malformed.
		return errShortState
	}

	s.currentTerm = term
	s.votedFor = votedFor
	s.log = log
	s.role = Follower

	// Membership is derived from the log, so a restarting server re-learns it the
	// same way it re-learns everything else. Without this a node would come back
	// with the configuration it was STARTED with — the one in its flags — and a
	// cluster that had since added or removed a server would have one member
	// counting quorum against a membership that no longer exists.
	s.adoptConfigFromLogLocked()

	// Figure 2: commitIndex and lastApplied are volatile and restart at 0. The
	// node re-learns its commit index from the leader.
	s.commitIndex = 0
	s.lastApplied = 0

	// Rebuild the state machine by replaying the log up to where it had previously
	// been applied. §7 describes exactly this — snapshotting exists *because*
	// "as the log grows longer, it occupies more space and takes more time to
	// replay", which presumes replay is how a restarted node recovers.
	//
	// This is safe precisely because applying the same log in the same order is
	// deterministic. Without it, a restarted node's ledger and its 2PC promises
	// start empty: a participant that voted YES would forget it, and the funds it
	// reserved would become spendable again while the transaction is still live.
	// Load the snapshot FIRST, before any replay.
	//
	// A compacted node's log no longer contains the entries before the snapshot
	// boundary, so replaying the log alone would rebuild the state machine from a
	// partial history — balances short by everything the snapshot covers, and 2PC
	// promises missing entirely. The snapshot is the base; the log is the delta.
	if ss, ok := s.storage.(SnapshotStorage); ok && s.sm != nil {
		idx, term, data, found, err := ss.LoadSnapshot()
		if err != nil {
			return err
		}
		if found {
			snapper, canRestore := s.sm.(Snapshotter)
			if !canRestore {
				// The log prefix is gone and this state machine cannot consume the
				// snapshot that replaced it. Booting anyway would serve reads from a
				// state machine missing everything before the boundary, which is worse
				// than not booting.
				return fmt.Errorf("raft: a snapshot at index %d exists but the state "+
					"machine cannot restore it; the compacted log prefix is unrecoverable", idx)
			}
			if err := snapper.RestoreSnapshot(data); err != nil {
				return fmt.Errorf("raft: restore snapshot at index %d: %w", idx, err)
			}
			s.snapshot = &snapshotState{
				lastIncludedIndex: Index(idx),
				lastIncludedTerm:  Term(term),
				data:              data,
			}
			// The state machine now reflects everything through the boundary, so
			// replay must start after it rather than from index 1.
			s.commitIndex = Index(idx)
			s.lastApplied = Index(idx)

			// A log that predates its own snapshot cannot happen if writes are
			// ordered (snapshot first, then the truncated log), but a torn or rolled
			// back state file could produce it. Rebase onto the snapshot rather than
			// replaying entries the snapshot already covers.
			if len(s.log) > 0 && s.log[0].Index < Index(idx) {
				if e, ok := s.entryAt(Index(idx)); ok && e.Term == Term(term) {
					s.discardThrough(Index(idx), Term(term))
				} else {
					s.log = []LogEntry{{Term: Term(term), Index: Index(idx)}}
				}
			}
		}
	}

	if as, ok := s.storage.(AppliedStorage); ok && s.sm != nil {
		raw, err := as.LoadApplied()
		if err != nil {
			return err
		}
		prevApplied := Index(raw)
		if prevApplied < s.baseIndex() {
			// The marker predates the snapshot, which is normal: the snapshot itself
			// advanced the applied point. The restored state machine is already at
			// the boundary, so there is nothing to replay.
			prevApplied = s.baseIndex()
		}
		if prevApplied > s.lastIndex() {
			// The applied marker is ahead of the log. This is NOT impossible, and an
			// earlier version of this code wrongly refused to start because of it:
			// a follower can apply entries 1..5, then AppendEntries receiver rule 3
			// truncates its log to 3 when a new leader replaces a diverged suffix.
			// The marker and the log are separate files with no ordering between
			// them, so they can and do diverge.
			//
			// Clamp and continue. Refusing to boot turns a routine, recoverable
			// state into a permanent outage — the same reasoning AppliedFile.Load
			// already applies to a corrupt record.
			prevApplied = s.lastIndex()
		}
		// Replay starts after the snapshot boundary. Entries at or below it are
		// already reflected in the restored state machine, and re-applying them
		// would double-count every one.
		for i := s.baseIndex() + 1; i <= prevApplied; i++ {
			e, ok := s.entryAt(i)
			if !ok {
				return fmt.Errorf("raft: missing log entry %d during replay", i)
			}
			if e.Command == nil {
				continue // a no-op entry carries nothing to apply
			}
			if ism, ok := s.sm.(IndexedStateMachine); ok {
				ism.ApplyAt(e.Index, e.Command)
			} else {
				s.sm.Apply(e.Command)
			}
		}
		if prevApplied > s.commitIndex {
			s.commitIndex = prevApplied
		}
		if prevApplied > s.lastApplied {
			s.lastApplied = prevApplied
		}
	}
	return nil
}
