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
	s.commitIndex = 0
	s.lastApplied = 0
	s.role = Follower
	return nil
}
