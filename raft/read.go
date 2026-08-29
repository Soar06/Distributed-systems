package raft

import (
	"errors"
	"sync"
	"time"
)

// Linearizable reads — Raft §8.
//
// A read served straight from the leader's state machine can return stale data:
// the leader may already have been deposed by a newer leader it has not heard
// from yet. §8 requires two precautions:
//
//  1. The leader must know which entries are committed. It learns this by
//     committing a no-op entry from its own term at the start of its term (done in
//     becomeLeader).
//  2. The leader must confirm it has not been deposed, by exchanging heartbeats
//     with a majority BEFORE answering.
//
// This is the ReadIndex approach. The paper also mentions a lease-based
// alternative, which we deliberately do not implement: it "would rely on timing
// for safety (it assumes bounded clock skew)", and this project's premise is that
// safety must not depend on clock assumptions.

// ErrNotLeader is returned when a read or write is attempted on a non-leader.
// The client should retry against the leader (§8: a non-leader rejects and
// redirects).
var ErrNotLeader = errors.New("raft: not leader")

// ErrLostLeadership is returned when a leader could not confirm with a majority
// that it is still the leader. The read is refused rather than answered from
// possibly-stale state — this is Raft choosing consistency over availability,
// exactly as CAP describes.
var ErrLostLeadership = errors.New("raft: could not confirm leadership")

// LeaderID returns the current leader as far as this server knows, and whether
// this server is itself the leader. A follower reports the leader it last heard
// an AppendEntries from, which is what lets it redirect a client.
func (s *Server) LeaderID() (NodeID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderID, s.role == Leader
}

// ReadIndex confirms this server is still the leader and returns the index that a
// linearizable read must wait for.
//
// The returned index is the commit index as of the moment leadership was
// confirmed. A reader must wait until lastApplied reaches it before reading state,
// so the read reflects everything committed before the read began.
func (s *Server) ReadIndex(timeout time.Duration) (Index, error) {
	s.mu.Lock()
	if s.role != Leader {
		s.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := s.currentTerm
	idx := s.commitIndex
	me := s.id
	peers := append([]NodeID(nil), s.peers...)
	needed := s.majority()
	s.mu.Unlock()

	// Single-node cluster: this server is its own majority, nothing to confirm.
	if needed <= 1 {
		return idx, nil
	}

	// Precaution 2: exchange heartbeats with a majority. If a newer leader exists,
	// a peer replies with a higher term and we step down instead of answering.
	var (
		mu        sync.Mutex
		acks      = 1 // self
		confirmed = make(chan struct{})
		once      sync.Once
	)

	for _, p := range peers {
		if p == me {
			continue
		}
		go func(peer NodeID) {
			s.mu.Lock()
			prevIdx := s.lastIndex()
			prevTerm := s.lastTerm()
			commit := s.commitIndex
			s.mu.Unlock()

			reply, err := s.transport.SendAppendEntries(peer, AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				LeaderCommit: commit,
			})
			if err != nil {
				return
			}

			s.mu.Lock()
			if reply.Term > s.currentTerm {
				s.becomeFollower(reply.Term)
				s.mustPersistLocked()
				s.mu.Unlock()
				return
			}
			stillLeader := s.role == Leader && s.currentTerm == term
			s.mu.Unlock()
			if !stillLeader {
				return
			}

			mu.Lock()
			acks++
			enough := acks >= needed
			mu.Unlock()
			if enough {
				once.Do(func() { close(confirmed) })
			}
		}(p)
	}

	select {
	case <-confirmed:
		s.mu.Lock()
		defer s.mu.Unlock()
		// Re-check: leadership may have been lost while waiting.
		if s.role != Leader || s.currentTerm != term {
			return 0, ErrLostLeadership
		}
		return idx, nil
	case <-time.After(timeout):
		return 0, ErrLostLeadership
	}
}

// LinearizableRead confirms leadership, waits for the state machine to catch up
// to the confirmed read index, and then runs fn against the state machine.
//
// fn is called while no other apply is in progress, so it observes a consistent
// snapshot.
func (s *Server) LinearizableRead(timeout time.Duration, fn func(StateMachine)) error {
	idx, err := s.ReadIndex(timeout)
	if err != nil {
		return err
	}

	// Wait for lastApplied to reach the read index, so the read reflects
	// everything committed before it began.
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		applied := s.lastApplied
		s.mu.Unlock()
		if applied >= idx {
			break
		}
		if time.Now().After(deadline) {
			return ErrLostLeadership
		}
		time.Sleep(time.Millisecond)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.sm)
	return nil
}
