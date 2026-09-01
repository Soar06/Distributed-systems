package raft

import (
	"math/rand"
	"sync"
	"time"
)

// The role loop — Figure 2's "Rules for Servers" box, which the paper describes
// as "a set of rules that trigger independently and repeatedly."
//
// This is the part that makes Raft *run*: the receiver handlers in server.go only
// react to inbound RPCs, whereas everything here acts on its own — timing out,
// starting elections, sending heartbeats, and advancing the commit index.

// Start begins the role loop in the background. Stop must be called to shut it
// down. Start is a no-op if the loop is already running.
func (s *Server) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.resetElectionTimerLocked()
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run()
}

// Stop shuts the role loop down and waits for it to exit. Safe to call more than
// once. This models a node crash in tests: the server stops acting, but its
// state remains inspectable.
func (s *Server) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.stopped = true // latched: this node no longer participates in consensus
	close(s.stopCh)
	s.mu.Unlock()

	s.wg.Wait()
}

// run is the main loop. It ticks frequently and checks what the current role
// requires, rather than modelling each role as its own long-lived goroutine.
// A single loop is easier to reason about and makes role transitions atomic with
// respect to the tick.
func (s *Server) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 5)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

// tick performs one iteration of the Rules for Servers.
func (s *Server) tick() {
	s.mu.Lock()
	role := s.role
	timedOut := !time.Now().Before(s.electionDeadline)
	heartbeatDue := !time.Now().Before(s.heartbeatDeadline)
	s.mu.Unlock()

	switch role {
	case Follower, Candidate:
		// Followers: "If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate: convert to
		// candidate."
		//
		// Candidates: "If election timeout elapses: start new election." Both
		// cases start an election, which is why they share a branch.
		if timedOut {
			s.startElection()
		}
	case Leader:
		// Leaders: "send initial empty AppendEntries RPCs (heartbeat) to each
		// server; repeat during idle periods to prevent election timeouts."
		if heartbeatDue {
			s.broadcastAppendEntries()
		}
	}
}

// resetElectionTimerLocked picks a new randomized election deadline.
// Caller must hold s.mu.
//
// The randomization is essential (§5.2): a fixed timeout makes servers time out
// in lockstep, split the vote, and potentially livelock. Spreading the timeouts
// makes it likely one server times out first and wins outright.
func (s *Server) resetElectionTimerLocked() {
	span := s.cfg.ElectionTimeoutMax - s.cfg.ElectionTimeoutMin
	d := s.cfg.ElectionTimeoutMin

	if span > 0 {
		// Randomized across the FULL window first (§5.2: randomization is what
		// breaks split votes), then scaled by health so a healthier node tends to
		// campaign sooner (health_priority.go).
		//
		// Scaling rather than banding is deliberate: confining each health level to
		// its own slice of the window narrowed the randomness and raised the
		// minimum timeout, which broke elections outright.
		d += int64(float64(s.rnd.Int63n(span)) * s.NodeHealth().electionBias())
	}

	s.electionDeadline = time.Now().Add(time.Duration(d) * time.Millisecond)
}

// startElection implements the Candidates block of Figure 2:
//
//	On conversion to candidate, start election:
//	  - Increment currentTerm
//	  - Vote for self
//	  - Reset election timer
//	  - Send RequestVote RPCs to all other servers
func (s *Server) startElection() {
	s.mu.Lock()

	s.role = Candidate
	s.currentTerm++
	me := s.id
	s.votedFor = &me
	s.resetElectionTimerLocked()

	// The new term and self-vote must be durable before any RequestVote goes out:
	// a crash after campaigning but before persisting could let this server vote
	// again in the same term after restart.
	s.mustPersistLocked()

	term := s.currentTerm
	lastIdx, lastTerm := s.lastIndex(), s.lastTerm()
	peers := append([]NodeID(nil), s.peers...)
	needed := s.majority()
	s.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Votes are counted in a closure guarded by its own mutex: the election must
	// resolve as soon as a majority replies, without waiting for slow or
	// unreachable peers. That is the point of "a command can complete as soon as
	// a majority of the cluster has responded" (§2).
	var (
		mu       sync.Mutex
		votes    = 1 // vote for self
		resolved bool
	)

	// A single-node cluster is its own majority: the self-vote already wins, and
	// there are no peers to ask. Without this the election never resolves, because
	// the tally below is only ever checked inside a per-peer goroutine — so a
	// one-node cluster could never elect a leader or make any progress at all.
	if votes >= needed {
		s.becomeLeader(term)
		return
	}

	for _, p := range peers {
		if p == me {
			continue
		}
		go func(peer NodeID) {
			if s.transport == nil {
				return // no transport configured; nothing to send
			}
			reply, err := s.transport.SendRequestVote(peer, args)
			if err != nil {
				return // unreachable peers simply do not vote
			}

			s.mu.Lock()
			// All Servers rule: a higher term in a *response* also causes a step
			// down. Missing this is a classic bug — the rule applies to replies,
			// not just requests.
			if reply.Term > s.currentTerm {
				s.becomeFollower(reply.Term)
				s.mustPersistLocked()
				s.mu.Unlock()
				return
			}
			// Discard a reply that arrived after we already moved on, otherwise a
			// late vote from an old election could wrongly promote us.
			stale := s.currentTerm != term || s.role != Candidate
			s.mu.Unlock()
			if stale || !reply.VoteGranted {
				return
			}

			mu.Lock()
			votes++
			won := !resolved && votes >= needed
			if won {
				resolved = true
			}
			mu.Unlock()

			if won {
				s.becomeLeader(term)
			}
		}(p)
	}
}

// becomeLeader promotes this server, but only if it is still a candidate in the
// term it campaigned for.
//
// Figure 2, Leaders: nextIndex is initialized to leader last log index + 1, and
// matchIndex to 0. The optimistic nextIndex is what drives the decrement-and-retry
// probe that finds where a follower's log diverges (§5.3).
func (s *Server) becomeLeader(term Term) {
	s.mu.Lock()
	if s.role != Candidate || s.currentTerm != term {
		s.mu.Unlock()
		return // the world moved on while votes were being counted
	}

	s.role = Leader
	s.leaderID = s.id
	next := s.lastIndex() + 1
	for _, p := range s.peers {
		s.nextIndex[p] = next
		s.matchIndex[p] = 0
	}
	// A leader trivially matches its own log; this makes the commit-index
	// majority count uniform over all peers.
	s.matchIndex[s.id] = s.lastIndex()

	// Contact history starts empty for a new term.
	//
	// Carrying it over would let a leader elected inside a minority partition
	// report quorum from timestamps recorded BEFORE the partition — the readiness
	// signal would then be a memory of a majority that no longer exists, which is
	// precisely the blindness it exists to remove. Rebuilt from the first round of
	// heartbeats, which follows immediately.
	s.lastContact = make(map[NodeID]time.Time, len(s.peers))

	// §8: "it needs to commit an entry from its term. Raft handles this by having
	// each leader commit a blank no-op entry into the log at the start of its
	// term."
	//
	// Without this, entries carried over from a previous term can never be
	// committed by a new leader — §5.4.2 forbids committing a previous term's
	// entry directly, so nothing would ever advance commitIndex after a failover
	// until fresh client traffic arrived. A no-op with a nil command commits under
	// the leader's own term and drags the earlier entries along with it via Log
	// Matching.
	idx := s.lastIndex() + 1
	s.log = append(s.log, LogEntry{Term: s.currentTerm, Index: idx, Command: nil})
	s.matchIndex[s.id] = idx
	s.nextIndex[s.id] = idx + 1
	s.mustPersistLocked()
	s.advanceCommitIndexLocked() // a single-node cluster commits it immediately
	s.mu.Unlock()

	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
	// server" — this asserts leadership immediately and stops other servers from
	// timing out.
	s.broadcastAppendEntries()
}

// broadcastAppendEntries sends AppendEntries to every peer, carrying whatever
// entries that peer is missing (empty for a pure heartbeat).
func (s *Server) broadcastAppendEntries() {
	s.mu.Lock()
	if s.role != Leader {
		s.mu.Unlock()
		return
	}
	s.heartbeatDeadline = time.Now().Add(time.Duration(s.cfg.HeartbeatInterval) * time.Millisecond)
	term := s.currentTerm
	me := s.id
	peers := append([]NodeID(nil), s.peers...)
	s.mu.Unlock()

	for _, p := range peers {
		if p == me {
			continue
		}
		go s.replicateTo(p, term)
	}
}

// replicateTo sends one AppendEntries to a single peer and processes the reply.
//
// Figure 2, Leaders:
//   - If last log index >= nextIndex: send AppendEntries with entries starting at
//     nextIndex.
//   - If successful: update nextIndex and matchIndex.
//   - If AppendEntries fails because of log inconsistency: decrement nextIndex
//     and retry.
func (s *Server) replicateTo(peer NodeID, term Term) {
	s.mu.Lock()
	if s.role != Leader || s.currentTerm != term {
		s.mu.Unlock()
		return
	}

	next := s.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	// The follower needs entries this leader has already discarded (§7). They
	// cannot be sent as AppendEntries because they no longer exist, so the leader
	// sends a snapshot instead.
	//
	// This branch is not merely an optimization: without it s.slot(next) computes a
	// NEGATIVE slice index and the leader panics. A follower falling behind a
	// compacted leader is routine — it restarts, or is briefly partitioned — so
	// this crashed the leader on an ordinary event.
	if next <= s.baseIndex() {
		s.mu.Unlock()
		s.sendSnapshotTo(peer, term)
		return
	}

	prevIdx := next - 1
	prevTerm, ok := s.termAt(prevIdx)
	if !ok {
		prevTerm = 0
	}

	var entries []LogEntry
	if s.lastIndex() >= next {
		entries = append(entries, s.log[s.slot(next):]...)
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     s.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	s.mu.Unlock()

	if s.transport == nil {
		// NewServer explicitly permits a nil transport, for testing the RPC
		// receiver rules in isolation. Dereferencing it here crashed the process
		// rather than simply not replicating — a latent panic on a documented
		// configuration.
		return
	}
	reply, err := s.transport.SendAppendEntries(peer, args)
	if err != nil {
		return // unreachable; the next heartbeat retries
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// All Servers rule, applied to a response.
	if reply.Term > s.currentTerm {
		s.becomeFollower(reply.Term)
		s.mustPersistLocked()
		return
	}
	if s.role != Leader || s.currentTerm != term {
		return // no longer the leader we were when this was sent
	}

	// Quorum contact is recorded only on SUCCESS.
	//
	// The tempting alternative — count any reply, on the grounds that a
	// consistency-check failure is a healthy peer disagreeing about its log rather
	// than an unreachable one — is wrong, and a test caught it. A STOPPED node also
	// replies, with Success=false: raft.Server latches `stopped` and refuses to
	// participate while its RPC handlers keep answering over the shared listener.
	// Counting that as contact made a leader whose every peer had been shut down
	// still report a full quorum, which is precisely the blindness readiness exists
	// to remove.
	//
	// A peer that is genuinely repairing its log converges within a few rounds and
	// then succeeds, so the cost of this stricter rule is a brief not-ready window
	// during repair. That is the right trade: reporting NOT ready while repairing is
	// conservative, reporting ready while isolated is dangerous.
	if reply.Success {
		if s.lastContact == nil {
			s.lastContact = make(map[NodeID]time.Time)
		}
		s.lastContact[peer] = time.Now()
	}

	if reply.Success {
		// Derive from what we sent, never from current log length: the log may
		// have grown since this RPC went out.
		if m := prevIdx + Index(len(entries)); m > s.matchIndex[peer] {
			s.matchIndex[peer] = m
			s.nextIndex[peer] = m + 1
		}
		s.advanceCommitIndexLocked()
		return
	}

	// Failed the consistency check: back up and probe again on the next round.
	if s.nextIndex[peer] > 1 {
		s.nextIndex[peer]--
	}
}

// advanceCommitIndexLocked implements the final Leaders rule (§5.3, §5.4):
//
//	If there exists an N such that N > commitIndex, a majority of
//	matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N.
//
// Caller must hold s.mu.
//
// The log[N].term == currentTerm check is a safety requirement, not an
// optimization. §5.4.2 shows that committing an entry from an *earlier* term
// merely because it is present on a majority can result in that entry later being
// overwritten — which would violate State Machine Safety. A leader only ever
// commits entries from its own term; earlier entries become committed indirectly,
// carried along by the Log Matching property.
func (s *Server) advanceCommitIndexLocked() {
	if s.role != Leader {
		return
	}

	for n := s.lastIndex(); n > s.commitIndex; n-- {
		t, ok := s.termAt(n)
		if !ok || t != s.currentTerm {
			continue // never commit an entry from a previous term directly
		}

		count := 0
		for _, p := range s.peers {
			if s.matchIndex[p] >= n {
				count++
			}
		}
		if count >= s.majority() {
			s.commitIndex = n
			s.applyCommitted()
			// A leader that has just committed a configuration removing itself must
			// step down — but only NOW, not when the entry was appended. It had to
			// keep serving until the change committed, or it would have stranded the
			// very entry that removes it (membership.go).
			s.checkSelfRemovalLocked()
			return
		}
	}
}

// Submit appends a command to the leader's log so it can be replicated.
//
// Figure 2, Leaders: "If command received from client: append entry to local log,
// respond after entry applied to state machine." This returns as soon as the
// entry is appended; the caller waits for commitment by observing commitIndex.
// Wiring the client-facing wait is the client API's job (DESIGN.md §8).
//
// Returns the entry's index and term, and false if this server is not the leader
// — a non-leader must reject and redirect, never accept a write (NOW.md).
func (s *Server) Submit(cmd []byte) (Index, Term, bool) {
	s.mu.Lock()
	if s.role != Leader || s.stopped {
		s.mu.Unlock()
		return 0, 0, false
	}

	idx := s.lastIndex() + 1
	term := s.currentTerm
	s.log = append(s.log, LogEntry{Term: term, Index: idx, Command: cmd})
	s.matchIndex[s.id] = idx

	// The leader must have the entry durably before counting itself toward the
	// replication majority — otherwise its own vote for commitment is a promise
	// it might not keep.
	s.mustPersistLocked()

	// A single-node cluster commits immediately: it is its own majority.
	s.advanceCommitIndexLocked()
	s.mu.Unlock()

	// Replicate right away rather than waiting for the next heartbeat.
	s.broadcastAppendEntries()
	return idx, term, true
}

// newRand returns a per-server random source. Each server gets its own so that
// election timeouts are independent, and so a seeded simulation is reproducible.
func newRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}
