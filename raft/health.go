package raft

import "time"

// Health signals: can this server actually make progress? (G5)
//
// The distinction that matters, and the reason this file exists: a Raft node that
// has lost quorum still accepts connections, still answers RPCs, still reports a
// role, and its leader still believes it leads. Every superficial signal is green.
// What it cannot do is COMMIT anything.
//
// From outside, a cluster committing against a degraded quorum is
// indistinguishable from a healthy one — and for a bank that means writes which
// appear accepted and are not, the same class of failure as the Indeterminate
// hazard in the client contract. Theory in learn/READING_LIST.md §16.
//
// So liveness and readiness are answered separately:
//
//   - ALIVE: the process is running and its role loop has not wedged. A live-but-
//     stuck node is a candidate for a restart.
//   - READY: this node can serve. A leader is ready when it has heard from a
//     majority recently enough to believe it still leads; a follower is ready when
//     it has heard from a leader within an election timeout. A node that is
//     perfectly alive and cannot reach quorum is ALIVE and NOT READY.
//
// Conflating the two produces opposite failures, both damaging: a readiness check
// wired to liveness restarts nodes that were merely waiting out a partition,
// turning a recoverable degradation into a restart storm; a liveness check wired
// to readiness keeps routing writes to a node that cannot commit.

// Health is a point-in-time summary of one Raft server, for /healthz, /readyz,
// /metrics, and the Phase 4 dashboard.
type Health struct {
	ID          NodeID
	Role        Role
	Term        Term
	CommitIndex Index
	LastApplied Index
	LogLength   int
	LeaderID    NodeID

	// Ready reports whether this node is in a quorum that can make progress.
	Ready bool

	// NotReadyReason explains a false Ready in operator-readable terms. Empty when
	// ready. A boolean with no reason forces whoever is paged at 3am to guess.
	NotReadyReason string

	// QuorumContact is how many servers (including self) this leader has heard
	// from within the readiness window. Zero for a follower, which has no such
	// view — only the leader tracks peer contact.
	QuorumContact int

	// QuorumNeeded is the majority size for the full cluster.
	QuorumNeeded int

	// Snapshot state (§7), so an operator can see compaction working.
	SnapshotIndex Index
	HasSnapshot   bool

	// Error counters. These are the "this shouldn't happen" branches, counted
	// rather than silently swallowed.
	AppliedPersistFailures int
	SnapshotFailures       int

	// ConfigFailures counts malformed configuration entries seen. Non-zero means
	// this node may be operating under the wrong membership, which is a SAFETY
	// problem rather than an availability one — it decides who counts toward
	// quorum — so it must be visible rather than merely counted.
	ConfigFailures int

	// ConfigServers is the membership this node is currently operating under.
	// Exposed so an operator can see a reconfiguration propagate, and spot a node
	// that has been left behind on an old configuration.
	ConfigServers []NodeID
}

// Health returns this server's current health.
//
// readinessWindow bounds how stale peer contact may be before a leader stops
// counting it toward quorum. It should be at least a heartbeat interval and well
// under the election timeout: shorter than a heartbeat and a healthy leader flaps
// between ready and not, longer than an election timeout and readiness outlives
// the leadership it describes.
func (s *Server) Health(readinessWindow time.Duration) Health {
	s.mu.Lock()
	defer s.mu.Unlock()

	h := Health{
		ID:                     s.id,
		Role:                   s.role,
		Term:                   s.currentTerm,
		CommitIndex:            s.commitIndex,
		LastApplied:            s.lastApplied,
		LogLength:              len(s.log) - 1,
		LeaderID:               s.leaderID,
		QuorumNeeded:           s.majority(),
		AppliedPersistFailures: s.appliedErrs,
		SnapshotFailures:       s.snapshotErrs,
		ConfigFailures:         s.configErrs,
		ConfigServers:          append([]NodeID(nil), s.peers...),
	}
	if s.snapshot != nil {
		h.SnapshotIndex = s.snapshot.lastIncludedIndex
		h.HasSnapshot = true
	}

	if s.stopped {
		h.NotReadyReason = "server is stopped"
		return h
	}

	now := time.Now()

	switch s.role {
	case Leader:
		// A leader is ready only while it can still see a majority. Counting self
		// alone would make a completely partitioned leader report ready — which is
		// exactly the degraded-quorum blindness this exists to remove.
		contact := 1 // self
		for _, p := range s.peers {
			if p == s.id {
				continue
			}
			if last, ok := s.lastContact[p]; ok && now.Sub(last) <= readinessWindow {
				contact++
			}
		}
		h.QuorumContact = contact
		if contact >= h.QuorumNeeded {
			h.Ready = true
		} else {
			h.NotReadyReason = "leader has heard from " + itoa(contact) + " of " +
				itoa(h.QuorumNeeded) + " needed for quorum; cannot commit"
		}

	case Follower:
		// A follower is ready if a leader has contacted it within an election
		// timeout. Beyond that it is about to start an election anyway, and
		// answering reads from it would be answering from a node that has lost
		// touch with the cluster.
		if s.leaderID == "" {
			h.NotReadyReason = "no known leader"
		} else if now.After(s.electionDeadline) {
			h.NotReadyReason = "no contact from leader " + string(s.leaderID) +
				" within the election timeout"
		} else {
			h.Ready = true
		}

	case Candidate:
		// Mid-election: by definition not serving.
		h.NotReadyReason = "election in progress"
	}

	return h
}

// itoa is a tiny non-negative int formatter, to keep this file free of fmt in a
// path that runs on every scrape.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
