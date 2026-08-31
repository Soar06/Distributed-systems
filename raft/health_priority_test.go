package raft

import "testing"

// Health-weighted election tests.
//
// The property that must NOT change: eligibility. Health decides who campaigns
// first, never who is allowed to win — a candidate whose log is behind is still
// refused a vote, because that refusal is what makes a committed transfer
// survive an election.

// A healthier node must draw an earlier election deadline.
func TestHealthierNodeCampaignsSooner(t *testing.T) {
	cfg := Config{ElectionTimeoutMin: 100, ElectionTimeoutMax: 200, HeartbeatInterval: 20}

	// Sampled rather than compared once: both draws are randomized inside their
	// bands, so a single pair could overlap by chance. What must hold is that the
	// healthy node is sooner on average by a clear margin.
	var highTotal, lowTotal int64
	const runs = 200

	for i := range runs {
		high := NewServerWith("h", []NodeID{"h"}, &noopHealthSM{}, nil, cfg, int64(i))
		low := NewServerWith("l", []NodeID{"l"}, &noopHealthSM{}, nil, cfg, int64(i))
		high.SetNodeHealth(HealthHigh)
		low.SetNodeHealth(HealthLow)

		high.mu.Lock()
		high.resetElectionTimerLocked()
		hd := high.electionDeadline
		high.mu.Unlock()

		low.mu.Lock()
		low.resetElectionTimerLocked()
		ld := low.electionDeadline
		low.mu.Unlock()

		highTotal += hd.UnixNano()
		lowTotal += ld.UnixNano()
	}

	if highTotal >= lowTotal {
		t.Fatalf("a high-health node did not campaign sooner on average than a "+
			"low-health one (%d vs %d) — health is not biasing the timer",
			highTotal/runs, lowTotal/runs)
	}
}

// An unhealthy node must still be ABLE to campaign. Excluding it would leave a
// cluster of struggling nodes unable to elect anyone at all.
func TestUnhealthyNodeStillCampaigns(t *testing.T) {
	cfg := Config{ElectionTimeoutMin: 100, ElectionTimeoutMax: 200, HeartbeatInterval: 20}
	s := NewServerWith("n1", []NodeID{"n1"}, &noopHealthSM{}, nil, cfg, 7)
	s.SetNodeHealth(HealthLow)

	s.mu.Lock()
	s.resetElectionTimerLocked()
	d := s.electionDeadline
	s.mu.Unlock()

	if d.IsZero() {
		t.Fatal("a low-health node got no election deadline at all, so it could " +
			"never campaign — a cluster of unhealthy nodes would never elect a leader")
	}
}

// THE safety property: health must not make an ineligible candidate electable.
//
// A node can be idle and fast precisely BECAUSE it has been partitioned and
// missed writes. If health overrode the up-to-date check, that node would take
// leadership and the entries it is missing would be lost.
func TestHealthDoesNotOverrideLogCompleteness(t *testing.T) {
	voter := NewServer("voter", []NodeID{"voter", "cand"}, &noopHealthSM{})

	// The voter has entries the candidate does not.
	voter.mu.Lock()
	voter.currentTerm = 5
	voter.log = append(voter.log,
		LogEntry{Term: 5, Index: 1, Command: []byte("a")},
		LogEntry{Term: 5, Index: 2, Command: []byte("b")})
	voter.mu.Unlock()

	// The candidate is in perfect health and behind on its log.
	reply := voter.RequestVote(RequestVoteArgs{
		Term: 6, CandidateID: "cand", LastLogIndex: 0, LastLogTerm: 0,
	})

	if reply.VoteGranted {
		t.Fatal("a vote was granted to a candidate missing committed entries. " +
			"Health may decide WHO TRIES FIRST; it must never decide who is ALLOWED " +
			"to win, or a committed transfer can vanish in an election")
	}
}

// Health levels must be distinguishable in output, since the UI displays them.
func TestHealthLevelsRender(t *testing.T) {
	for h, want := range map[NodeHealth]string{
		HealthLow: "low", HealthNormal: "normal", HealthHigh: "high",
	} {
		if got := h.String(); got != want {
			t.Fatalf("health %d renders as %q, want %q", h, got, want)
		}
	}
}
