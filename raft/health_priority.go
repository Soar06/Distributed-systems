package raft

import "sync/atomic"

// Health-weighted leader election.
//
// Raft's own choice among candidates is ARBITRARY BY DESIGN. Two mechanisms are
// at work in an election and only one of them is a guarantee:
//
//   - WHO IS ELIGIBLE — decided by the up-to-date check in RequestVote
//     (§5.4.1). A candidate whose log is missing committed entries is refused a
//     vote, full stop. This is Leader Completeness, and it is what makes a
//     committed transfer survive an election.
//
//   - WHICH ELIGIBLE ONE WINS — decided by whoever's randomized timer fires
//     first. The randomization exists to break split votes, not to pick well:
//     any node with a complete log is equally correct as leader, so Raft picks
//     fast rather than picking carefully.
//
// That second choice being don't-care is what makes this feature safe. Health
// replaces "whoever happened to time out first" with "whoever is in the best
// shape" — and changes nothing about who is ALLOWED to win.
//
// [project decision] Health biases the election TIMER, it does not override the
// vote. A healthier node waits less before campaigning, so it usually gets there
// first; an unhealthy node still wins if it is the only eligible one. Making
// health authoritative instead would let a node that is idle and fast BECAUSE it
// has been partitioned — and therefore missing writes — take leadership and lose
// committed money. Real systems (etcd's leadership priority, TiKV's leader
// weights) bias exactly this way and for exactly this reason.

// NodeHealth is a node's operational condition, on three levels.
//
// Named NodeHealth rather than Health because raft.Health already exists: that
// one is the observability snapshot (health.go, G5) reporting whether a server
// can commit. This is a separate, simulated signal about the MACHINE, and
// conflating the two names would make it unclear which a caller meant.
//
// Deliberately coarse. A continuous score would imply a precision this does not
// have — it is a simulated signal for the demo, not a measurement.
type NodeHealth int32

const (
	// HealthNormal is the default: no reason to prefer or avoid this node.
	HealthNormal NodeHealth = iota

	// HealthLow marks a struggling node: it should lead only if nothing better is
	// available, never merely because its timer fired first.
	HealthLow

	// HealthHigh marks a node in good shape and the preferred leader.
	HealthHigh
)

func (h NodeHealth) String() string {
	switch h {
	case HealthLow:
		return "low"
	case HealthHigh:
		return "high"
	default:
		return "normal"
	}
}

// electionBias returns a multiplier applied to the randomized election delay.
//
// Lower campaigns sooner. This SCALES the draw rather than confining it to a
// slice of the window, and the difference matters: an earlier version gave each
// health level its own narrow band (normal drew from 35-80% of the window),
// which cut the randomized spread from 100ms to 46ms and raised the minimum
// timeout. Both are §5.2 violations — a narrower spread makes split votes
// likelier, and a raised floor makes every failover slower. It broke five
// election tests.
//
// Scaling keeps the FULL randomized range for a normal node, so an unmodified
// cluster behaves exactly as before, while a healthy node's draw is compressed
// toward zero and an unhealthy one's is stretched. Health then shifts the
// expected ordering without narrowing the randomness that prevents split votes.
func (h NodeHealth) electionBias() float64 {
	switch h {
	case HealthHigh:
		return 0.55 // campaigns sooner
	case HealthLow:
		return 1.45 // hangs back, but still campaigns
	default:
		return 1.0 // unchanged: a cluster with no health signal behaves as before
	}
}

// SetNodeHealth sets this server's health, which biases how eagerly it campaigns.
//
// Safe to call at any time from any goroutine: it is read when the election timer
// is reset, and reading a slightly stale value only shifts a timeout by
// milliseconds.
func (s *Server) SetNodeHealth(h NodeHealth) { s.health.Store(int32(h)) }

// NodeHealth returns this server's current simulated health.
func (s *Server) NodeHealth() NodeHealth { return NodeHealth(s.health.Load()) }

// healthAtomic is the storage for a server's health level.
//
// An atomic rather than mutex-guarded state because the election timer reads it
// on every tick, and taking the server lock there would put the health signal in
// contention with the consensus loop — the observer effect this project has
// already measured twice (§20).
type healthAtomic = atomic.Int32
