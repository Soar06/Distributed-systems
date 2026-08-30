package sim

import "github.com/homura/core-bank/raft"

// Election timings for the simulator (G5 follow-up).
//
// These are sized for the TEST ENVIRONMENT, not for a LAN and not for realism.
//
// The original 60-120ms was tight enough that a full parallel run under `-race`
// lost elections to SCHEDULING delay rather than to any injected failure:
// goroutines did not get a core within the timeout, followers timed out on a
// perfectly healthy leader, and tests that submit a write and then assert it
// committed failed intermittently with "leadership lost", "not leader", or "no
// leader". Measured at roughly one failure per three full-suite runs, on
// different tests each time — the signature of a timing artifact rather than a
// defect.
//
// That is §5.2's inequality being violated by the machine:
//
//	broadcastTime << electionTimeout << MTBF
//
// The race detector instruments every memory access, which inflates
// broadcastTime until it is no longer << electionTimeout. The same effect was
// measured directly in the sharded throughput benchmark, where widening these
// timings took lost writes from 26-of-240 to zero.
//
// Widened here so the inequality holds even under instrumentation. Chaos tests
// that deliberately violate the inequality set their own timings and are
// unaffected — the point of those tests is that timing costs liveness, never
// safety, and they still make it.

// simConfig is the standard timing for simulator clusters.
func simConfig() raft.Config {
	return raft.Config{
		ElectionTimeoutMin: 250,
		ElectionTimeoutMax: 500,
		HeartbeatInterval:  40,
	}
}
