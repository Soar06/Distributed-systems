package sim

import (
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Submitting through leadership changes, for tests that care about the RESULT
// rather than about which node served it.
//
// A test that finds a leader, then submits to it, is racing an election: Raft may
// legitimately elect a new leader between the two, and under a full parallel
// `-race` run that happens often, because the simulator's 60-120ms election
// timers are tight enough that scheduling delay alone triggers a timeout. That is
// the same §5.2 timing-violated-by-the-machine effect the sharded throughput
// benchmark measured, and it is a property of the test environment rather than a
// defect in the code under test.
//
// The right response is what the client contract already prescribes for exactly
// this case: on NotLeader, retry at the current leader. A test that instead fails
// is asserting "no election happened during this window", which is not a property
// Raft offers.

// SubmitWithRetry submits a command, retrying at whoever leads until it is
// accepted or the timeout expires. Returns the accepting leader.
func (c *Cluster) SubmitWithRetry(t *testing.T, cmd []byte, timeout time.Duration) (raft.NodeID, bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, ok := c.WaitForLeader(timeout)
		if !ok {
			return "", false
		}
		if _, _, accepted := c.Nodes[leader].Submit(cmd); accepted {
			return leader, true
		}
		// Leadership moved between finding it and submitting. Retry: this is the
		// §8 redirect, and it is why a client carries an idempotency key.
		time.Sleep(10 * time.Millisecond)
	}
	return "", false
}
