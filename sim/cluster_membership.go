package sim

import (
	"time"

	"github.com/homura/core-bank/raft"
)

// WaitForLeaderAmong waits for a leader drawn from a specific set of nodes.
//
// Needed for membership tests, where the cluster's configuration is deliberately
// not the full set of registered servers: a node outside the configuration may
// still be running, and WaitForLeader would happily return it.
func (c *Cluster) WaitForLeaderAmong(candidates []raft.NodeID, timeout time.Duration) (raft.NodeID, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range candidates {
			if c.Nodes[id].Role() == raft.Leader {
				return id, true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", false
}
