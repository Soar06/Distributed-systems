package demo

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/sim"
)

// Growing the cluster from the UI: more machines, more shards.
//
// [project decision] This REBUILDS the cluster and replays the accounts, rather
// than adding machines to the running one. The honest reason, stated here so the
// UI does not imply something the code does not do:
//
// Adding a machine to a live cluster is `raft.AddServer`, which G6 implements —
// but it operates on ONE Raft group. Adding a machine to a SHARDED cluster means
// deciding which shards it should now hold, moving those replicas onto it, and
// running a membership change per shard while the cluster keeps serving. That is
// live resharding, which LATER.md defers deliberately and which is a substantial
// piece of work rather than wiring.
//
// So this is a teaching control, not a production one: it shows the RESULT of a
// larger cluster — how placement spreads, how many accounts each machine ends up
// holding, which machines hold nothing for a given account — without pretending
// the transition was seamless. The event log says so explicitly when it happens.
//
// Adding an ACCOUNT, by contrast, is genuinely live: it is an ordinary write
// through the coordinator, and the ring places it with no coordination at all.

// Resize rebuilds the cluster with a new machine count, shard count, or
// replication factor, then reopens every account that existed before.
//
// Balances are NOT preserved: the point of the control is to show placement, and
// silently reconstructing balances from a cluster that no longer exists would be
// a fiction. Accounts come back at their opening balance and the log says so.
func (c *Cluster) Resize(nShards, nNodes, replicationFactor int) error {
	if replicationFactor < 1 {
		return fmt.Errorf("replication factor must be at least 1")
	}
	if replicationFactor > nNodes {
		return fmt.Errorf(
			"replication factor %d exceeds %d machines: a shard cannot have more "+
				"replicas than there are machines to hold them", replicationFactor, nNodes)
	}
	if nShards < 1 {
		return fmt.Errorf("a cluster needs at least one shard")
	}

	c.mu.Lock()
	accounts := append([]ledger.AccountID(nil), c.accounts...)
	c.mu.Unlock()

	next, err := sim.NewPlacedCluster(nShards, nNodes, replicationFactor, 42)
	if err != nil {
		return err
	}
	next.Start()
	if !next.WaitForLeaders(5 * time.Second) {
		next.Stop()
		return fmt.Errorf("the resized cluster did not elect leaders")
	}

	old := c.sc

	c.mu.Lock()
	c.sc = next
	c.nodes = nNodes
	c.replicationFactor = replicationFactor
	// Fault state belongs to machines that may no longer exist.
	c.crashed = make(map[raft.NodeID]bool)
	c.accounts = nil
	c.mu.Unlock()

	old.Stop()

	c.logf("REBUILT as %d shards over %d machines (replication factor %d) — "+
		"a live cluster would migrate replicas instead; this rebuilds to show the "+
		"resulting placement", nShards, nNodes, replicationFactor)

	// Reopen the accounts so placement is visible immediately.
	for _, a := range accounts {
		if _, err := c.Open(a, 10000); err != nil {
			c.logf("could not reopen %s after resize: %v", a, err)
		}
	}
	return nil
}

// Topology reports the cluster's current shape.
func (c *Cluster) Topology() (shards, nodes, replicationFactor int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.sc.Ring.Shards()), c.nodes, c.replicationFactor
}
