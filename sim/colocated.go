package sim

import (
	"fmt"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// A CO-LOCATED cluster: N machines, each hosting a replica of EVERY shard.
//
// NewShardCluster gives each shard its own three nodes and its own network, so a
// 3-shard cluster is 9 nodes and killing one affects exactly one shard. That is
// easier to reason about, but it is not what a real deployment looks like and it
// makes the two dimensions — machines and shards — appear locked together.
//
// This is the topology `cmd/shardnode -hosts shard-0,shard-1,shard-2` runs, and
// the one CockroachDB uses for Ranges: a machine carries many groups, so losing
// it costs one replica of every shard AT ONCE. Same aggregate fault tolerance,
// very different blast radius per physical failure.
//
// It is also what makes "which machines hold this account?" an interesting
// question rather than a trivial one: with co-location, an account's three
// replicas live on three machines that also hold slices of everything else.

// NewColocatedCluster builds nNodes machines, each hosting a replica of every one
// of nShards shards.
//
// Node ids are plain machine names (node-1, node-2, ...) rather than
// shard-scoped ones, because a machine is not owned by a shard here — it hosts
// all of them.
func NewColocatedCluster(nShards, nNodes int, seed int64) *ShardCluster {
	cfg := simConfig()

	var shardIDs []shard.ID
	for i := range nShards {
		shardIDs = append(shardIDs, shard.ID(fmt.Sprintf("shard-%d", i)))
	}
	ring := shard.NewRing(shardIDs, shard.DefaultVNodes)

	// Machine names are shared across every shard: node-1 hosts a replica of
	// shard-0, shard-1 and shard-2, exactly as one process would.
	var nodeIDs []raft.NodeID
	for i := range nNodes {
		nodeIDs = append(nodeIDs, raft.NodeID(fmt.Sprintf("node-%d", i+1)))
	}

	// ONE network for the whole cluster, not one per shard.
	//
	// This is the point of the topology: a machine is reachable or not, and when
	// it goes down every group it hosts loses that replica together. Separate
	// networks per shard would make a machine independently reachable for shard-0
	// and unreachable for shard-1, which no physical failure does.
	net := NewNetwork(seed)

	sc := &ShardCluster{
		Ring:   ring,
		Groups: make(map[shard.ID]*ShardGroup),
		Nets:   make(map[shard.ID]*Network),
	}

	groups := make(map[shard.ID]shard.Group)
	for si, sid := range shardIDs {
		g := &ShardGroup{
			ID:    sid,
			IDs:   append([]raft.NodeID(nil), nodeIDs...),
			Nodes: make(map[raft.NodeID]*raft.Server),
			SMs:   make(map[raft.NodeID]*shard.Machine),
			Net:   net,
		}

		for j, nid := range nodeIDs {
			// Still one ledger and one state machine per (node, shard): co-location
			// shares the machine, never the data. Two shards on one node have
			// separate logs and separate ledgers, exactly as two processes would.
			machine := shard.NewMachine(sid, ledger.New())
			srv := raft.NewServerWith(nid, nodeIDs, machine,
				net.ForGroup(sid), cfg, seed+int64(si)*7919+int64(j)*31)

			net.RegisterGroup(sid, nid, srv)
			g.Nodes[nid] = srv
			g.SMs[nid] = machine
		}

		sc.Groups[sid] = g
		sc.Nets[sid] = net
		groups[sid] = g
	}

	sc.Coordinator = shard.NewCoordinator(ring, groups)
	return sc
}
