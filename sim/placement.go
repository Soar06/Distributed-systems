package sim

import (
	"fmt"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Replica placement across a cluster larger than the replication factor.
//
// NewColocatedCluster puts every shard on every machine, which is only correct
// while the machine count equals the replication factor. Grow the cluster to 9
// machines and that would give each shard 9 replicas — a quorum of 5 instead of
// 2, so writes get slower and less available as you add hardware, which is the
// opposite of what adding hardware is for.
//
// Real systems keep the replication factor FIXED (3 is the usual choice) and
// spread shards across whatever machines exist. That is what makes the two
// dimensions independent:
//
//   - replication factor decides how many failures ONE shard survives
//   - machine count decides how much total capacity the cluster has
//
// The consequence worth seeing: with 9 machines and RF=3, most machines hold only
// SOME shards, and therefore only some accounts. A machine holding nothing for
// alice can burn without alice noticing. With RF == machine count, every failure
// touches everything.

// NewPlacedCluster builds nNodes machines and places each shard on exactly
// replicationFactor of them.
//
// Placement walks the machine list with a per-shard offset, so shards are spread
// evenly rather than piling onto the first few machines. It is deterministic:
// the same inputs always produce the same layout, which matters because every
// node must agree on who holds what without coordinating.
func NewPlacedCluster(nShards, nNodes, replicationFactor int, seed int64) (*ShardCluster, error) {
	if replicationFactor < 1 {
		return nil, fmt.Errorf("sim: replication factor must be at least 1")
	}
	if replicationFactor > nNodes {
		return nil, fmt.Errorf(
			"sim: replication factor %d exceeds %d machines; a shard cannot have more "+
				"replicas than there are machines to put them on",
			replicationFactor, nNodes)
	}
	// An even replication factor is legal but wasteful: RF=4 tolerates one failure
	// just like RF=3 (a majority of 4 is 3, so losing 2 breaks quorum either way),
	// while costing an extra copy of every write.
	cfg := simConfig()

	var shardIDs []shard.ID
	for i := range nShards {
		shardIDs = append(shardIDs, shard.ID(fmt.Sprintf("shard-%d", i)))
	}
	ring := shard.NewRing(shardIDs, shard.DefaultVNodes)

	var nodeIDs []raft.NodeID
	for i := range nNodes {
		nodeIDs = append(nodeIDs, raft.NodeID(fmt.Sprintf("node-%d", i+1)))
	}

	// One network for the whole cluster: a machine is reachable or it is not, and
	// killing it must take down every replica it happens to hold.
	net := NewNetwork(seed)

	sc := &ShardCluster{
		Ring:   ring,
		Groups: make(map[shard.ID]*ShardGroup),
		Nets:   make(map[shard.ID]*Network),
	}

	groups := make(map[shard.ID]shard.Group)
	for si, sid := range shardIDs {
		// This shard's replicas: replicationFactor machines starting at a STAGGERED
		// offset.
		//
		// Advancing by one machine per shard rather than by replicationFactor is
		// what stops the cluster splitting into rigid blocks. With 9 machines, RF=3
		// and a stride of 3, machines 1-3 would hold an identical set of shards, so
		// losing all three loses everything they held and no other machine could
		// help — the failure domains would be as coarse as if there were only three
		// machines. Overlapping the groups spreads each machine's exposure across
		// different neighbours instead.
		holders := make([]raft.NodeID, 0, replicationFactor)
		for k := range replicationFactor {
			holders = append(holders, nodeIDs[(si+k)%nNodes])
		}

		g := &ShardGroup{
			ID:    sid,
			IDs:   holders,
			Nodes: make(map[raft.NodeID]*raft.Server),
			SMs:   make(map[raft.NodeID]*shard.Machine),
			Net:   net,
		}

		for j, nid := range holders {
			// One ledger and one state machine per (machine, shard). Co-location
			// shares the machine, never the data.
			machine := shard.NewMachine(sid, ledger.New())
			// The peer set is THIS SHARD'S holders, not every machine in the cluster.
			// That is what keeps the quorum at 2-of-3 no matter how large the cluster
			// grows, and it is the whole point of a fixed replication factor.
			srv := raft.NewServerWith(nid, holders, machine,
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
	return sc, nil
}
