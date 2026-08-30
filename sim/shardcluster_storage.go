package sim

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/storage"
)

// Storage-backed shard clusters, for crash-and-restart testing of 2PC.
//
// The 2PC state a participant holds — its YES vote and the funds it reserved — is
// derived by applying the shard's Raft log, exactly like balances. That makes it
// durable only if the shard's Raft group actually persists its log AND the node
// replays it on restart. NewShardCluster does neither: it builds every group with
// no storage at all, so a restarted participant came back with an empty ledger
// and no memory of its promise.
//
// A prepared participant forgetting its vote is the one thing 2PC may never do:
// having voted yes it has made an unretractable promise, and the funds it holds
// must stay held until the transaction resolves. DESIGN.md §11 states the
// requirement — "Nothing about the 2PC state lives in memory" — and this file is
// what lets a test hold the implementation to it.

// NewShardClusterWithStorage builds nShards groups of nPerShard nodes each, whose
// nodes persist to WAL files under dir. Reusing the same dir in a later call
// simulates every node in the cluster dying and coming back.
//
// Each node gets its own <node>.wal and <node>.applied, mirroring node/main.go:
// nodes share nothing, not even in a test harness.
func NewShardClusterWithStorage(t *testing.T, nShards, nPerShard int, seed int64, dir string) *ShardCluster {
	t.Helper()

	cfg := simConfig()

	var shardIDs []shard.ID
	for i := range nShards {
		shardIDs = append(shardIDs, shard.ID(fmt.Sprintf("shard-%d", i)))
	}
	ring := shard.NewRing(shardIDs, shard.DefaultVNodes)

	sc := &ShardCluster{
		Ring:   ring,
		Groups: make(map[shard.ID]*ShardGroup),
		Nets:   make(map[shard.ID]*Network),
	}

	groups := make(map[shard.ID]shard.Group)
	for si, sid := range shardIDs {
		net := NewNetwork(seed + int64(si)*104729)

		var ids []raft.NodeID
		for j := range nPerShard {
			ids = append(ids, raft.NodeID(fmt.Sprintf("%s-n%d", sid, j+1)))
		}

		g := &ShardGroup{
			ID: sid, IDs: ids,
			Nodes: make(map[raft.NodeID]*raft.Server),
			SMs:   make(map[raft.NodeID]*shard.Machine),
			Net:   net,
		}

		for j, nid := range ids {
			machine := shard.NewMachine(sid, ledger.New())
			srv := raft.NewServerWith(nid, ids, machine, net, cfg,
				seed+int64(si)*7919+int64(j)*31)

			// The applied marker is what makes replay possible: without it Restore
			// loads the log but replays nothing, and the state machine — balances,
			// reservations, and 2PC promises alike — comes back empty.
			applied, err := storage.OpenApplied(filepath.Join(dir, string(nid)+".applied"))
			if err != nil {
				t.Fatalf("open applied file for %s: %v", nid, err)
			}
			t.Cleanup(func() { applied.Close() })
			srv.SetStorage(storage.OpenRaftState(filepath.Join(dir, string(nid)+".wal"), applied))

			net.Register(nid, srv)
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

// RestoreAll loads persisted state into every node of every shard, replaying each
// node's log into its state machine. Call before Start when simulating a restart.
func (sc *ShardCluster) RestoreAll() error {
	for sid, g := range sc.Groups {
		for _, id := range g.IDs {
			if err := g.Nodes[id].Restore(); err != nil {
				return fmt.Errorf("restore %s/%s: %w", sid, id, err)
			}
		}
	}
	return nil
}

// startRestored starts a cluster that has already been restored, and waits for
// every shard to elect a leader again.
func (sc *ShardCluster) startRestored(t *testing.T, timeout time.Duration) {
	t.Helper()
	sc.Start()
	t.Cleanup(sc.Stop)
	if !sc.WaitForLeaders(timeout) {
		t.Fatalf("not every shard re-elected a leader after restart%s", sc.View())
	}
}
