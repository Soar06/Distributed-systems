package sim

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/storage"
)

// Storage-backed cluster construction, for crash-and-restart testing.
//
// Nodes here keep their state in real files, so "restart" means building fresh
// Server objects over the same WALs — the same thing that happens when a process
// dies and comes back.

// NewClusterWithStorage builds an n-node cluster whose nodes persist to WAL files
// under dir. Reusing the same dir in a later call simulates a restart.
func NewClusterWithStorage(t *testing.T, n int, seed int64, dir string) *Cluster {
	t.Helper()

	net := NewNetwork(seed)
	cfg := raft.Config{
		ElectionTimeoutMin: 60,
		ElectionTimeoutMax: 120,
		HeartbeatInterval:  15,
	}

	ids := make([]raft.NodeID, n)
	for i := range n {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}

	c := &Cluster{
		Net:     net,
		IDs:     ids,
		Nodes:   make(map[raft.NodeID]*raft.Server),
		SMs:     make(map[raft.NodeID]*CountingSM),
		history: make(map[raft.NodeID][]string),
		wals:    make(map[raft.NodeID]*storage.WAL),
	}

	for i, id := range ids {
		wal, err := storage.Open(filepath.Join(dir, string(id)+".wal"))
		if err != nil {
			t.Fatalf("open wal for %s: %v", id, err)
		}
		sm := &CountingSM{}
		s := raft.NewServerWith(id, ids, sm, net, cfg, seed+int64(i)*7919)
		s.SetStorage(storage.NewRaftState(wal))

		net.Register(id, s)
		c.Nodes[id] = s
		c.SMs[id] = sm
		c.wals[id] = wal
	}
	return c
}

// RestoreAll loads persisted state into every node. Call before Start when
// simulating a restart.
func (c *Cluster) RestoreAll() error {
	for _, id := range c.IDs {
		if err := c.Nodes[id].Restore(); err != nil {
			return fmt.Errorf("restore %s: %w", id, err)
		}
	}
	return nil
}

// CloseStorage closes every node's WAL, releasing the file handles so the same
// directory can be reopened by a "restarted" cluster.
func (c *Cluster) CloseStorage() {
	for _, w := range c.wals {
		if w != nil {
			w.Close()
		}
	}
}
