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
	cfg := simConfig()

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
	}

	for i, id := range ids {
		sm := &CountingSM{}
		s := raft.NewServerWith(id, ids, sm, net, cfg, seed+int64(i)*7919)
		s.SetStorage(storage.OpenRaftState(filepath.Join(dir, string(id)+".wal"), nil))

		net.Register(id, s)
		c.Nodes[id] = s
		c.SMs[id] = sm
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

// CloseStorage is retained for symmetry with the restart flow. Nothing is held
// open: state files are replaced atomically by rename and read by path, so a
// "restarted" cluster can reopen the same directory immediately.
func (c *Cluster) CloseStorage() {}
