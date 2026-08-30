package sim

import (
	"fmt"
	"strings"

	"github.com/homura/core-bank/raft"
)

// The cluster view: a snapshot of what every node currently believes.
//
// NOW.md's Phase 4 dashboard needs exactly this — per-node Raft role, term, the
// log each node holds, and how far each has committed and applied. Producing it
// here, in the shape the dashboard will consume, means wiring the real UI later
// is a rendering job rather than a redesign.
//
// It is also what makes a chaos test *legible*: when a run fails, the printed
// view shows which node diverged, rather than only that an assertion tripped.

// NodeView is one node's observable state.
type NodeView struct {
	ID          raft.NodeID
	Role        raft.Role
	Term        raft.Term
	CommitIndex raft.Index
	LastApplied raft.Index
	LogLength   int
	Log         []raft.LogEntry
	Applied     []string

	// Crashed and Partition come from the network, not the node: a node cannot
	// know it has been isolated, which is precisely why partitions are dangerous.
	Crashed   bool
	Partition int
}

// ClusterView is a snapshot of the whole cluster at one moment.
type ClusterView struct {
	Nodes []NodeView
}

// View captures the current state of every node.
func (c *Cluster) View() ClusterView {
	c.Net.mu.Lock()
	crashed := make(map[raft.NodeID]bool, len(c.Net.crashed))
	parts := make(map[raft.NodeID]int, len(c.Net.partitions))
	for k, v := range c.Net.crashed {
		crashed[k] = v
	}
	for k, v := range c.Net.partitions {
		parts[k] = v
	}
	c.Net.mu.Unlock()

	cv := ClusterView{}
	for _, id := range c.IDs {
		s := c.Nodes[id]
		log := s.LogEntries()
		cv.Nodes = append(cv.Nodes, NodeView{
			ID:          id,
			Role:        s.Role(),
			Term:        s.CurrentTerm(),
			CommitIndex: s.CommitIndex(),
			LastApplied: s.LastApplied(),
			LogLength:   len(log) - 1, // exclude the sentinel
			Log:         log,
			Applied:     c.SMs[id].AppliedCopy(),
			Crashed:     crashed[id],
			Partition:   parts[id],
		})
	}
	return cv
}

// String renders the view as a table. Used in test failure output so a failing
// chaos run explains itself.
func (v ClusterView) String() string {
	var b strings.Builder
	b.WriteString("\n  NODE   ROLE       TERM  COMMIT  APPLIED  LOG  STATUS\n")
	b.WriteString("  ----   ----       ----  ------  -------  ---  ------\n")
	for _, n := range v.Nodes {
		status := "ok"
		if n.Crashed {
			status = "CRASHED"
		} else if n.Partition != 0 {
			status = fmt.Sprintf("partition %d", n.Partition)
		}
		b.WriteString(fmt.Sprintf("  %-6s %-10s %-5d %-7d %-8d %-4d %s\n",
			n.ID, n.Role, n.Term, n.CommitIndex, n.LastApplied, n.LogLength, status))
	}
	return b.String()
}

// LogsString renders each node's log entries side by side, for diagnosing a Log
// Matching failure.
func (v ClusterView) LogsString() string {
	var b strings.Builder
	b.WriteString("\n  logs (index:term:command)\n")
	for _, n := range v.Nodes {
		b.WriteString(fmt.Sprintf("  %-6s ", n.ID))
		for _, e := range n.Log[1:] { // skip sentinel
			b.WriteString(fmt.Sprintf("[%d:%d:%s] ", e.Index, e.Term, e.Command))
		}
		b.WriteString("\n")
	}
	return b.String()
}
