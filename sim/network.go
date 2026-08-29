// Package sim provides a deterministic simulated network and the chaos harness
// used to test Raft under failure.
//
// The methodology is chaos engineering (Netflix's Chaos Monkey / Simian Army) —
// see learn/READING_LIST.md §5. Four of the five canonical principles apply here;
// the fifth ("run experiments in production") is deliberately not adopted, since
// injecting faults into a real ledger is not defensible. The simulated network is
// the blast radius by construction.
//
// Every fault is driven by a seeded PRNG so a failing run is reproducible. An
// unreproducible consensus bug is close to impossible to fix.
package sim

import (
	"math/rand"
	"sync"

	"github.com/homura/core-bank/raft"
)

// Network is an in-memory raft.Transport that can inject faults.
//
// It implements the same interface a real gRPC transport will, so the Raft code
// cannot tell the difference — chaos testing requires no changes to consensus
// logic.
type Network struct {
	mu sync.Mutex

	nodes map[raft.NodeID]*raft.Server

	// crashed nodes accept no RPCs and send none: a stopped process.
	crashed map[raft.NodeID]bool

	// partitions assigns each node a partition id. Nodes in different partitions
	// cannot communicate. Same id == mutually reachable.
	partitions map[raft.NodeID]int

	// dropRate is the probability [0,1] that any given RPC is silently lost.
	dropRate float64

	// duplicateRate is the probability that a delivered RPC is delivered twice.
	// Real networks duplicate packets, and Raft must tolerate it.
	duplicateRate float64

	rnd *rand.Rand

	// counters for assertions and for the cluster view.
	sent    map[raft.NodeID]int
	dropped map[raft.NodeID]int
}

// NewNetwork creates an empty network with the given seed. All nodes start in
// partition 0 (fully connected) and healthy.
func NewNetwork(seed int64) *Network {
	return &Network{
		nodes:      make(map[raft.NodeID]*raft.Server),
		crashed:    make(map[raft.NodeID]bool),
		partitions: make(map[raft.NodeID]int),
		rnd:        rand.New(rand.NewSource(seed)),
		sent:       make(map[raft.NodeID]int),
		dropped:    make(map[raft.NodeID]int),
	}
}

// Register adds a server to the network.
func (n *Network) Register(id raft.NodeID, s *raft.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = s
	n.partitions[id] = 0
}

// deliverable reports whether an RPC from -> to should be delivered, and
// consumes randomness for the drop decision. Caller must not hold n.mu.
func (n *Network) deliverable(from, to raft.NodeID) (*raft.Server, bool, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.sent[from]++

	// A crashed node neither sends nor receives.
	if n.crashed[from] || n.crashed[to] {
		n.dropped[from]++
		return nil, false, false
	}
	// Partitioned nodes cannot reach each other.
	if n.partitions[from] != n.partitions[to] {
		n.dropped[from]++
		return nil, false, false
	}
	// Random loss.
	if n.dropRate > 0 && n.rnd.Float64() < n.dropRate {
		n.dropped[from]++
		return nil, false, false
	}

	target, ok := n.nodes[to]
	if !ok {
		return nil, false, false
	}

	dup := n.duplicateRate > 0 && n.rnd.Float64() < n.duplicateRate
	return target, true, dup
}

// SendAppendEntries implements raft.Transport.
func (n *Network) SendAppendEntries(to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	target, ok, dup := n.deliverable(args.LeaderID, to)
	if !ok {
		return raft.AppendEntriesReply{}, raft.ErrUnreachable
	}

	reply := target.AppendEntries(args)
	if dup {
		// Deliver a second time. The receiver rules must make this a no-op —
		// this is the duplicate-delivery flow RULES.md rule 3 requires.
		target.AppendEntries(args)
	}
	return reply, nil
}

// SendRequestVote implements raft.Transport.
func (n *Network) SendRequestVote(to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	target, ok, dup := n.deliverable(args.CandidateID, to)
	if !ok {
		return raft.RequestVoteReply{}, raft.ErrUnreachable
	}

	reply := target.RequestVote(args)
	if dup {
		target.RequestVote(args)
	}
	return reply, nil
}

// --- Chaos controls -------------------------------------------------------

// Crash stops a node from sending or receiving. Models a process death or a
// machine going away — Chaos Monkey's original fault.
func (n *Network) Crash(id raft.NodeID) {
	n.mu.Lock()
	n.crashed[id] = true
	n.mu.Unlock()
}

// Restore brings a crashed node back onto the network.
func (n *Network) Restore(id raft.NodeID) {
	n.mu.Lock()
	n.crashed[id] = false
	n.partitions[id] = 0
	n.mu.Unlock()
}

// Partition splits the cluster into groups that cannot talk to each other.
// Each argument is one side of the split. Models a network partition — the
// condition CAP is about, and Chaos Gorilla's zone-loss scenario.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for gi, g := range groups {
		for _, id := range g {
			n.partitions[id] = gi
		}
	}
}

// Heal removes all partitions, returning every node to one group.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.partitions {
		n.partitions[id] = 0
	}
}

// SetDropRate sets the probability that any RPC is silently lost.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	n.dropRate = p
	n.mu.Unlock()
}

// SetDuplicateRate sets the probability that a delivered RPC is delivered twice.
func (n *Network) SetDuplicateRate(p float64) {
	n.mu.Lock()
	n.duplicateRate = p
	n.mu.Unlock()
}

// Stats returns per-node sent and dropped RPC counts.
func (n *Network) Stats() (sent, dropped map[raft.NodeID]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	sent, dropped = make(map[raft.NodeID]int), make(map[raft.NodeID]int)
	for k, v := range n.sent {
		sent[k] = v
	}
	for k, v := range n.dropped {
		dropped[k] = v
	}
	return sent, dropped
}
