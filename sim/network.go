// Package sim provides a deterministic simulated network and the chaos harness
// used to test Raft under failure.
//
// The methodology is chaos engineering (Netflix's Chaos Monkey / Simian Army) —
// see learn/READING_LIST.md §5. Four of the five canonical principles apply here;
// the fifth ("run experiments in production") is deliberately not adopted, since
// injecting faults into a real ledger is not defensible. The simulated network is
// the blast radius by construction.
//
// FAULT SELECTION is driven by per-link seeded PRNGs, so which RPCs are dropped
// or duplicated on a given link is a function of the seed alone.
//
// FULL RUN REPRODUCIBILITY IS NOT ACHIEVED, and claiming otherwise would be
// dishonest. Election timeouts fire on wall-clock time, so the NUMBER and
// ORDERING of RPCs still varies between runs of the same seed — measured at
// roughly 2% variance in total sends. A failing chaos run is therefore usually,
// but not always, replayable from its seed.
//
// Closing the gap requires driving the whole simulation on logical time (a
// discrete-event scheduler where raft's timers are virtual), which is a
// substantial redesign of both this package and raft's role loop. It is recorded
// as outstanding in context/DESIGN.md rather than papered over.
//
// An earlier version used ONE shared PRNG, which was materially worse: goroutines
// reached it in scheduler-dependent order and each call consumed one or two draws,
// so even the fault pattern differed between runs. That much is now fixed.
package sim

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

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

	// latencyMin/latencyMax bound the simulated one-way delay per RPC.
	//
	// Zero latency (the previous behaviour) hid an entire class of bug: RPCs were
	// delivered synchronously on the caller's goroutine, so head-of-line blocking,
	// shared-connection aborts, and an RPC timeout set ABOVE the election timeout
	// could not manifest. The simulator validated consensus logic while providing
	// no evidence at all about timing behaviour, which is where the production
	// risk actually lives.
	latencyMin time.Duration
	latencyMax time.Duration

	// links holds one PRNG per (from,to) pair, each seeded deterministically from
	// the base seed.
	//
	// A single shared PRNG made seeded runs NON-reproducible, which defeats the
	// entire point of seeding: goroutines reach the mutex in scheduler-dependent
	// order, and each call consumes one or two draws depending on whether the drop
	// check fired, so the stream interleaved differently every run. Measured: the
	// same seed produced 200 vs 204 sends across runs.
	//
	// Per-link streams make each link's sequence a function of the seed alone.
	links map[string]*rand.Rand
	seed  int64

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
		links:      make(map[string]*rand.Rand),
		seed:       seed,
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
func (n *Network) deliverable(from, to raft.NodeID) (*raft.Server, bool, bool, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.sent[from]++

	// A crashed node neither sends nor receives.
	if n.crashed[from] || n.crashed[to] {
		n.dropped[from]++
		return nil, false, false, 0
	}
	// Partitioned nodes cannot reach each other.
	if n.partitions[from] != n.partitions[to] {
		n.dropped[from]++
		return nil, false, false, 0
	}
	// Random loss, drawn from this link's own stream so the result does not
	// depend on how goroutines interleaved.
	rnd := n.linkRand(from, to)
	if n.dropRate > 0 && rnd.Float64() < n.dropRate {
		n.dropped[from]++
		return nil, false, false, 0
	}

	target, ok := n.nodes[to]
	if !ok {
		return nil, false, false, 0
	}

	dup := n.duplicateRate > 0 && rnd.Float64() < n.duplicateRate

	// Draw the delay from the same link stream, before releasing the lock.
	var delay time.Duration
	if n.latencyMax > 0 {
		span := int64(n.latencyMax - n.latencyMin)
		delay = n.latencyMin
		if span > 0 {
			delay += time.Duration(rnd.Int63n(span))
		}
	}
	return target, true, dup, delay
}

// SetLatency sets the simulated one-way delay range applied to every RPC.
//
// Zero (the default) preserves the original instant delivery, which keeps the
// existing fast tests fast. Set a real range to exercise timing behaviour.
func (n *Network) SetLatency(minD, maxD time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latencyMin, n.latencyMax = minD, maxD
}

// SendAppendEntries implements raft.Transport.
func (n *Network) SendAppendEntries(to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	target, ok, dup, delay := n.deliverable(args.LeaderID, to)
	if !ok {
		return raft.AppendEntriesReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay) // outbound flight time
	}

	reply := target.AppendEntries(args)
	if delay > 0 {
		time.Sleep(delay) // return flight time
	}
	if dup {
		// Deliver a second time. The receiver rules must make this a no-op —
		// this is the duplicate-delivery flow RULES.md rule 3 requires.
		target.AppendEntries(args)
	}
	return reply, nil
}

// SendRequestVote implements raft.Transport.
func (n *Network) SendRequestVote(to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	target, ok, dup, delay := n.deliverable(args.CandidateID, to)
	if !ok {
		return raft.RequestVoteReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	reply := target.RequestVote(args)
	if delay > 0 {
		time.Sleep(delay)
	}
	if dup {
		target.RequestVote(args)
	}
	return reply, nil
}

// --- Chaos controls -------------------------------------------------------

// linkRand returns the PRNG for one directed link, creating it on first use.
// Caller must hold n.mu.
//
// The per-link seed is derived from the base seed and the endpoint names via
// FNV-1a, so it is stable across runs and independent of map iteration order.
func (n *Network) linkRand(from, to raft.NodeID) *rand.Rand {
	key := string(from) + "->" + string(to)
	if r, ok := n.links[key]; ok {
		return r
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	r := rand.New(rand.NewSource(n.seed ^ int64(h.Sum64())))
	n.links[key] = r
	return r
}

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
