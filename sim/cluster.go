package sim

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/homura/core-bank/raft"
)

// Cluster is a set of Raft servers wired to a simulated Network, plus the
// assertions that check Figure 3's safety properties.
//
// Chaos engineering's first principle is "build a hypothesis around steady state
// behavior" — for a Raft cluster, the steady state IS Figure 3. The assertions
// below are that hypothesis, stated executably.
type Cluster struct {
	Net   *Network
	IDs   []raft.NodeID
	Nodes map[raft.NodeID]*raft.Server
	SMs   map[raft.NodeID]*CountingSM

	// history records, per node, the commands applied at each index. It is what
	// makes State Machine Safety checkable: the property is about what nodes
	// have *already applied*, so it cannot be verified from current state alone.
	history map[raft.NodeID][]string
}

// CountingSM is a trivial deterministic state machine that records what it
// applied, in order.
type CountingSM struct {
	mu      sync.Mutex
	Applied []string
}

// Apply implements raft.StateMachine.
func (c *CountingSM) Apply(cmd []byte) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Applied = append(c.Applied, string(cmd))
	return len(c.Applied)
}

// Snapshot returns a copy of what has been applied.
//
// NOTE the name collision this deliberately does not have: raft.Snapshotter
// requires Snapshot() ([]byte, error), which this signature would shadow. The
// serialized form is TakeSnapshot/RestoreSnapshot below, so CountingSM can
// satisfy raft.Snapshotter while tests keep the convenient []string view.
func (c *CountingSM) AppliedCopy() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.Applied))
	copy(out, c.Applied)
	return out
}

// NewCluster builds an n-node cluster on a seeded network with fast timings so
// tests run quickly. The timings still respect the §5.2 inequality
// (broadcastTime << electionTimeout).
func NewCluster(n int, seed int64) *Cluster {
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
		// Each server gets a distinct seed so election timeouts are independent —
		// identical seeds would make every node time out in lockstep and split
		// the vote forever.
		s := raft.NewServerWith(id, ids, sm, net, cfg, seed+int64(i)*7919)
		net.Register(id, s)
		c.Nodes[id] = s
		c.SMs[id] = sm
	}
	return c
}

// Start starts every node's role loop.
func (c *Cluster) Start() {
	for _, id := range c.IDs {
		c.Nodes[id].Start()
	}
}

// Stop stops every node.
func (c *Cluster) Stop() {
	for _, id := range c.IDs {
		c.Nodes[id].Stop()
	}
}

// Leaders returns every node currently claiming to be leader, with its term.
func (c *Cluster) Leaders() map[raft.NodeID]raft.Term {
	out := make(map[raft.NodeID]raft.Term)
	for _, id := range c.IDs {
		s := c.Nodes[id]
		if s.Role() == raft.Leader {
			out[id] = s.CurrentTerm()
		}
	}
	return out
}

// WaitForLeader polls until exactly one leader exists among the reachable nodes,
// or the timeout expires. Returns the leader's id and whether one was found.
//
// Polling rather than blocking on a signal keeps the harness simple and mirrors
// how an operator would observe a real cluster.
func (c *Cluster) WaitForLeader(timeout time.Duration) (raft.NodeID, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := c.Leaders()
		if len(leaders) == 1 {
			for id := range leaders {
				return id, true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", false
}

// WaitForCommit polls until every listed node has applied at least n commands.
func (c *Cluster) WaitForCommit(n int, timeout time.Duration, ids ...raft.NodeID) bool {
	if len(ids) == 0 {
		ids = c.IDs
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if len(c.SMs[id].AppliedCopy()) < n {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// RecordHistory captures what each node has applied so far, so State Machine
// Safety can be checked across time rather than only at the end.
func (c *Cluster) RecordHistory() {
	for _, id := range c.IDs {
		applied := c.SMs[id].AppliedCopy()
		if len(applied) > len(c.history[id]) {
			c.history[id] = applied
		}
	}
}

// --- Figure 3 safety assertions -------------------------------------------

// CheckElectionSafety verifies "at most one leader can be elected in a given
// term" (§5.2). Returns an error describing the violation, or nil.
func (c *Cluster) CheckElectionSafety() error {
	byTerm := make(map[raft.Term][]raft.NodeID)
	for id, term := range c.Leaders() {
		byTerm[term] = append(byTerm[term], id)
	}
	for term, ids := range byTerm {
		if len(ids) > 1 {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			return fmt.Errorf("Election Safety violated: %d leaders in term %d: %v",
				len(ids), term, ids)
		}
	}
	return nil
}

// CheckLogMatching verifies "if two logs contain an entry with the same index and
// term, then the logs are identical in all entries up through the given index"
// (§5.3).
func (c *Cluster) CheckLogMatching() error {
	// Indexed by LOG INDEX, not by slice position.
	//
	// Those coincided before compaction existed, and this check used to walk the
	// slices in parallel. Once a node compacts, its slice starts at the snapshot
	// boundary rather than at index 0, so position k means a different entry on
	// each node — and comparing them reported a Log Matching violation that was
	// purely an artifact of the checker. The property is stated over log indices,
	// so the check must be too.
	byIndex := make(map[raft.NodeID]map[raft.Index]raft.LogEntry, len(c.IDs))
	for _, id := range c.IDs {
		m := make(map[raft.Index]raft.LogEntry)
		entries := c.Nodes[id].LogEntries()
		for i, e := range entries {
			// entries[0] is the SENTINEL, and it is not a real log entry.
			//
			// Uncompacted it sits at index 0 with a zero term. After compaction it
			// carries the snapshot boundary — a real index and term, but still no
			// command, because it stands in for an entry whose content now lives in
			// the snapshot. Comparing it against the real entry another node still
			// holds at that index reports a difference that is not one: "" versus
			// "part-23" at index 25, both term 1.
			//
			// Skipping by position rather than by index is what makes this correct
			// for both cases.
			if i == 0 {
				continue
			}
			m[e.Index] = e
		}
		byIndex[id] = m
	}

	for i, a := range c.IDs {
		for _, b := range c.IDs[i+1:] {
			la, lb := byIndex[a], byIndex[b]

			// Find the highest index both hold with the same term. Everything at or
			// below it must be identical on both.
			var agreeAt raft.Index
			var agreeTerm raft.Term
			for idx, ea := range la {
				eb, ok := lb[idx]
				if !ok || ea.Term != eb.Term {
					continue
				}
				if idx > agreeAt {
					agreeAt, agreeTerm = idx, ea.Term
				}
			}
			if agreeAt == 0 {
				continue // no common entry; nothing the property constrains
			}

			for idx := raft.Index(1); idx <= agreeAt; idx++ {
				ea, okA := la[idx]
				eb, okB := lb[idx]
				// An entry missing from one log is compacted away, not divergent:
				// a snapshot only ever covers committed entries.
				if !okA || !okB {
					continue
				}
				if ea.Term != eb.Term || string(ea.Command) != string(eb.Command) {
					return fmt.Errorf(
						"Log Matching violated: %s and %s agree at index %d (term %d) but differ at index %d (%q/t%d vs %q/t%d)",
						a, b, agreeAt, agreeTerm, idx,
						ea.Command, ea.Term, eb.Command, eb.Term)
				}
			}
		}
	}
	return nil
}

// CheckStateMachineSafety verifies "if a server has applied a log entry at a
// given index to its state machine, no other server will ever apply a different
// log entry for the same index" (§5.4.3).
//
// This is checked against recorded history, not just current state: the property
// is about what was applied at any point, so a node that later caught up must
// still never have applied something different.
func (c *Cluster) CheckStateMachineSafety() error {
	c.RecordHistory()

	// index -> the command every node must agree on at that position.
	agreed := make(map[int]string)
	owner := make(map[int]raft.NodeID)

	for _, id := range c.IDs {
		for i, cmd := range c.history[id] {
			if prev, seen := agreed[i]; seen {
				if prev != cmd {
					return fmt.Errorf(
						"State Machine Safety violated at applied position %d: %s applied %q, %s applied %q",
						i+1, owner[i], prev, id, cmd)
				}
				continue
			}
			agreed[i] = cmd
			owner[i] = id
		}
	}
	return nil
}

// CheckLeaderCompleteness verifies that every entry committed on any node is
// present, with the same command, in the current leader's log (§5.4).
//
// This is the property that became testable only once elections existed.
func (c *Cluster) CheckLeaderCompleteness() error {
	leaders := c.Leaders()
	if len(leaders) != 1 {
		return nil // no unique leader right now; nothing to check
	}
	var leaderID raft.NodeID
	for id := range leaders {
		leaderID = id
	}
	// Indexed by log index rather than slice position, for the same reason as
	// CheckLogMatching: a compacted log does not start at index 0.
	leaderByIndex := make(map[raft.Index]raft.LogEntry)
	var leaderLast raft.Index
	leaderEntries := c.Nodes[leaderID].LogEntries()
	for i, e := range leaderEntries {
		if i == 0 {
			continue // sentinel, see CheckLogMatching
		}
		leaderByIndex[e.Index] = e
		if e.Index > leaderLast {
			leaderLast = e.Index
		}
	}
	// A compacted leader's log ends at its last real entry, but everything at or
	// below its snapshot boundary is still present — in the snapshot. Treating the
	// boundary as the floor keeps "the leader is missing a committed entry" from
	// firing on entries the leader deliberately discarded.
	leaderBase := leaderEntries[0].Index

	for _, id := range c.IDs {
		if id == leaderID {
			continue
		}
		s := c.Nodes[id]
		committed := s.CommitIndex()

		nodeEntries := s.LogEntries()
		for i, e := range nodeEntries {
			if i == 0 || e.Index > committed {
				continue // sentinel, or not yet committed here
			}
			if e.Index <= leaderBase {
				continue // covered by the leader's snapshot
			}
			le, ok := leaderByIndex[e.Index]
			if !ok {
				// Absent from the leader's log because the leader compacted past it,
				// not because it was lost: a snapshot only covers committed entries,
				// and Leader Completeness is about entries surviving, not about them
				// staying in uncompacted form. A genuinely missing entry shows up as
				// the leader's log ending BELOW the committed index.
				if e.Index > leaderLast {
					return fmt.Errorf(
						"Leader Completeness violated: %s committed index %d but leader %s's log ends at %d",
						id, e.Index, leaderID, leaderLast)
				}
				continue
			}
			if string(e.Command) != string(le.Command) {
				return fmt.Errorf(
					"Leader Completeness violated: committed entry %d is %q on %s but %q on leader %s",
					e.Index, e.Command, id, le.Command, leaderID)
			}
		}
	}
	return nil
}

// CheckAll runs every safety assertion. This is the steady-state hypothesis:
// whatever chaos was injected, all of these must still hold.
func (c *Cluster) CheckAll() error {
	for _, check := range []func() error{
		c.CheckElectionSafety,
		c.CheckLogMatching,
		c.CheckStateMachineSafety,
		c.CheckLeaderCompleteness,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}
