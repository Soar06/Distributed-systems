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
func (c *CountingSM) Snapshot() []string {
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
			if len(c.SMs[id].Snapshot()) < n {
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
		applied := c.SMs[id].Snapshot()
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
	logs := make(map[raft.NodeID][]raft.LogEntry)
	for _, id := range c.IDs {
		logs[id] = c.Nodes[id].LogEntries()
	}

	for i, a := range c.IDs {
		for _, b := range c.IDs[i+1:] {
			la, lb := logs[a], logs[b]
			limit := min(len(la), len(lb))
			for k := limit - 1; k >= 0; k-- {
				if la[k].Term != lb[k].Term {
					continue
				}
				// Same index and term: everything before must match too.
				for j := 0; j <= k; j++ {
					if la[j].Term != lb[j].Term || string(la[j].Command) != string(lb[j].Command) {
						return fmt.Errorf(
							"Log Matching violated: %s and %s agree at index %d (term %d) but differ at index %d (%q/t%d vs %q/t%d)",
							a, b, la[k].Index, la[k].Term, la[j].Index,
							la[j].Command, la[j].Term, lb[j].Command, lb[j].Term)
					}
				}
				break
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
	leaderLog := c.Nodes[leaderID].LogEntries()

	for _, id := range c.IDs {
		if id == leaderID {
			continue
		}
		s := c.Nodes[id]
		committed := s.CommitIndex()
		nodeLog := s.LogEntries()

		for idx := raft.Index(1); idx <= committed && int(idx) < len(nodeLog); idx++ {
			if int(idx) >= len(leaderLog) {
				return fmt.Errorf(
					"Leader Completeness violated: %s committed index %d but leader %s's log ends at %d",
					id, idx, leaderID, len(leaderLog)-1)
			}
			if string(nodeLog[idx].Command) != string(leaderLog[idx].Command) {
				return fmt.Errorf(
					"Leader Completeness violated: committed entry %d is %q on %s but %q on leader %s",
					idx, nodeLog[idx].Command, id, leaderLog[idx].Command, leaderID)
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
