package sim

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// A multi-group cluster: several independent Raft groups, one per shard.
//
// This is what makes write throughput scale — separate shards have separate
// leaders committing in parallel, unlike Phase 1 where every write funnelled
// through one leader (measured: 3 nodes 119.9 tx/s vs 5 nodes 105.9 tx/s).

// ShardGroup is one shard's Raft group.
type ShardGroup struct {
	ID    shard.ID
	IDs   []raft.NodeID
	Nodes map[raft.NodeID]*raft.Server
	Net   *Network

	// SMs holds ONE state machine per node. Replicas must not share a state
	// machine: each node applies the log independently, and sharing one instance
	// would apply every committed entry once per replica — a 3x debit on a 3-node
	// shard. It would also violate the project's rule that nodes share nothing.
	SMs map[raft.NodeID]*shard.Machine
}

// Propose implements shard.Group: replicate a command and wait for it to apply.
func (g *ShardGroup) Propose(cmd shard.Command, timeout time.Duration) (ledger.Result, bool, error) {
	leader := g.leader()
	if leader == "" {
		return ledger.Result{}, false, fmt.Errorf("sim: shard %s has no leader", g.ID)
	}

	idx, _, ok := g.Nodes[leader].Submit(cmd.Encode())
	if !ok {
		return ledger.Result{}, false, nil
	}

	// Wait for the entry to be applied on the leader, then return the state
	// machine's ACTUAL result.
	//
	// Returning a canned {OK: true} here would mean "the entry replicated",
	// which is a different question from "the operation succeeded" — a prepare
	// that votes NO replicates perfectly well. Conflating the two made the
	// coordinator read every vote as YES.
	select {
	case <-g.Nodes[leader].WaitApplied(idx):
		return g.SMs[leader].AppliedResult(idx), true, nil
	case <-time.After(timeout):
		if g.Nodes[leader].Role() != raft.Leader {
			return ledger.Result{}, false, fmt.Errorf("sim: shard %s lost leadership mid-propose", g.ID)
		}
		return ledger.Result{}, true, fmt.Errorf("sim: shard %s timed out applying entry %d", g.ID, idx)
	}
}

// Machine implements shard.Group, returning the LEADER's state machine — the
// authoritative copy for reads and for coordinator decisions. Followers hold
// their own replicas of the same state.
func (g *ShardGroup) Machine() *shard.Machine {
	if l := g.leader(); l != "" {
		return g.SMs[l]
	}
	return g.SMs[g.IDs[0]]
}

// IsLeader implements shard.Group.
func (g *ShardGroup) IsLeader() bool { return g.leader() != "" }

func (g *ShardGroup) leader() raft.NodeID {
	for _, id := range g.IDs {
		if g.Nodes[id].Role() == raft.Leader {
			return id
		}
	}
	return ""
}

// ShardCluster is several ShardGroups plus the ring and coordinator.
type ShardCluster struct {
	Ring        *shard.Ring
	Groups      map[shard.ID]*ShardGroup
	Coordinator *shard.Coordinator
	Nets        map[shard.ID]*Network
}

// NewShardCluster builds nShards groups of nPerShard nodes each.
func NewShardCluster(nShards, nPerShard int, seed int64) *ShardCluster {
	cfg := raft.Config{ElectionTimeoutMin: 60, ElectionTimeoutMax: 120, HeartbeatInterval: 15}

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
		// Each shard gets its own network: groups are genuinely independent, with
		// no shared transport, exactly as separate machines would be.
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
			// Each node gets its own ledger and its own state machine, exactly as
			// separate processes would.
			machine := shard.NewMachine(sid, ledger.New())
			srv := raft.NewServerWith(nid, ids, machine, net, cfg,
				seed+int64(si)*7919+int64(j)*31)
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

// Start starts every node in every group.
func (sc *ShardCluster) Start() {
	for _, g := range sc.Groups {
		for _, id := range g.IDs {
			g.Nodes[id].Start()
		}
	}
}

// Stop stops everything.
func (sc *ShardCluster) Stop() {
	for _, g := range sc.Groups {
		for _, id := range g.IDs {
			g.Nodes[id].Stop()
		}
	}
}

// WaitForLeaders waits until every shard has elected a leader.
func (sc *ShardCluster) WaitForLeaders(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, g := range sc.Groups {
			if g.leader() == "" {
				all = false
				break
			}
		}
		if all {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// Open creates an account on whichever shard owns it.
func (sc *ShardCluster) Open(account ledger.AccountID, amount ledger.Money) error {
	sid := sc.Coordinator.ShardFor(account)
	g := sc.Groups[sid]

	_, _, err := g.Propose(shard.Command{
		Op: shard.OpSingle,
		Ledger: ledger.Command{
			Op: ledger.OpOpenAccount, IdempotencyKey: "open-" + string(account),
			To: account, Amount: amount,
		},
	}, 3*time.Second)
	return err
}

// TotalMoney sums balances across every shard.
//
// This is Phase 2's central invariant: no chaos scenario may change it. Reserved
// funds still count — reserved money is unavailable, not gone.
func (sc *ShardCluster) TotalMoney() ledger.Money {
	var total ledger.Money
	for _, g := range sc.Groups {
		for _, b := range g.Machine().State.Balances() {
			total += b
		}
	}
	return total
}

// Balance finds an account's balance on its owning shard.
func (sc *ShardCluster) Balance(account ledger.AccountID) (ledger.Money, bool) {
	sid := sc.Coordinator.ShardFor(account)
	return sc.Groups[sid].Machine().State.Balance(account)
}

// InDoubt returns the total number of blocked transactions across all shards.
func (sc *ShardCluster) InDoubt() int { return sc.Coordinator.InDoubtCount() }

// View renders shard state, for diagnosing failures and for the dashboard.
func (sc *ShardCluster) View() string {
	out := "\n  SHARD      LEADER              TERM  COMMIT  ACCOUNTS  IN-DOUBT\n"
	out += "  -----      ------              ----  ------  --------  --------\n"
	for _, sid := range sc.Ring.Shards() {
		g := sc.Groups[sid]
		leader := g.leader()
		var term raft.Term
		var commit raft.Index
		if leader != "" {
			term = g.Nodes[leader].CurrentTerm()
			commit = g.Nodes[leader].CommitIndex()
		}
		out += fmt.Sprintf("  %-10s %-19s %-5d %-7d %-9d %d\n",
			sid, leader, term, commit,
			len(g.Machine().State.Balances()), len(g.Machine().InDoubt()))
	}
	return out
}
