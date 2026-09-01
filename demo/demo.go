// Package demo runs a live cluster behind a web UI (Phase 4).
//
// Everything this project claims has so far been verified only by test
// assertions. This makes the claims OBSERVABLE: which node leads, which follower
// lags, which transaction is in doubt, and what happens to a key's placement when
// a shard loses quorum. The difference between "the tests say a new leader is
// elected in ~200ms" and watching it happen is the whole point.
//
// [project decision] SSE + HTTP control, NOT WebSocket.
//
// NOW.md's frontend stack decision named gorilla/websocket or nhooyr.io/websocket.
// Both are third-party, and this module has zero third-party dependencies — a
// property the README advertises and that already shaped two earlier decisions
// (net/rpc over gRPC, hand-written Prometheus text over the client library).
// Server-Sent Events gets the same user-visible result in a few lines of
// net/http: the STREAM is one-way, and the control actions (kill a node, move
// money) are one-shot commands that fit ordinary HTTP requests. WebSocket's
// bidirectional framing solves a problem this does not have. Theory in
// learn/READING_LIST.md §20.
//
// Push rather than polling, and that is not a preference: a candidate state lasts
// one election timeout (150-300ms), a follower's pending-to-committed transition
// lasts one round trip, and an in-doubt 2PC transaction may resolve in
// milliseconds. Poll at one second and the UI shows only outcomes, never the
// mechanism.
//
// SAFETY BOUNDARY: this is a demo control plane. It can kill nodes and move money
// with no authentication, so it is a separate binary that must be started
// deliberately — never something a production node exposes.
package demo

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/homura/core-bank/hlc"
	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/sim"
)

// NodeView is one Raft replica as the dashboard sees it.
type NodeView struct {
	ID      string `json:"id"`
	Shard   string `json:"shard"`
	Role    string `json:"role"`
	Term    uint64 `json:"term"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
	LogLen  int    `json:"log_len"`

	// Crashed is the operator-injected fault: this node has been killed from the
	// UI and is not reachable on the network.
	Crashed bool `json:"crashed"`

	// Lagging marks a replica whose applied index is behind its group's leader.
	// Shown because a follower catching up must LOOK different from one that is
	// current — that visible difference is the point of the per-node view.
	Lagging bool `json:"lagging"`
}

// ShardView is one shard's state.
type ShardView struct {
	ID       string           `json:"id"`
	Leader   string           `json:"leader"`
	Term     uint64           `json:"term"`
	Nodes    []NodeView       `json:"nodes"`
	Accounts map[string]int64 `json:"accounts"`
	InDoubt  int              `json:"in_doubt"`
	Ready    bool             `json:"ready"`
	Reason   string           `json:"reason,omitempty"`
	Keys     []string         `json:"keys"`

	// Unreachable means every replica of this shard is down, so its balances
	// cannot be read at all. Distinct from "not ready": a shard with one live
	// replica can still be read (staler, but real), while this one cannot be read
	// from anywhere.
	Unreachable bool              `json:"unreachable"`
	Recent      []TransactionView `json:"recent"`
	Reserved    map[string]int64  `json:"reserved,omitempty"`

	// Live and Needed are the machines currently reachable and the majority
	// required. The CAP trade-off in two numbers: below the threshold this shard
	// REFUSES writes rather than accepting one a majority has not agreed to.
	Live   int `json:"live"`
	Needed int `json:"needed"`

	// CanWrite and CanRead separate the two halves that losing quorum affects
	// differently. Collapsing them would hide the most interesting state — a shard
	// that cannot commit but can still show the last agreed balance.
	CanWrite bool `json:"can_write"`
	CanRead  bool `json:"can_read"`
}

// TransactionView is one ledger transaction, for the activity feed.
type TransactionView struct {
	Seq       uint64 `json:"seq"`
	Op        string `json:"op"`
	Key       string `json:"key"`
	Timestamp string `json:"ts"`
	Wall      uint64 `json:"wall"`
	Logical   uint32 `json:"logical"`
	Summary   string `json:"summary"`
}

// RingArc is one span of the ring owned by a shard, in degrees.
//
// The ring's real structure is virtual nodes — 150 per shard here — so the arcs
// are what decides placement. A view plotting only account keys shows where those
// few keys happen to land while hiding the structure that put them there, which
// is the opposite of what a ring diagram is for.
type RingArc struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Shard string  `json:"shard"`
}

// RingPoint is one account's placement on the consistent-hash ring.
type RingPoint struct {
	Account string  `json:"account"`
	Shard   string  `json:"shard"`
	Angle   float64 `json:"angle"`
}

// MachineView is one physical machine and every shard replica it hosts.
//
// The dimension the shard-first view could not show. With co-location a machine
// carries a slice of several shards, so "what does node-2 hold?" and "which
// machines hold alice?" are both real questions — and the second is what proves
// an account is replicated rather than stored once.
type MachineView struct {
	ID      string `json:"id"`
	Crashed bool   `json:"crashed"`

	// Replicas is one entry per shard this machine hosts.
	Replicas []MachineReplica `json:"replicas"`

	// Accounts lists every account this machine holds a copy of, across all its
	// replicas. The answer to "if this machine burns, whose money is on it?"
	Accounts []string `json:"accounts"`

	// LeaderOf names the shards this machine currently leads.
	LeaderOf []string `json:"leader_of"`

	// Health is the simulated machine condition biasing how eagerly it campaigns,
	// and Locked reports whether the operator has pinned it.
	Health string `json:"health"`
	Locked bool   `json:"health_locked"`
}

// LogEntryView is one Raft log entry as the UI shows it.
//
// This is what actually gets replicated. Balances are a FOLD over these entries,
// not the thing being copied — which is why every machine replaying the same log
// lands on the same balance, and why replication is possible at all.
type LogEntryView struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Summary   string `json:"summary"`
	Committed bool   `json:"committed"`
	Applied   bool   `json:"applied"`
}

// MachineReplica is one shard's replica on one machine.
type MachineReplica struct {
	Shard    string           `json:"shard"`
	Role     string           `json:"role"`
	Term     uint64           `json:"term"`
	Applied  uint64           `json:"applied"`
	Commit   uint64           `json:"commit"`
	LogLen   int              `json:"log_len"`
	Lagging  bool             `json:"lagging"`
	Accounts map[string]int64 `json:"accounts"`

	// Log is the tail of this replica's own log — its copy, not the leader's.
	// Comparing them across machines is what makes replication visible rather
	// than merely asserted.
	Log []LogEntryView `json:"log"`
}

// ClusterView is the whole cluster, pushed to the browser on every change.
//
// A complete snapshot rather than a delta, deliberately. It makes a dropped
// update harmless — a client that misses three frames and receives the fourth is
// fully current — which is what lets the stream drop for a slow consumer instead
// of blocking the producer (§20, and the same bounded-queue argument as §18).
type ClusterView struct {
	Time     string        `json:"time"`
	Shards   []ShardView   `json:"shards"`
	Machines []MachineView `json:"machines"`
	Ring     []RingPoint   `json:"ring"`
	Arcs     []RingArc     `json:"arcs"`
	VNodes   int           `json:"vnodes"`
	Total    int64         `json:"total_money"`

	// The cluster's shape, so the UI can show the controls' current values and
	// report a machine that holds nothing.
	NodeCount         int      `json:"node_count"`
	ReplicationFactor int      `json:"replication_factor"`
	Events            []string `json:"events"`
	Healthy           bool     `json:"healthy"`
}

// Cluster is the demo's live cluster plus the fault state the UI has injected.
type Cluster struct {
	sc *sim.ShardCluster

	mu      sync.Mutex
	crashed map[raft.NodeID]bool
	events  []string
	clock   *hlc.Clock

	// accounts is every account the demo has opened, so the ring view can show
	// placement without scanning ledgers.
	accounts []ledger.AccountID

	// nodes is the CONFIGURED machine count, not the number that happen to hold a
	// shard. With a fixed replication factor a machine can hold nothing at all —
	// and an idle machine is exactly the case worth being able to see, so it must
	// not vanish from the count.
	nodes int

	// replicationFactor is how many machines each shard is placed on.
	replicationFactor int

	// health is the simulated per-machine condition, and whether the operator has
	// pinned it against automatic drift (health.go).
	health map[raft.NodeID]*healthState

	// driftDone stops the health drift goroutine.
	driftDone chan struct{}

	// eventLog is the structured, filterable form of events (eventlog.go). The
	// plain events slice above is kept in sync for readers that only want text.
	eventLog *eventLog

	// sampled is set once the raft watcher has taken its first observation.
	sampled bool
}

// New builds a demo cluster: nShards shards spread over nNodes machines, each
// shard replicated onto exactly replicationFactor of them.
func New(nShards, nNodes, replicationFactor int, seed int64) (*Cluster, error) {
	return NewWithStorage(nShards, nNodes, replicationFactor, seed, "")
}

// NewWithStorage is New with optional durability.
//
// An empty dataDir keeps the cluster in memory — fast, isolated, nothing left on
// disk, which is what tests and a throwaway demo want. A non-empty dataDir gives
// every replica its own WAL, so balances and in-flight 2PC promises survive the
// process being killed and restarted.
//
// Restoring is deliberately a separate step from constructing: Restore replays
// each node's log into its state machine BEFORE the servers start, because a
// node that begins campaigning with an empty log could be elected leader and
// then overwrite the very entries it was supposed to recover.
func NewWithStorage(nShards, nNodes, replicationFactor int, seed int64, dataDir string) (*Cluster, error) {
	// Each shard is placed on exactly `replicationFactor` of the machines.
	//
	// Keeping the replication factor FIXED while the machine count grows is what
	// makes the two dimensions independent: RF decides how many failures one shard
	// survives, machine count decides total capacity. Putting every shard on every
	// machine would make a 9-machine cluster need a quorum of 5, so adding
	// hardware would make writes slower and less available — the opposite of the
	// point.
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("demo: creating data directory: %w", err)
		}
	}

	sc, err := sim.NewPlacedClusterWithStorage(nShards, nNodes, replicationFactor, seed, dataDir)
	if err != nil {
		return nil, err
	}

	// Replay before starting. Restore loads each node's persisted term, vote and
	// log, then re-applies committed entries into its state machine.
	restored := false
	if dataDir != "" {
		if err := sc.RestoreAll(); err != nil {
			sc.Stop()
			return nil, fmt.Errorf("demo: restoring from %s: %w", dataDir, err)
		}
		restored = true
	}

	sc.Start()

	if !sc.WaitForLeaders(5 * time.Second) {
		sc.Stop()
		return nil, fmt.Errorf("demo: cluster did not elect leaders")
	}

	c := &Cluster{
		sc:       sc,
		crashed:  make(map[raft.NodeID]bool),
		eventLog: newEventLog(500),
		clock:    hlc.New(),
	}
	c.nodes = nNodes
	c.replicationFactor = replicationFactor
	c.health = make(map[raft.NodeID]*healthState)
	c.driftDone = make(chan struct{})
	go c.driftHealth(5*time.Second, c.driftDone)

	// Watch consensus so elections appear in the event log instead of only
	// showing up as a different leader on the next frame (raftwatch.go).
	go c.watchRaft(120*time.Millisecond, c.driftDone)

	c.logEvent(Event{
		Kind: KindControl,
		Text: fmt.Sprintf("cluster started: %d shards across %d machines, replication factor %d",
			nShards, nNodes, replicationFactor),
	})
	if restored {
		c.logEvent(Event{
			Kind: KindControl,
			Text: fmt.Sprintf("restored from %s — balances and in-flight transactions "+
				"replayed from each replica's log", dataDir),
		})
	}
	return c, nil
}

// Stop shuts the cluster down.
func (c *Cluster) Stop() {
	c.mu.Lock()
	done := c.driftDone
	c.driftDone = nil
	c.mu.Unlock()

	if done != nil {
		close(done)
	}
	c.sc.Stop()
}

// logf records an event for the UI's activity log.
func (c *Cluster) logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, fmt.Sprintf("%s  %s",
		time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...)))
	// Bounded: an unbounded event log is an unbounded buffer, and this one is
	// appended to on every action.
	if len(c.events) > 200 {
		c.events = c.events[len(c.events)-200:]
	}
}

// Snapshot builds the current cluster view.
//
// Every read of Raft state is a short, independent call. The alternative —
// holding a lock across the whole walk — would put the UI's refresh loop in
// contention with the consensus loop, which this project has already measured
// twice: reportRole polling the server mutex, and the client API busy-waiting at
// 2ms so client traffic degraded consensus itself (§20).
func (c *Cluster) Snapshot() ClusterView {
	view := ClusterView{
		Time:    time.Now().Format("15:04:05.000"),
		Healthy: true,
	}

	c.mu.Lock()
	crashed := make(map[raft.NodeID]bool, len(c.crashed))
	for k, v := range c.crashed {
		crashed[k] = v
	}
	events := append([]string(nil), c.events...)
	accounts := append([]ledger.AccountID(nil), c.accounts...)
	c.mu.Unlock()

	// Newest first: the interesting event is the one that just happened.
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	if len(events) > 40 {
		events = events[:40]
	}
	view.Events = events

	for _, sid := range c.sc.Ring.Shards() {
		g := c.sc.Groups[sid]

		sv := ShardView{
			ID:       string(sid),
			Accounts: make(map[string]int64),
			Reserved: make(map[string]int64),
		}

		// Highest applied index across live replicas, so "lagging" is measured
		// against what the group has actually achieved rather than against a
		// leader that may itself be behind.
		var highestApplied uint64
		for _, nid := range g.IDs {
			if crashed[nid] {
				continue
			}
			if a := uint64(g.Nodes[nid].LastApplied()); a > highestApplied {
				highestApplied = a
			}
		}

		for _, nid := range g.IDs {
			srv := g.Nodes[nid]
			nv := NodeView{
				ID:      string(nid),
				Shard:   string(sid),
				Role:    srv.Role().String(),
				Term:    uint64(srv.CurrentTerm()),
				Commit:  uint64(srv.CommitIndex()),
				Applied: uint64(srv.LastApplied()),
				LogLen:  len(srv.LogEntries()) - 1,
				Crashed: crashed[nid],
			}
			if crashed[nid] {
				// A killed node keeps its last known role in the struct, but the UI
				// shows it as down: reporting "Leader" for a node that is gone is
				// exactly the degraded-quorum blindness G5 removed.
				nv.Role = "Down"
			} else {
				nv.Lagging = nv.Applied < highestApplied
				if srv.Role() == raft.Leader {
					sv.Leader = string(nid)
					sv.Term = nv.Term
				}
			}
			sv.Nodes = append(sv.Nodes, nv)
		}

		// The CAP numbers: how many machines are reachable, and how many are needed.
		live := 0
		for _, nid := range g.IDs {
			if !crashed[nid] {
				live++
			}
		}
		sv.Live = live
		sv.Needed = len(g.IDs)/2 + 1
		sv.CanWrite = live >= sv.Needed
		sv.CanRead = live > 0

		// Readiness from the leader's own health, which is the only view that
		// knows whether a quorum is reachable.
		if sv.Leader != "" {
			h := g.Nodes[raft.NodeID(sv.Leader)].Health(500 * time.Millisecond)
			sv.Ready = h.Ready
			sv.Reason = h.NotReadyReason
		} else {
			sv.Reason = "no leader"
		}
		if !sv.Ready {
			view.Healthy = false
		}

		// Ledger state, read from the most advanced LIVE replica.
		//
		// Deliberately not ShardGroup.Machine(), which falls back to IDs[0] when
		// there is no leader. That fallback reads from whichever node happens to be
		// first — possibly one that is crashed, and possibly one that is behind
		// (measured here: n1 at applied=11 next to n2 at applied=5 in the same
		// shard). Showing a crashed node's ledger as the shard's balance is the
		// degraded-quorum blindness this project spent G5 removing, reintroduced in
		// the view.
		//
		// When every replica is down there is no honest answer, so the shard reports
		// no accounts and its balances are EXCLUDED from the total. A total that
		// silently includes unreachable money is worse than one that visibly drops:
		// the first looks correct and is not.
		if m := c.freshestMachine(g, crashed); m != nil {
			for acct, bal := range m.State.Balances() {
				sv.Accounts[string(acct)] = int64(bal)
				sv.Keys = append(sv.Keys, string(acct))
				view.Total += int64(bal)
				if r := m.State.Reserved(acct); r != 0 {
					sv.Reserved[string(acct)] = int64(r)
				}
			}
			sv.InDoubt = len(m.InDoubt())
			sv.Recent = recentTransactions(m.State.History(), 8)
		} else {
			sv.Unreachable = true
		}
		sort.Strings(sv.Keys)

		view.Shards = append(view.Shards, sv)
	}

	// Ring placement for every account the demo has opened. This is what makes
	// re-placement visible when a shard is lost.
	for _, a := range accounts {
		sid := c.sc.Ring.Lookup(string(a))
		view.Ring = append(view.Ring, RingPoint{
			Account: string(a),
			Shard:   string(sid),
			Angle:   ringAngle(string(a)),
		})
	}
	sort.Slice(view.Ring, func(i, j int) bool { return view.Ring[i].Angle < view.Ring[j].Angle })

	view.NodeCount = c.nodes
	view.ReplicationFactor = c.replicationFactor
	view.Arcs = c.ringArcs()
	view.VNodes = c.sc.Ring.VNodes()
	view.Machines = c.machines(crashed)
	return view
}

// ringArcs converts the ring's virtual-node segments into drawable arcs.
//
// Adjacent segments owned by the same shard are merged: with 150 vnodes per shard
// the raw list is 450 arcs, most of them a fraction of a degree, and drawing them
// individually produces noise rather than structure. Merging keeps every boundary
// that matters — where ownership actually changes — while cutting the count to
// something a browser can render honestly.
func (c *Cluster) ringArcs() []RingArc {
	segs := c.sc.Ring.Segments()
	if len(segs) == 0 {
		return nil
	}

	const full = float64(uint64(1) << 32)
	deg := func(h uint32) float64 { return float64(h) / full * 360.0 }

	out := make([]RingArc, 0, len(segs))
	for _, sg := range segs {
		start, end := deg(sg.Start), deg(sg.End)
		// A wrapping arc is split at 0 degrees so the renderer never has to handle
		// an end that precedes its start.
		if end < start {
			out = append(out, RingArc{Start: start, End: 360, Shard: string(sg.Shard)})
			out = append(out, RingArc{Start: 0, End: end, Shard: string(sg.Shard)})
			continue
		}
		out = append(out, RingArc{Start: start, End: end, Shard: string(sg.Shard)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })

	// Merge neighbours belonging to the same shard.
	merged := make([]RingArc, 0, len(out))
	for _, a := range out {
		if n := len(merged); n > 0 && merged[n-1].Shard == a.Shard && merged[n-1].End >= a.Start-1e-9 {
			if a.End > merged[n-1].End {
				merged[n-1].End = a.End
			}
			continue
		}
		merged = append(merged, a)
	}
	return merged
}

// machines inverts the shard-first view into a machine-first one.
//
// Same underlying state, read the other way round: instead of "which machines
// does this shard live on", it answers "which shards does this machine hold" —
// and, by union, "whose accounts would I lose if this machine burned".
func (c *Cluster) machines(crashed map[raft.NodeID]bool) []MachineView {
	byMachine := make(map[raft.NodeID]*MachineView)
	var order []raft.NodeID

	for _, sid := range c.sc.Ring.Shards() {
		g := c.sc.Groups[sid]

		// Highest applied among live replicas of THIS shard, so "lagging" compares
		// like with like.
		var highest uint64
		for _, nid := range g.IDs {
			if crashed[nid] {
				continue
			}
			if a := uint64(g.Nodes[nid].LastApplied()); a > highest {
				highest = a
			}
		}

		for _, nid := range g.IDs {
			mv, ok := byMachine[nid]
			if !ok {
				mv = &MachineView{
					ID:      string(nid),
					Crashed: crashed[nid],
					// Empty slices rather than nil: they serialize as [] instead of
					// null, so the UI never has to guard against a missing list.
					LeaderOf: []string{},
					Accounts: []string{},
				}
				byMachine[nid] = mv
				order = append(order, nid)
			}

			srv := g.Nodes[nid]
			rep := MachineReplica{
				Shard:    string(sid),
				Role:     srv.Role().String(),
				Term:     uint64(srv.CurrentTerm()),
				Applied:  uint64(srv.LastApplied()),
				Commit:   uint64(srv.CommitIndex()),
				LogLen:   len(srv.LogEntries()) - 1,
				Accounts: make(map[string]int64),
			}
			if crashed[nid] {
				rep.Role = "Down"
			} else {
				rep.Lagging = rep.Applied < highest
				if srv.Role() == raft.Leader {
					mv.LeaderOf = append(mv.LeaderOf, string(sid))
				}
			}

			rep.Log = logTail(srv, 6)

			// This machine's OWN copy of the shard's ledger, not the leader's. A
			// per-machine view that showed the leader's data would hide exactly the
			// divergence it exists to reveal — a lagging replica looking current.
			for acct, bal := range g.SMs[nid].State.Balances() {
				rep.Accounts[string(acct)] = int64(bal)
				if !contains(mv.Accounts, string(acct)) {
					mv.Accounts = append(mv.Accounts, string(acct))
				}
			}

			mv.Replicas = append(mv.Replicas, rep)
		}
	}

	// Machines holding NO shard must still appear. With a fixed replication factor
	// a large cluster leaves some machines idle, and an idle machine silently
	// missing from the list is the opposite of what this view is for.
	for i := range c.nodes {
		nid := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		if _, ok := byMachine[nid]; !ok {
			byMachine[nid] = &MachineView{
				ID: string(nid), Crashed: crashed[nid],
				LeaderOf: []string{}, Accounts: []string{},
				Replicas: []MachineReplica{},
			}
			order = append(order, nid)
		}
	}

	out := make([]MachineView, 0, len(order))
	for _, nid := range order {
		mv := byMachine[nid]
		lvl, locked := c.HealthOf(nid)
		mv.Health, mv.Locked = lvl.String(), locked
		sort.Strings(mv.Accounts)
		sort.Slice(mv.Replicas, func(i, j int) bool { return mv.Replicas[i].Shard < mv.Replicas[j].Shard })
		out = append(out, *mv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// freshestMachine returns the state machine of the most up-to-date LIVE replica,
// or nil when every replica of the shard is down.
//
// Prefers the leader — the only replica that can promise a current read — and
// otherwise takes the live replica with the highest applied index, which is the
// closest thing to the truth still reachable.
func (c *Cluster) freshestMachine(g *sim.ShardGroup, crashed map[raft.NodeID]bool) *shard.Machine {
	var best raft.NodeID
	var bestApplied raft.Index

	for _, nid := range g.IDs {
		if crashed[nid] {
			continue
		}
		srv := g.Nodes[nid]
		if srv.Role() == raft.Leader {
			return g.SMs[nid]
		}
		if a := srv.LastApplied(); best == "" || a > bestApplied {
			best, bestApplied = nid, a
		}
	}
	if best == "" {
		return nil
	}
	return g.SMs[best]
}

// logTail renders the last n entries of a replica's log.
//
// Each entry is marked committed (a majority has stored it) and applied (this
// machine has folded it into its balances). The gap between those two is the
// replication story: an entry exists on a follower before it is committed, and is
// committed before it is applied.
func logTail(srv *raft.Server, n int) []LogEntryView {
	entries := srv.LogEntries()
	commit := uint64(srv.CommitIndex())
	applied := uint64(srv.LastApplied())

	// entries[0] is the sentinel, never a real entry.
	if len(entries) > 1 {
		entries = entries[1:]
	} else {
		return nil
	}
	if len(entries) > n {
		entries = entries[len(entries)-n:]
	}

	out := make([]LogEntryView, 0, len(entries))
	for _, e := range entries {
		out = append(out, LogEntryView{
			Index:     uint64(e.Index),
			Term:      uint64(e.Term),
			Summary:   summarizeCommand(e.Command),
			Committed: uint64(e.Index) <= commit,
			Applied:   uint64(e.Index) <= applied,
		})
	}
	return out
}

// summarizeCommand renders a log entry's payload for display.
//
// Best-effort: raft treats commands as opaque bytes, so this decodes what it can
// and says so plainly when it cannot rather than showing misleading detail.
func summarizeCommand(cmd []byte) string {
	if len(cmd) == 0 {
		return "no-op (new leader)"
	}
	sc, err := shard.DecodeCommand(cmd)
	if err != nil {
		return "entry"
	}
	switch sc.Op {
	case shard.OpPrepare:
		return "2PC prepare " + string(sc.TxID)
	case shard.OpDecision:
		if sc.Commit {
			return "2PC COMMIT " + string(sc.TxID)
		}
		return "2PC ABORT " + string(sc.TxID)
	case shard.OpOutcome:
		return "2PC outcome " + string(sc.TxID)
	}

	l := sc.Ledger
	switch l.Op {
	case ledger.OpOpenAccount:
		return "open " + string(l.To) + " " + l.Amount.String()
	case ledger.OpDeposit:
		return "deposit " + l.Amount.String() + " to " + string(l.To)
	case ledger.OpWithdraw:
		return "withdraw " + l.Amount.String() + " from " + string(l.From)
	case ledger.OpTransfer:
		return "transfer " + l.Amount.String() + " " + string(l.From) + "->" + string(l.To)
	}
	return "entry"
}

// recentTransactions renders the tail of a ledger history for the activity feed.
func recentTransactions(history []ledger.Transaction, n int) []TransactionView {
	if len(history) > n {
		history = history[len(history)-n:]
	}
	out := make([]TransactionView, 0, len(history))
	for _, t := range history {
		var summary string
		for _, e := range t.Entries {
			sign := "+"
			amt := e.Amount
			if amt < 0 {
				sign, amt = "-", -amt
			}
			summary += fmt.Sprintf("%s %s%s  ", e.Account, sign, amt)
		}
		out = append(out, TransactionView{
			Seq:       t.Seq,
			Op:        t.Op.String(),
			Key:       t.IdempotencyKey,
			Timestamp: t.Timestamp.String(),
			Wall:      t.Timestamp.Wall,
			Logical:   t.Timestamp.Logical,
			Summary:   summary,
		})
	}
	// Newest first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// ringAngle maps an account onto the 2^32 hash ring, in degrees.
//
// The same hash the ring itself uses, so the drawn position is the real one
// rather than a decorative approximation.
func ringAngle(key string) float64 {
	return float64(shard.HashKey(key)) / float64(1<<32) * 360.0
}

// MarshalView serializes a view for the SSE stream.
func MarshalView(v ClusterView) ([]byte, error) { return json.Marshal(v) }
