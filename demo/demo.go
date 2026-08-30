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
	ID       string            `json:"id"`
	Leader   string            `json:"leader"`
	Term     uint64            `json:"term"`
	Nodes    []NodeView        `json:"nodes"`
	Accounts map[string]int64  `json:"accounts"`
	InDoubt  int               `json:"in_doubt"`
	Ready    bool              `json:"ready"`
	Reason   string            `json:"reason,omitempty"`
	Keys     []string          `json:"keys"`
	Recent   []TransactionView `json:"recent"`
	Reserved map[string]int64  `json:"reserved,omitempty"`
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

// RingPoint is one account's placement on the consistent-hash ring.
type RingPoint struct {
	Account string  `json:"account"`
	Shard   string  `json:"shard"`
	Angle   float64 `json:"angle"`
}

// ClusterView is the whole cluster, pushed to the browser on every change.
//
// A complete snapshot rather than a delta, deliberately. It makes a dropped
// update harmless — a client that misses three frames and receives the fourth is
// fully current — which is what lets the stream drop for a slow consumer instead
// of blocking the producer (§20, and the same bounded-queue argument as §18).
type ClusterView struct {
	Time    string      `json:"time"`
	Shards  []ShardView `json:"shards"`
	Ring    []RingPoint `json:"ring"`
	Total   int64       `json:"total_money"`
	Events  []string    `json:"events"`
	Healthy bool        `json:"healthy"`
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
}

// New builds a demo cluster of nShards shards with nPerShard replicas each.
func New(nShards, nPerShard int, seed int64) (*Cluster, error) {
	sc := sim.NewShardCluster(nShards, nPerShard, seed)
	sc.Start()

	if !sc.WaitForLeaders(5 * time.Second) {
		sc.Stop()
		return nil, fmt.Errorf("demo: cluster did not elect leaders")
	}

	c := &Cluster{
		sc:      sc,
		crashed: make(map[raft.NodeID]bool),
		clock:   hlc.New(),
	}
	c.logf("cluster started: %d shards x %d replicas", nShards, nPerShard)
	return c, nil
}

// Stop shuts the cluster down.
func (c *Cluster) Stop() { c.sc.Stop() }

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

		// Ledger state, read from the leader when there is one.
		if m := g.Machine(); m != nil {
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

	return view
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
