package rpc

import (
	"fmt"
	"net/rpc"
	"sort"
	"strings"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Multi-shard hosting: one process, many independent Raft groups (G1).
//
// Phase 2 proved sharding works with every shard on its own in-process network.
// A real deployment cannot give each shard its own port and its own connection to
// every peer — S shards across N nodes is S×N connections per process. Real
// systems multiplex: CockroachDB carries many Ranges over shared node-to-node
// connections, TiKV does the same for Regions. Theory in
// learn/READING_LIST.md §14.
//
// [project decision] Multiplex by net/rpc SERVICE NAME — each hosted shard
// registers as "Raft-<shard-id>" — rather than inventing a framed header.
// net/rpc already demultiplexes by service name and by sequence number over one
// connection, and it already runs each call in its own goroutine. That last
// property is what keeps shards independent on a shared transport: a slow handler
// for shard A cannot stall shard B's messages. The change is confined to
// registration and addressing; no consensus code is touched, which is the
// property that made raft.Transport worth abstracting.

// Replica is one shard's replica hosted in this process: its own Raft server, its
// own state machine, its own storage. Replicas share nothing but the transport.
type Replica struct {
	ShardID shard.ID
	Raft    *raft.Server
	Machine *shard.Machine
}

// ShardHost hosts a set of shard replicas behind one listener.
type ShardHost struct {
	replicas map[shard.ID]*Replica
	server   *Server

	// transport is the shared connection pool every replica dials through.
	transport *Transport
}

// RegisterShards registers each replica under its own net/rpc service name and
// starts a listener on addr.
//
// The shard id is part of the service name, so an AppendEntries for shard-0
// cannot be delivered to shard-1's Raft server even by a malformed caller: the
// method simply does not exist and net/rpc rejects the call. That is stronger
// than validating a shard field inside the message, because there is no code path
// where the check can be forgotten.
func RegisterShards(addr string, replicas []*Replica, transport *Transport,
	client *ShardClientService, tc TLSConfig) (*ShardHost, error) {

	if len(replicas) == 0 {
		return nil, fmt.Errorf("rpc: no shard replicas to host")
	}

	r := rpc.NewServer()
	byID := make(map[shard.ID]*Replica, len(replicas))

	for _, rep := range replicas {
		if rep == nil || rep.Raft == nil || rep.Machine == nil {
			return nil, fmt.Errorf("rpc: incomplete replica for shard %q", rep.ShardID)
		}
		if _, dup := byID[rep.ShardID]; dup {
			// The same class of error as a duplicate node id in -peers: it would
			// silently host one shard twice while another goes unhosted.
			return nil, fmt.Errorf("rpc: shard %s registered twice on this node", rep.ShardID)
		}
		name := "Raft-" + string(rep.ShardID)
		if err := r.RegisterName(name, &RaftService{srv: rep.Raft}); err != nil {
			return nil, fmt.Errorf("rpc: register %s: %w", name, err)
		}
		byID[rep.ShardID] = rep
	}

	if client != nil {
		if err := r.RegisterName("Bank", client); err != nil {
			return nil, fmt.Errorf("rpc: register bank: %w", err)
		}
	}

	srv, err := listenOn(addr, r, tc)
	if err != nil {
		return nil, err
	}
	return &ShardHost{replicas: byID, server: srv, transport: transport}, nil
}

// Addr returns the bound address.
func (h *ShardHost) Addr() string { return h.server.Addr() }

// Replica returns a hosted replica.
func (h *ShardHost) Replica(id shard.ID) (*Replica, bool) {
	r, ok := h.replicas[id]
	return r, ok
}

// ShardIDs lists the shards this process hosts, in a stable order.
func (h *ShardHost) ShardIDs() []shard.ID {
	out := make([]shard.ID, 0, len(h.replicas))
	for id := range h.replicas {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Start starts every hosted replica's Raft server.
func (h *ShardHost) Start() {
	for _, id := range h.ShardIDs() {
		h.replicas[id].Raft.Start()
	}
}

// Stop stops every hosted replica and closes the listener.
//
// Order matters and mirrors the single-group shutdown: leadership is given up
// first so clients are redirected rather than left waiting on a node that is
// going away, and only then does the listener close.
func (h *ShardHost) Stop() {
	for _, id := range h.ShardIDs() {
		h.replicas[id].Raft.Stop()
	}
	h.server.Close()
}

// NetworkGroup implements shard.Group over the network for one shard.
//
// sim.ShardGroup was the only implementation until now, and it drives Raft
// in-process. This is its production counterpart: same three methods, but the
// group may be led by another process.
//
// [project decision] A non-leader REDIRECTS rather than forwarding. The client
// gets NotLeader plus the leader's address and retries there, which is §8's
// prescription ("that server will reject the client's request and supply
// information about the most recent leader"). Server-side forwarding was
// considered and rejected: it hides which node actually served the write, doubles
// the number of hops a timeout can hide behind, and makes an Indeterminate result
// ambiguous between two different nodes' logs. The client already carries an
// idempotency key precisely so that retrying at a new address is safe.
type NetworkGroup struct {
	id shard.ID

	// local is this process's replica of the shard, when it hosts one. Nil means
	// this process holds no replica of this shard, so it cannot serve the group at
	// all and says so rather than guessing.
	local *Replica
}

// NewNetworkGroup builds a shard.Group backed by a local replica.
func NewNetworkGroup(id shard.ID, local *Replica) *NetworkGroup {
	return &NetworkGroup{id: id, local: local}
}

// Propose implements shard.Group: replicate a command through this shard's Raft
// group and return what it actually did.
//
// Returning a canned success would conflate "the entry replicated" with "the
// operation succeeded" — a prepare that votes NO replicates perfectly well. That
// conflation made the coordinator read every vote as YES, and the fix was to key
// results to the applied log index. The same rule applies here.
func (g *NetworkGroup) Propose(cmd shard.Command, timeout time.Duration) (ledger.Result, bool, error) {
	if g.local == nil {
		return ledger.Result{}, false, fmt.Errorf("rpc: no replica of shard %s on this node", g.id)
	}

	idx, _, isLeader := g.local.Raft.Submit(cmd.Encode())
	if !isLeader {
		// isLeader=false with a nil error is the coordinator's signal to report a
		// redirect. Returning an error here instead would be read as a failed
		// operation rather than a misrouted one.
		return ledger.Result{}, false, nil
	}

	select {
	case <-g.local.Raft.WaitApplied(idx):
		return g.local.Machine.AppliedResult(idx), true, nil
	case <-time.After(timeout):
		if g.local.Raft.Role() != raft.Leader {
			return ledger.Result{}, false, fmt.Errorf("rpc: shard %s lost leadership mid-propose", g.id)
		}
		// Timing out does NOT mean the operation failed: the entry is in the
		// leader's log and may still commit. Reported as an error so the caller
		// treats it as indeterminate rather than as an abort — a 2PC coordinator
		// that read this as a NO vote would abort a transaction that then commits.
		return ledger.Result{}, true, fmt.Errorf("rpc: shard %s timed out applying entry %d", g.id, idx)
	}
}

// Machine implements shard.Group.
func (g *NetworkGroup) Machine() *shard.Machine {
	if g.local == nil {
		return nil
	}
	return g.local.Machine
}

// IsLeader implements shard.Group.
func (g *NetworkGroup) IsLeader() bool {
	return g.local != nil && g.local.Raft.Role() == raft.Leader
}

// ParseShardAssignment parses "shard-0,shard-2" into shard ids.
//
// Validated with the same severity as -peers: a process that believes it hosts a
// shard it does not is a phantom-quorum-class bug — the ring would route accounts
// to a group with no replica here, and reads would silently find nothing.
func ParseShardAssignment(s string) ([]shard.ID, error) {
	var out []shard.ID
	seen := make(map[shard.ID]struct{})

	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id := shard.ID(part)
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("duplicate shard %q in assignment", id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no shards given")
	}
	return out, nil
}
