package sim

import (
	"time"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Group-aware routing, for a CO-LOCATED cluster where one machine hosts a
// replica of several shards.
//
// The plain Network maps a node id to one server, which is right when each node
// belongs to exactly one Raft group. Co-location breaks that: node-1 runs a
// replica of shard-0 AND shard-1 AND shard-2, so a message has to name both the
// machine and the group.
//
// The important part is what stays keyed by MACHINE: crashed, partitions,
// latency, drops. A machine is reachable or it is not — when it dies, every group
// it hosts loses that replica together. Making reachability per-group would model
// a failure no physical machine has.
//
// This mirrors what rpc/ does over the wire, where the shard id is part of the
// net/rpc service name and the connection pool is shared per peer.

// RegisterGroup adds a server for one (shard, node) pair.
func (n *Network) RegisterGroup(sid shard.ID, id raft.NodeID, s *raft.Server) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.groups == nil {
		n.groups = make(map[shard.ID]map[raft.NodeID]*raft.Server)
	}
	if n.groups[sid] == nil {
		n.groups[sid] = make(map[raft.NodeID]*raft.Server)
	}
	n.groups[sid][id] = s

	// Reachability is per MACHINE, so the machine is registered once regardless of
	// how many groups it hosts.
	n.partitions[id] = 0
}

// ForGroup returns a transport that addresses one shard's replicas.
//
// The returned transport shares this Network's crash and partition state, which
// is the entire point: killing node-2 must take down its replica of every shard
// at once.
func (n *Network) ForGroup(sid shard.ID) raft.Transport {
	return &groupTransport{net: n, shard: sid}
}

// groupTransport delivers RPCs within one shard's group.
type groupTransport struct {
	net   *Network
	shard shard.ID
}

// target resolves a (shard, node) pair, applying the machine-level reachability
// rules the plain Network already implements.
func (t *groupTransport) target(from, to raft.NodeID) (*raft.Server, bool, bool, time.Duration) {
	// deliverable applies crash, partition, drop and latency — all keyed by
	// machine — and returns the node-level server, which is nil here because a
	// co-located cluster registers through RegisterGroup instead.
	_, ok, dup, delay := t.net.deliverable(from, to)
	if !ok {
		return nil, false, false, 0
	}

	t.net.mu.Lock()
	srv := t.net.groups[t.shard][to]
	t.net.mu.Unlock()

	if srv == nil {
		return nil, false, false, 0
	}
	return srv, true, dup, delay
}

func (t *groupTransport) SendAppendEntries(to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	srv, ok, dup, delay := t.target(args.LeaderID, to)
	if !ok {
		return raft.AppendEntriesReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	reply := srv.AppendEntries(args)
	if delay > 0 {
		time.Sleep(delay)
	}
	if dup {
		// Duplicate delivery must be a no-op; the receiver rules make it one.
		srv.AppendEntries(args)
	}
	return reply, nil
}

func (t *groupTransport) SendRequestVote(to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	srv, ok, dup, delay := t.target(args.CandidateID, to)
	if !ok {
		return raft.RequestVoteReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	reply := srv.RequestVote(args)
	if delay > 0 {
		time.Sleep(delay)
	}
	if dup {
		srv.RequestVote(args)
	}
	return reply, nil
}

// SendInstallSnapshot implements raft.SnapshotTransport (§7).
func (t *groupTransport) SendInstallSnapshot(to raft.NodeID, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	srv, ok, dup, delay := t.target(args.LeaderID, to)
	if !ok {
		return raft.InstallSnapshotReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	reply := srv.InstallSnapshot(args)
	if delay > 0 {
		time.Sleep(delay)
	}
	if dup {
		srv.InstallSnapshot(args)
	}
	return reply, nil
}
