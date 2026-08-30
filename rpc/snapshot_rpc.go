package rpc

import (
	"time"

	"github.com/homura/core-bank/raft"
)

// InstallSnapshot over the network (§7).
//
// Registered on the same service as the other two Raft RPCs, so a sharded node's
// snapshot traffic is demultiplexed by shard exactly like its AppendEntries —
// there is no second addressing scheme to keep in step.

// InstallSnapshot is the net/rpc entry point for raft.InstallSnapshot.
func (s *RaftService) InstallSnapshot(args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	*reply = s.srv.InstallSnapshot(args)
	return nil
}

// SendInstallSnapshot implements raft.SnapshotTransport.
//
// A snapshot is larger than an AppendEntries and may legitimately take longer
// than a heartbeat-sized RPC timeout, so it is given a longer budget. Using the
// per-RPC timeout here would make every install fail on a large state machine and
// leave the follower permanently behind — the failure would look like a network
// problem rather than a timeout that is too short.
func (t *Transport) SendInstallSnapshot(to raft.NodeID, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	var reply, scratch raft.InstallSnapshotReply
	err := t.callWithTimeout(to, t.service+".InstallSnapshot", args, &reply, &scratch,
		snapshotSendTimeout(t.timeout))
	return reply, err
}

// snapshotSendTimeout scales the ordinary RPC timeout up for snapshot transfers.
//
// Ten times the per-RPC timeout, with a floor: the ratio keeps it proportional to
// whatever the operator configured, and the floor stops an aggressively short
// -rpc-timeout from making snapshots impossible.
func snapshotSendTimeout(base time.Duration) time.Duration {
	const floor = 5 * time.Second
	if scaled := base * 10; scaled > floor {
		return scaled
	}
	return floor
}
