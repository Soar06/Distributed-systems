package sim

import (
	"time"

	"github.com/homura/core-bank/raft"
)

// SendInstallSnapshot implements raft.SnapshotTransport (§7).
//
// Subject to the same fault injection as every other RPC — drops, delays, and
// duplicate delivery — because a snapshot install is not privileged: it crosses
// the same network and must tolerate the same failures. Duplicate delivery in
// particular has to be a no-op, and the receiver's "already covered" guard is
// what makes it one.
func (n *Network) SendInstallSnapshot(to raft.NodeID, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	target, ok, dup, delay := n.deliverable(args.LeaderID, to)
	if !ok {
		return raft.InstallSnapshotReply{}, raft.ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay) // outbound flight time
	}

	reply := target.InstallSnapshot(args)
	if delay > 0 {
		time.Sleep(delay) // return flight time
	}
	if dup {
		// The duplicate-delivery flow RULES.md rule 3 requires. Installing the same
		// snapshot twice must not rewind a state machine that has moved on.
		target.InstallSnapshot(args)
	}
	return reply, nil
}
