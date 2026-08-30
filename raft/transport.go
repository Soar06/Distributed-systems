package raft

import "errors"

// ErrUnreachable is returned by a Transport when a peer cannot be reached — the
// node is down, or the network is partitioned. It is an ordinary, expected
// condition in Raft, not a bug: the algorithm is designed to make progress as
// long as a majority is reachable.
var ErrUnreachable = errors.New("raft: peer unreachable")

// Transport sends outbound RPCs to peers.
//
// Raft's correctness must not depend on the transport being reliable: calls may
// fail, be delayed, be delivered more than once, or be delivered out of order.
// The receiver rules in server.go are written to tolerate all of that.
//
// Phase 1 has two implementations: an in-memory one in sim/ that can inject
// faults deterministically, and (later) a gRPC one in rpc/. The Raft code cannot
// tell the difference, which is what makes chaos testing possible without
// touching consensus logic.
type Transport interface {
	SendAppendEntries(to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendRequestVote(to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
}

// Config holds the timing parameters that drive the role loop.
//
// §5.2 requires the timing inequality:
//
//	broadcastTime << electionTimeout << MTBF
//
// The heartbeat interval must be well below the election timeout, or followers
// will time out and start pointless elections while a healthy leader is running.
type Config struct {
	// ElectionTimeoutMin and ElectionTimeoutMax bound the randomized election
	// timeout. Randomization is not a detail — it is what breaks split votes
	// (§5.2). With a fixed timeout, servers repeatedly time out together, split
	// the vote, and can livelock without ever electing a leader.
	ElectionTimeoutMin int64 // milliseconds
	ElectionTimeoutMax int64 // milliseconds

	// HeartbeatInterval is how often a leader sends empty AppendEntries during
	// idle periods to prevent elections (§5.2).
	HeartbeatInterval int64 // milliseconds
}

// DefaultConfig returns timings in the range the paper suggests (§5.6 uses
// 150-300ms election timeouts). Tests override these with much smaller values so
// suites stay fast.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 300,
		HeartbeatInterval:  50,
	}
}
