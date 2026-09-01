package rpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"time"

	"github.com/homura/core-bank/raft"
)

// Cluster membership over the wire (Raft §6).
//
// AddServer and RemoveServer were implemented and tested well before this file
// existed, but nothing called them from outside the process, so a running cluster
// could not actually be reconfigured — the README listed it as the top remaining
// gap. This exposes them, and nothing more: the safety rules stay where they
// belong, inside raft/membership.go.
//
// WHY THIS IS THIN ON PURPOSE
//
// Every rule that makes a configuration change safe is already enforced by the
// consensus layer:
//
//   - leader-only, because a config change is an ordinary log entry;
//   - one change at a time (differsByOne), so overlapping changes are refused
//     rather than queued;
//   - the change takes effect on APPEND, not on commit, because waiting for
//     commit can deadlock — the commit may need the new configuration's majority;
//   - a leader removing itself keeps serving until the change commits.
//
// Re-checking any of that here would duplicate the rules in a place that can
// drift out of step with them. This layer's whole job is to carry the call and
// report the answer honestly.
//
// THE ONE THING THIS LAYER MUST GET RIGHT
//
// A configuration entry that is appended but not committed is INDETERMINATE, in
// exactly the sense the client contract already uses for writes: the change may
// still take effect. Reporting that as a plain failure invites the operator to
// retry, and a retried add can produce a cluster that grew twice. So this waits
// for the entry to commit and distinguishes the three outcomes — applied,
// rejected, unknown — rather than collapsing them into ok/error.

// ErrConfigUnknown means a configuration entry was appended but its commit could
// not be confirmed in time. The change may still take effect.
//
// The operator must NOT blindly retry: re-issuing an add that later commits would
// attempt to add a server already in the configuration, which raft correctly
// rejects — but the confusion is avoidable by reading the current configuration
// first. CurrentConfiguration is the safe way to find out what actually happened.
var ErrConfigUnknown = errors.New("rpc: configuration change appended but commit unconfirmed")

// AdminArgs names the server to add or remove.
type AdminArgs struct {
	NodeID raft.NodeID

	// Timeout bounds the wait for the change to commit. Zero uses a default.
	Timeout time.Duration
}

// AdminReply reports what happened to a configuration change.
type AdminReply struct {
	// OK means the change was appended AND committed.
	OK bool

	// Indeterminate means the entry was appended but the commit was not confirmed
	// within the timeout. Distinct from Err: the change may yet take effect, so
	// the correct response is to read the configuration, not to retry.
	Indeterminate bool

	// NotLeader means nothing was proposed. §8's redirect applies: retry against
	// the leader. Safe and final — no entry exists.
	NotLeader bool
	LeaderID  raft.NodeID

	// Index is the log index of the configuration entry, when one was appended.
	Index raft.Index

	// Servers is the configuration after the change, for a committed change.
	Servers []raft.NodeID

	Err string
}

// AdminService exposes membership changes for one Raft group.
type AdminService struct {
	srv *raft.Server

	// commitTimeout bounds the wait for a configuration entry to commit when the
	// caller does not specify one.
	commitTimeout time.Duration
}

// NewAdminService builds the membership endpoint for a Raft server.
func NewAdminService(srv *raft.Server, commitTimeout time.Duration) *AdminService {
	if commitTimeout <= 0 {
		commitTimeout = 3 * time.Second
	}
	return &AdminService{srv: srv, commitTimeout: commitTimeout}
}

// AddServer adds one server to the cluster configuration.
func (a *AdminService) AddServer(args AdminArgs, reply *AdminReply) error {
	*reply = a.change(args, a.srv.AddServer)
	return nil
}

// RemoveServer removes one server from the cluster configuration.
func (a *AdminService) RemoveServer(args AdminArgs, reply *AdminReply) error {
	*reply = a.change(args, a.srv.RemoveServer)
	return nil
}

// Configuration returns the group's current membership.
//
// Deliberately available on any node, not just the leader: after an
// indeterminate change this is how an operator finds out what actually happened,
// and requiring a leader would make it unavailable in exactly the situation it
// is needed. A follower's view can be a moment stale, which is acceptable for an
// answer that is already about the past.
func (a *AdminService) Configuration(args AdminArgs, reply *AdminReply) error {
	cfg := a.srv.CurrentConfiguration()
	*reply = AdminReply{OK: true, Servers: cfg.Servers}
	if id, ok := a.srv.LeaderID(); ok {
		reply.LeaderID = id
	}
	return nil
}

// change runs one configuration mutation and reports the outcome faithfully.
func (a *AdminService) change(args AdminArgs, mutate func(raft.NodeID) (raft.Index, error)) AdminReply {
	if args.NodeID == "" {
		return AdminReply{Err: "rpc: no node id given"}
	}

	idx, err := mutate(args.NodeID)
	if err != nil {
		// Nothing was appended. §8: tell the caller where the leader is so the
		// retry goes somewhere useful, and mark it NotLeader rather than a generic
		// failure — this is safe to retry, unlike the indeterminate case below.
		if errors.Is(err, raft.ErrNotLeader) {
			r := AdminReply{NotLeader: true, Err: err.Error()}
			if id, ok := a.srv.LeaderID(); ok {
				r.LeaderID = id
			}
			return r
		}
		return AdminReply{Err: err.Error()}
	}

	timeout := args.Timeout
	if timeout <= 0 {
		timeout = a.commitTimeout
	}

	// The entry exists in the leader's log now. Whether it COMMITS is a separate
	// question, and the difference is what the operator needs.
	select {
	case <-a.srv.WaitApplied(idx):
		cfg := a.srv.CurrentConfiguration()
		return AdminReply{OK: true, Index: idx, Servers: cfg.Servers}

	case <-time.After(timeout):
		return AdminReply{
			Indeterminate: true,
			Index:         idx,
			Err: fmt.Sprintf("%v (index %d); read Configuration to see whether it took effect",
				ErrConfigUnknown, idx),
		}
	}
}

// AdminClient calls the membership API on a remote node.
//
// Separate from the bank client because these are operator actions, not client
// traffic: they are rare, they are privileged, and conflating them would put
// cluster reconfiguration behind the same admission control that sheds load.
type AdminClient struct {
	addr string
	tc   TLSConfig
}

// NewAdminClient addresses one node's Admin service.
func NewAdminClient(addr string, tc TLSConfig) *AdminClient {
	return &AdminClient{addr: addr, tc: tc}
}

// AddServer asks the node to add id to the configuration.
func (c *AdminClient) AddServer(id raft.NodeID, timeout time.Duration) (AdminReply, error) {
	return c.call("Admin.AddServer", AdminArgs{NodeID: id, Timeout: timeout})
}

// RemoveServer asks the node to remove id from the configuration.
func (c *AdminClient) RemoveServer(id raft.NodeID, timeout time.Duration) (AdminReply, error) {
	return c.call("Admin.RemoveServer", AdminArgs{NodeID: id, Timeout: timeout})
}

// Configuration reads the node's current membership.
func (c *AdminClient) Configuration() (AdminReply, error) {
	return c.call("Admin.Configuration", AdminArgs{})
}

func (c *AdminClient) call(method string, args AdminArgs) (AdminReply, error) {
	// A fresh connection per call rather than a pool. Membership changes are rare
	// operator actions, so connection reuse buys nothing, and a long-lived admin
	// connection is one more thing holding a file descriptor open against a node
	// that may be about to be removed from the cluster.
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return AdminReply{}, fmt.Errorf("rpc: dial %s: %w", c.addr, err)
	}

	if c.tc.Enabled() {
		cfg, err := c.tc.clientTLS()
		if err != nil {
			conn.Close()
			return AdminReply{}, err
		}
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return AdminReply{}, fmt.Errorf("rpc: tls handshake with %s: %w", c.addr, err)
		}
		conn = tlsConn
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	var reply AdminReply
	if err := client.Call(method, args, &reply); err != nil {
		return AdminReply{}, fmt.Errorf("rpc: %s: %w", method, err)
	}
	return reply, nil
}
