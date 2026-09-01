package rpc

import (
	"errors"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// The bank-facing API for a sharded cluster (G1).
//
// The single-group ClientService talks to one ledger. This one routes by the
// consistent-hash ring: a write lands on whichever shard owns the account, and a
// transfer between accounts on different shards goes through 2PC. That routing is
// a pure function of (accountID, ring config), so every node independently agrees
// on who owns what with no coordination — the determinism requirement DESIGN.md
// §10 states.

// ShardStatusReply describes one shard replica hosted by this node.
type ShardStatusReply struct {
	ShardID     string
	Role        string
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LogLength   int
	LeaderID    string
	InDoubt     int
	Balances    map[string]int64
}

// ClusterStatusReply describes every shard replica on this node.
type ClusterStatusReply struct {
	NodeID string
	TLS    bool
	Shards []ShardStatusReply
}

// ShardClientService is the bank-facing RPC surface for a sharded cluster.
type ShardClientService struct {
	nodeID      string
	host        *ShardHost
	coordinator *shard.Coordinator
	ring        *shard.Ring
	auth        tokenAuth
	tlsOn       bool

	// admit applies backpressure and rate limiting. Nil disables both.
	admit *Admitter

	// addrs maps node ids to their client-reachable addresses, so a NotLeader
	// reply can name where to retry. §8: the server must "supply information about
	// the most recent leader it has heard from" — an id alone is not actionable.
	addrs map[raft.NodeID]string
}

// NewShardClientService builds the sharded client API.
//
// addrs may be nil, in which case a redirect still reports NotLeader but cannot
// name an address. That is strictly worse for the client, so node/ always passes
// the peer map.
func NewShardClientService(nodeID string, ring *shard.Ring, coord *shard.Coordinator,
	token string, addrs map[raft.NodeID]string) *ShardClientService {
	return &ShardClientService{
		nodeID:      nodeID,
		ring:        ring,
		coordinator: coord,
		auth:        tokenAuth{token: token},
		// Always present, even with no limits configured: it is what makes
		// graceful shutdown work on a node that never opted into backpressure.
		admit: NewAdmitter(Limits{}),
		addrs: addrs,
	}
}

// leaderAddrFor returns the client address of the node leading the shard that
// owns account, and whether one is known.
//
// Returns "" when no leader is known. An invented or stale-guessed address is
// worse than none: it sends the client into a retry loop against a node that
// cannot serve it, and the client cannot tell that from a slow leader.
func (c *ShardClientService) leaderAddrFor(account ledger.AccountID) (string, bool) {
	sid := c.coordinator.ShardFor(account)
	rep, hosted := c.host.Replica(sid)
	if !hosted {
		// This node holds no replica of the owning shard, so it has no view of that
		// group's leadership at all and must not speculate.
		return "", false
	}
	id, isLeader := rep.Raft.LeaderID()
	if isLeader || id == "" {
		return "", false
	}
	addr, ok := c.addrs[id]
	return addr, ok
}

// SetLimits attaches admission control. Must be called before serving.
func (c *ShardClientService) SetLimits(l Limits) {
	c.admit = NewAdmitter(l)
}

// Admitter exposes the admission controller, for metrics.
func (c *ShardClientService) Admitter() *Admitter { return c.admit }

// Attach links the service to the host once it exists. The host needs the service
// at construction and the service needs the host afterwards, so the cycle is
// broken here rather than by a partially-built object.
func (c *ShardClientService) Attach(h *ShardHost, tlsOn bool) {
	c.host, c.tlsOn = h, tlsOn
}

// Submit handles a write, routed to the owning shard.
//
// A cross-shard transfer goes through the 2PC coordinator; everything else is a
// single-group commit. The four-outcome client contract is preserved exactly:
// committed, not-leader, conflict, or indeterminate. A timed-out cross-shard
// transfer is INDETERMINATE, never a failure — the entry may still commit, and a
// client that records it as "did not happen" and reissues under a new key will
// double-send the money.
func (c *ShardClientService) Submit(args TxArgs, reply *TxReply) error {
	if !c.auth.check(args.Token) {
		reply.Unauthenticated = true
		reply.Err = ErrUnauthenticated
		return nil
	}

	// After authentication, before anything is proposed — see ClientService.Submit.
	release, rej, ok := c.admit.Admit(args.ClientID)
	if !ok {
		reply.Busy = true
		reply.Err = rej.Reason
		reply.RetryAfter = rej.RetryAfter
		return nil
	}
	defer release()

	cmd, err := toCommand(args)
	if err != nil {
		reply.Err = err.Error()
		return nil
	}
	if cmd.IdempotencyKey == "" {
		// Mandatory, and for the same reason as in the single-group API: without a
		// key a retry cannot be distinguished from a second request.
		reply.Err = "an idempotency key is required"
		return nil
	}

	res, err := c.coordinator.Transfer(shard.TxID(cmd.IdempotencyKey), cmd)
	switch {
	case err == nil:
		reply.OK, reply.Err, reply.Balance = res.OK, res.Err, int64(res.Balance)

	case errors.Is(err, shard.ErrNotLeader):
		// §8 redirect. Nothing was proposed, so this is emphatically NOT
		// Indeterminate: reporting "the entry may yet commit" for a write that was
		// never created tells the client to treat a non-event as an unknown
		// outcome, which is the most dangerous field in the whole contract.
		reply.NotLeader = true
		reply.Err = err.Error()
		if addr, ok := c.leaderAddrFor(routedAccount(cmd)); ok {
			reply.LeaderAddr = addr
		}

	case err == shard.ErrTxAborted:
		// Deterministic and final: the transaction did not commit anywhere.
		reply.OK, reply.Err = false, res.Err
		if reply.Err == "" {
			reply.Err = err.Error()
		}
	case err == shard.ErrInDoubt:
		// 2PC's inherent blocking, surfaced rather than papered over. The outcome
		// is genuinely unknown until the coordinator's replacement resolves it.
		reply.Indeterminate = true
		reply.Err = err.Error()
	default:
		reply.Indeterminate = true
		reply.Err = err.Error()
	}
	return nil
}

// routedAccount returns the account whose shard a command is routed to, matching
// Coordinator.Transfer's routing exactly. Kept beside the redirect logic so the
// address reported always names the group the request would actually have gone to.
func routedAccount(cmd ledger.Command) ledger.AccountID {
	switch cmd.Op {
	case ledger.OpOpenAccount, ledger.OpDeposit:
		return cmd.To
	default:
		return cmd.From
	}
}

// Balance reads an account from its owning shard.
func (c *ShardClientService) Balance(args BalanceArgs, reply *BalanceReply) error {
	if !c.auth.check(args.Token) {
		reply.Unauthenticated = true
		reply.Err = ErrUnauthenticated
		return nil
	}

	release, rej, ok := c.admit.Admit(args.ClientID)
	if !ok {
		reply.Busy = true
		reply.Err = rej.Reason
		reply.RetryAfter = rej.RetryAfter
		return nil
	}
	defer release()

	account := ledger.AccountID(args.Account)
	sid := c.coordinator.ShardFor(account)

	rep, hosted := c.host.Replica(sid)
	if !hosted {
		// This node holds no replica of the owning shard. Report it rather than
		// answering from nothing: a read that silently returns "not found" for an
		// account that exists elsewhere is worse than an error.
		reply.Err = "rpc: this node holds no replica of shard " + string(sid) +
			", which owns account " + args.Account
		return nil
	}

	if args.Linearizable && rep.Raft.Role() != raft.Leader {
		// A follower cannot promise linearizability: it may be behind, and §8's
		// ReadIndex confirmation is a leader-only mechanism. Redirect rather than
		// serving a stale value as though it were authoritative.
		reply.NotLeader = true
		reply.Err = raft.ErrNotLeader.Error()
		if addr, ok := c.leaderAddrFor(account); ok {
			reply.LeaderAddr = addr
		}
		return nil
	}

	bal, found := rep.Machine.State.Balance(account)
	reply.Balance, reply.Found = int64(bal), found

	// Reads here are served from the local replica, so they may be stale — and
	// the reply says so honestly rather than claiming a guarantee it does not
	// provide. A linearizable sharded read needs ReadIndex on the OWNING shard's
	// leader (§8), which this node is not necessarily hosting. Reporting Stale
	// when the guarantee cannot be met is the same discipline as returning
	// Indeterminate instead of a guessed outcome.
	reply.Stale = !args.Linearizable || rep.Raft.Role() != raft.Leader
	return nil
}

// Status reports every shard replica this node hosts, for the dashboard.
func (c *ShardClientService) Status(_ struct{}, reply *ClusterStatusReply) error {
	reply.NodeID = c.nodeID
	reply.TLS = c.tlsOn

	for _, sid := range c.host.ShardIDs() {
		rep, _ := c.host.Replica(sid)
		leader, _ := rep.Raft.LeaderID()

		st := ShardStatusReply{
			ShardID:     string(sid),
			Role:        rep.Raft.Role().String(),
			Term:        uint64(rep.Raft.CurrentTerm()),
			CommitIndex: uint64(rep.Raft.CommitIndex()),
			LastApplied: uint64(rep.Raft.LastApplied()),
			LogLength:   len(rep.Raft.LogEntries()) - 1,
			LeaderID:    string(leader),
			InDoubt:     len(rep.Machine.InDoubt()),
			Balances:    make(map[string]int64),
		}
		for id, bal := range rep.Machine.State.Balances() {
			st.Balances[string(id)] = int64(bal)
		}
		reply.Shards = append(reply.Shards, st)
	}
	return nil
}
