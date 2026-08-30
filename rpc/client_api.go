package rpc

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// The client-facing API: how a bank app talks to the cluster.
//
// §8: "Clients of Raft send all of their requests to the leader... If the client's
// first choice is not the leader, that server will reject the client's request and
// supply information about the most recent leader it has heard from."
//
// That redirect is implemented here: a non-leader returns NotLeader plus the
// leader's address, and the client retries there.

// TxArgs is a client's write request.
type TxArgs struct {
	Op             string // "deposit" | "withdraw" | "transfer" | "open"
	IdempotencyKey string
	From, To       string
	Amount         int64 // minor units (cents)

	// Token authenticates the caller when the node is configured with a client
	// token. Checked before the command is proposed — an unauthenticated write
	// must never reach the log, since a rejected entry that already replicated is
	// indistinguishable to the ledger from an accepted one.
	Token string

	// ClientID identifies the caller for per-client rate limiting. Callers that
	// omit it share one bucket rather than being exempt, since exempting them
	// would make the limit bypassable by leaving the field empty.
	ClientID string
}

// TxReply is the outcome of a write.
type TxReply struct {
	OK      bool
	Err     string
	Balance int64

	// NotLeader and LeaderAddr implement the §8 redirect.
	NotLeader  bool
	LeaderAddr string

	// Conflict means the idempotency key was already used for a DIFFERENT
	// request. This is neither a success nor an ordinary failure of the new
	// request: the key cannot be used here, and the caller must not retry it
	// unchanged.
	Conflict bool

	// Indeterminate means the outcome is UNKNOWN, not failed: the entry may yet
	// commit. A client must retry with the same idempotency key rather than
	// recording a failure — treating this as "did not happen" is how a real
	// transfer gets double-sent or wrongly reversed.
	Indeterminate bool

	// Unauthenticated means the caller presented no valid token. Distinct from an
	// ordinary failure: nothing was proposed, so there is nothing to retry until
	// the caller has credentials. Never set together with Indeterminate.
	Unauthenticated bool

	// Busy means the request was SHED: the node is at its in-flight limit, or the
	// caller is over its rate budget. Nothing was proposed.
	//
	// Distinct from Indeterminate, and the distinction is the whole point of
	// bounding the queue. Indeterminate means "the entry may yet commit, retry with
	// the same key". Busy means "no entry exists, retry whenever you like". An
	// unbounded queue turns every overloaded request into the first, more dangerous
	// answer; bounding it turns them into the second.
	Busy bool

	// RetryAfter advises how long to wait before retrying a Busy reply. Advisory,
	// but it is what stops a shed client from retrying immediately and making the
	// overload worse.
	RetryAfter time.Duration
}

// BalanceArgs requests one account's balance.
type BalanceArgs struct {
	Account string

	// Token authenticates the caller. Reads are authenticated too: every balance
	// in the bank is readable from this endpoint.
	Token string

	// ClientID identifies the caller for per-client rate limiting.
	ClientID string

	// Linearizable requests a read that is guaranteed not to be stale, at the
	// cost of a round trip to a majority (§8). A false value permits a local read
	// that may be stale — useful for showing the difference, and the basis of
	// follower reads in LATER.md.
	Linearizable bool
}

// BalanceReply carries a balance.
type BalanceReply struct {
	Balance int64
	Found   bool
	Err     string

	NotLeader  bool
	LeaderAddr string

	// Stale reports whether the read bypassed the linearizability check.
	Stale bool

	// Unauthenticated means the caller presented no valid token.
	Unauthenticated bool

	// Busy means the read was shed. Nothing was read; retrying is safe.
	Busy       bool
	RetryAfter time.Duration
}

// StatusReply describes a node, for the cluster dashboard.
type StatusReply struct {
	ID          string
	Role        string
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LogLength   int
	LeaderID    string
	Balances    map[string]int64
}

// ClientService is the bank-facing RPC surface.
type ClientService struct {
	raftSrv *raft.Server
	machine *ledger.Machine
	addrs   map[raft.NodeID]string

	// commitTimeout bounds how long a write waits for its entry to be applied.
	commitTimeout time.Duration

	// auth checks client bearer tokens. The zero value permits everything, which
	// keeps local development and the existing tests working.
	auth tokenAuth

	// admit applies backpressure and rate limiting. Nil disables both.
	admit *Admitter
}

// SetLimits attaches admission control. Must be called before serving.
func (c *ClientService) SetLimits(l Limits) {
	c.admit = NewAdmitter(l)
}

// Admitter exposes the admission controller, for metrics.
func (c *ClientService) Admitter() *Admitter { return c.admit }

// NewClientService builds the client API with no client authentication.
func NewClientService(s *raft.Server, m *ledger.Machine, addrs map[raft.NodeID]string) *ClientService {
	return NewClientServiceAuth(s, m, addrs, "")
}

// NewClientServiceAuth builds the client API requiring the given bearer token.
// An empty token disables client authentication.
func NewClientServiceAuth(s *raft.Server, m *ledger.Machine, addrs map[raft.NodeID]string, token string) *ClientService {
	return &ClientService{
		raftSrv:       s,
		machine:       m,
		addrs:         addrs,
		commitTimeout: 5 * time.Second,
		auth:          tokenAuth{token: token},
		// Always present, even with no limits configured: it is what makes
		// graceful shutdown work on a node that never opted into backpressure.
		admit: NewAdmitter(Limits{}),
	}
}

// leaderAddr returns the address of the leader this node knows about.
func (c *ClientService) leaderAddr() string {
	id, _ := c.raftSrv.LeaderID()
	if id == "" {
		return ""
	}
	return c.addrs[id]
}

// Submit handles a write.
//
// The write is only acknowledged after the entry has been COMMITTED and APPLIED
// (Figure 2, Leaders: "respond after entry applied to state machine"). Replying
// earlier would tell the client their money moved before the cluster agreed it
// did.
func (c *ClientService) Submit(args TxArgs, reply *TxReply) error {
	// Checked before anything else: an unauthenticated write must not reach the
	// log. Saltzer & Schroeder's complete mediation — every access checked, with
	// no path that skips the check (learn/READING_LIST.md §13).
	if !c.auth.check(args.Token) {
		reply.Unauthenticated = true
		reply.Err = ErrUnauthenticated
		return nil
	}

	// Admission control runs AFTER authentication and BEFORE anything is
	// proposed. After auth, so an unauthenticated caller cannot consume a slot it
	// was never entitled to; before proposing, so a shed request leaves no entry
	// and is safe to retry.
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

	// A retry of an already-applied command returns the original result without
	// going through the log again — but ONLY if the key was first used for this
	// same request.
	//
	// Without that check, reusing a key returned the FIRST request's result for a
	// completely different operation: a withdrawal from one account came back
	// ok=true carrying another account's balance, so the client recorded a debit
	// that never happened. A false success is worse than a false failure.
	if res, ok, err := c.machine.ResultFor(cmd); err != nil {
		reply.Err = err.Error()
		reply.Conflict = true
		return nil
	} else if ok {
		reply.OK, reply.Err, reply.Balance = res.OK, res.Err, int64(res.Balance)
		return nil
	}

	idx, _, isLeader := c.raftSrv.Submit(cmd.Encode())
	if !isLeader {
		reply.NotLeader = true
		reply.LeaderAddr = c.leaderAddr()
		reply.Err = raft.ErrNotLeader.Error()
		return nil
	}

	// Wait for the entry to be applied, event-driven rather than polling.
	//
	// The previous version busy-waited at 2ms, taking the raft mutex twice per
	// iteration for up to the full timeout. Under concurrent load that put client
	// goroutines in direct contention with the consensus loop for the same lock,
	// so client traffic degraded consensus itself.
	applied := c.raftSrv.WaitApplied(idx)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(c.commitTimeout)

	for {
		select {
		case <-applied:
			if res, ok := c.machine.Result(cmd.IdempotencyKey); ok {
				reply.OK, reply.Err, reply.Balance = res.OK, res.Err, int64(res.Balance)
				return nil
			}
			// Applied but no result recorded: the entry at this index was replaced
			// by a different leader's entry. Indeterminate, same as a timeout.
			reply.Indeterminate = true
			reply.Err = "entry was superseded before it committed; retry with the same idempotency key"
			return nil

		case <-ticker.C:
			// Losing leadership means our entry may never commit here. Checked on a
			// slow tick rather than a hot loop.
			if _, still := c.raftSrv.LeaderID(); !still {
				reply.NotLeader = true
				reply.LeaderAddr = c.leaderAddr()
				reply.Indeterminate = true
				reply.Err = "leadership lost before commit; retry with the same idempotency key"
				return nil
			}

		case <-deadline:
			// Timing out does NOT mean the write failed — the entry is in the
			// leader's log and may still commit. Flagged as INDETERMINATE so a
			// client does not record it as a failure; retrying with the same
			// idempotency key is safe and is exactly why the key exists.
			reply.Indeterminate = true
			reply.Err = "timed out waiting for commit; retry with the same idempotency key"
			return nil
		}
	}
}

// Balance handles a read.
func (c *ClientService) Balance(args BalanceArgs, reply *BalanceReply) error {
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

	if !args.Linearizable {
		// Stale-tolerant local read. Any node can serve it, including a follower
		// that is behind. This is the read path LATER.md's follower reads build on.
		bal, found := c.machine.State.Balance(ledger.AccountID(args.Account))
		reply.Balance, reply.Found, reply.Stale = int64(bal), found, true
		return nil
	}

	// Linearizable: confirm leadership with a majority first (§8).
	err := c.raftSrv.LinearizableRead(2*time.Second, func(_ raft.StateMachine) {
		bal, found := c.machine.State.Balance(ledger.AccountID(args.Account))
		reply.Balance, reply.Found = int64(bal), found
	})
	if err != nil {
		if err == raft.ErrNotLeader {
			reply.NotLeader = true
			reply.LeaderAddr = c.leaderAddr()
		}
		reply.Err = err.Error()
	}
	return nil
}

// Status reports node state, for the cluster dashboard.
func (c *ClientService) Status(_ struct{}, reply *StatusReply) error {
	leader, _ := c.raftSrv.LeaderID()

	reply.ID = string(c.raftSrv.ID())
	reply.Role = c.raftSrv.Role().String()
	reply.Term = uint64(c.raftSrv.CurrentTerm())
	reply.CommitIndex = uint64(c.raftSrv.CommitIndex())
	reply.LastApplied = uint64(c.raftSrv.LastApplied())
	reply.LogLength = len(c.raftSrv.LogEntries()) - 1
	reply.LeaderID = string(leader)

	reply.Balances = make(map[string]int64)
	for id, bal := range c.machine.State.Balances() {
		reply.Balances[string(id)] = int64(bal)
	}
	return nil
}

// toCommand converts a wire request into a ledger command.
func toCommand(args TxArgs) (ledger.Command, error) {
	var op ledger.Op
	switch args.Op {
	case "deposit":
		op = ledger.OpDeposit
	case "withdraw":
		op = ledger.OpWithdraw
	case "transfer":
		op = ledger.OpTransfer
	case "open":
		op = ledger.OpOpenAccount
	default:
		return ledger.Command{}, fmt.Errorf("unknown op %q", args.Op)
	}

	return ledger.Command{
		Op:             op,
		IdempotencyKey: args.IdempotencyKey,
		From:           ledger.AccountID(args.From),
		To:             ledger.AccountID(args.To),
		Amount:         ledger.Money(args.Amount),
	}, nil
}
