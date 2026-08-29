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
}

// TxReply is the outcome of a write.
type TxReply struct {
	OK      bool
	Err     string
	Balance int64

	// NotLeader and LeaderAddr implement the §8 redirect.
	NotLeader  bool
	LeaderAddr string
}

// BalanceArgs requests one account's balance.
type BalanceArgs struct {
	Account string

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
}

// NewClientService builds the client API.
func NewClientService(s *raft.Server, m *ledger.Machine, addrs map[raft.NodeID]string) *ClientService {
	return &ClientService{
		raftSrv:       s,
		machine:       m,
		addrs:         addrs,
		commitTimeout: 5 * time.Second,
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
	cmd, err := toCommand(args)
	if err != nil {
		reply.Err = err.Error()
		return nil
	}

	// A retry of an already-applied command returns the original result without
	// going through the log again.
	if res, ok := c.machine.Result(cmd.IdempotencyKey); ok {
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

	// Wait for the entry to be applied.
	deadline := time.Now().Add(c.commitTimeout)
	for time.Now().Before(deadline) {
		if c.raftSrv.LastApplied() >= idx {
			if res, ok := c.machine.Result(cmd.IdempotencyKey); ok {
				reply.OK, reply.Err, reply.Balance = res.OK, res.Err, int64(res.Balance)
				return nil
			}
		}
		// Losing leadership mid-flight means the entry may never commit here.
		if _, still := c.raftSrv.LeaderID(); !still {
			reply.NotLeader = true
			reply.LeaderAddr = c.leaderAddr()
			reply.Err = "leadership lost before commit; retry with the same idempotency key"
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Timing out does NOT mean the write failed — it may still commit. The client
	// must retry with the same idempotency key, which is exactly why that key
	// exists.
	reply.Err = "timed out waiting for commit; retry with the same idempotency key"
	return nil
}

// Balance handles a read.
func (c *ClientService) Balance(args BalanceArgs, reply *BalanceReply) error {
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
