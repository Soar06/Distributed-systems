package demo

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/sim"
)

// Control actions the UI can take: move money, kill nodes, run recovery.

// Open creates an account on whichever shard owns it.
func (c *Cluster) Open(account ledger.AccountID, amount ledger.Money) (ledger.Result, error) {
	if amount < 0 {
		return ledger.Result{}, fmt.Errorf("opening balance cannot be negative")
	}

	res, err := c.sc.Coordinator.Transfer(
		shard.TxID("open-"+string(account)),
		ledger.Command{
			Op: ledger.OpOpenAccount, IdempotencyKey: "open-" + string(account),
			To: account, Amount: amount,
		})

	if err == nil && res.OK {
		c.mu.Lock()
		c.accounts = append(c.accounts, account)
		c.mu.Unlock()
		c.logf("opened %s with %s on %s", account, amount, c.sc.Coordinator.ShardFor(account))
	} else {
		c.logf("open %s FAILED: %v %s", account, err, res.Err)
	}
	return res, err
}

// Transact performs a deposit, withdrawal, or transfer.
//
// The idempotency key is supplied by the caller so the UI can demonstrate a
// retry: firing the same key twice must move money once, which is the property
// that makes a timed-out request safe to reissue.
func (c *Cluster) Transact(op, key string, from, to ledger.AccountID, amount ledger.Money) (ledger.Result, error) {
	var lop ledger.Op
	switch op {
	case "deposit":
		lop = ledger.OpDeposit
	case "withdraw":
		lop = ledger.OpWithdraw
	case "transfer":
		lop = ledger.OpTransfer
	default:
		return ledger.Result{}, fmt.Errorf("unknown op %q", op)
	}
	if key == "" {
		// Mandatory, for the same reason as everywhere else in this system:
		// without it a retry cannot be told from a second request.
		return ledger.Result{}, fmt.Errorf("an idempotency key is required")
	}

	cmd := ledger.Command{
		Op: lop, IdempotencyKey: key, From: from, To: to, Amount: amount,
	}
	res, err := c.sc.Coordinator.Transfer(shard.TxID(key), cmd)

	switch {
	case err == nil && res.OK:
		c.logf("%s %s ok (key %s) balance=%s", op, amount, key, res.Balance)
	case err == shard.ErrInDoubt:
		// Surfaced as its own case, never as a plain failure: the outcome is
		// unknown and the entry may yet commit.
		c.logf("%s %s INDETERMINATE (key %s) — retry with the same key", op, amount, key)
	default:
		reason := res.Err
		if reason == "" && err != nil {
			reason = err.Error()
		}
		c.logf("%s %s refused (key %s): %s", op, amount, key, reason)
	}
	return res, err
}

// Kill takes a node off the network.
//
// Modelled as a network crash rather than by stopping the Raft server, so the
// node can be revived: raft.Server latches `stopped` permanently — deliberately,
// as the phantom-quorum fix — and a stopped server can never rejoin. Crashing the
// link reproduces what the demo is actually about (a machine that goes away and
// comes back) without weakening that latch.
func (c *Cluster) Kill(id raft.NodeID) error {
	g, ok := c.groupOf(id)
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}

	c.mu.Lock()
	if c.crashed[id] {
		c.mu.Unlock()
		return fmt.Errorf("%s is already down", id)
	}
	c.crashed[id] = true
	c.mu.Unlock()

	g.Net.Crash(id)
	c.logf("KILLED %s", id)
	return nil
}

// Revive brings a killed node back onto the network.
//
// What happens next is the interesting part, and it is not staged: the node
// rejoins, discovers the current term, and catches up from the leader — by
// AppendEntries if its log still overlaps, or by InstallSnapshot if the leader has
// compacted past it (§7).
func (c *Cluster) Revive(id raft.NodeID) error {
	g, ok := c.groupOf(id)
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}

	c.mu.Lock()
	if !c.crashed[id] {
		c.mu.Unlock()
		return fmt.Errorf("%s is not down", id)
	}
	delete(c.crashed, id)
	c.mu.Unlock()

	g.Net.Restore(id)
	c.logf("REVIVED %s — catching up from the leader", id)
	return nil
}

// RecoverInDoubt resolves blocked 2PC transactions.
//
// Exposed as a button because 2PC's blocking is inherent rather than a bug: a
// participant that voted yes and never heard back is holding customer funds and
// may not decide alone. Watching the in-doubt count sit at 1 and then clear is
// the visible form of that.
func (c *Cluster) RecoverInDoubt() (int, error) {
	n, err := c.sc.Coordinator.RecoverInDoubt()
	if err != nil {
		c.logf("recovery incomplete: %v", err)
	} else if n > 0 {
		c.logf("recovery resolved %d in-doubt transaction(s)", n)
	} else {
		c.logf("recovery found nothing in doubt")
	}
	return n, err
}

// WaitForLeaders waits until every shard has a leader again.
func (c *Cluster) WaitForLeaders(timeout time.Duration) bool {
	return c.sc.WaitForLeaders(timeout)
}

// groupOf finds the shard group hosting a node.
func (c *Cluster) groupOf(id raft.NodeID) (*sim.ShardGroup, bool) {
	for _, sid := range c.sc.Ring.Shards() {
		g := c.sc.Groups[sid]
		for _, nid := range g.IDs {
			if nid == id {
				return g, true
			}
		}
	}
	return nil, false
}
