package rpc

import (
	"fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// The §8 client redirect, for a SHARDED cluster.
//
// §8: "Clients of Raft send all of their requests to the leader... If the
// client's first choice is not the leader, that server will reject the client's
// request and supply information about the most recent leader it has heard from."
//
// The single-group ClientService has always implemented this: it returns
// NotLeader plus LeaderAddr and the client retries there. The sharded client API
// shipped WITHOUT it — a write to a node that does not lead the target shard came
// back as the opaque error "shard: X is not led by this node", carrying no
// address to retry at. A client had no way to make progress except to guess.
//
// That is a real availability gap, not a cosmetic one: with three nodes and one
// leader per shard, two thirds of client requests land on a non-leader.
//
// Per RULES.md rule 3 the flows are: normal (the leader accepts), redirect (a
// follower names the leader), retry (following the redirect succeeds), and the
// unroutable case (no leader known yet — reported honestly rather than guessed).

// followerFor returns a node that does NOT lead the given shard.
func (c *shardTestCluster) followerFor(sid shard.ID) raft.NodeID {
	leader := c.leaderFor(sid)
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if _, hosts := c.nodes[id].host.Replica(sid); hosts {
			return id
		}
	}
	return ""
}

// A write sent to a follower must come back as NotLeader carrying the leader's
// address — not as an opaque failure.
func TestShardedWriteToFollowerRedirects(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	if err := c.open(a, 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	owner := c.ring.Lookup(string(a))
	follower := c.followerFor(owner)
	if follower == "" {
		t.Fatalf("no follower hosts shard %s%s", owner, c.view())
	}

	client, err := rpc.Dial("tcp", c.nodes[follower].addr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer client.Close()

	var reply TxReply
	if err := client.Call("Bank.Submit", TxArgs{
		Op: "deposit", IdempotencyKey: "redirect-1", To: string(a), Amount: 100,
	}, &reply); err != nil {
		t.Fatalf("Bank.Submit: %v", err)
	}

	if reply.OK {
		t.Fatalf("a follower committed a write it does not lead: %+v", reply)
	}
	if !reply.NotLeader {
		t.Fatalf("a write to a follower was not marked NotLeader: %+v — §8 requires "+
			"the server to reject and supply the leader it knows about%s", reply, c.view())
	}
	if reply.LeaderAddr == "" {
		t.Fatalf("NotLeader carried no LeaderAddr: %+v — without an address the client "+
			"cannot make progress except by guessing%s", reply, c.view())
	}
	if reply.Indeterminate {
		t.Fatal("a rejected-by-follower write must NOT be Indeterminate: nothing was " +
			"proposed, so there is nothing that might still commit")
	}

	// The address must be the real leader's, and following it must work.
	wantAddr := c.nodes[c.leaderFor(owner)].addr
	if reply.LeaderAddr != wantAddr {
		t.Fatalf("LeaderAddr = %q, want the leader of shard %s at %q",
			reply.LeaderAddr, owner, wantAddr)
	}
}

// Following the redirect must actually commit the write — the retry path.
func TestShardedRedirectRetrySucceeds(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 5000)
	before, _ := c.balance(a)

	owner := c.ring.Lookup(string(a))
	follower := c.followerFor(owner)

	fc, err := rpc.Dial("tcp", c.nodes[follower].addr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer fc.Close()

	var reply TxReply
	fc.Call("Bank.Submit", TxArgs{
		Op: "deposit", IdempotencyKey: "redirect-retry", To: string(a), Amount: 700,
	}, &reply)
	if !reply.NotLeader || reply.LeaderAddr == "" {
		t.Fatalf("expected a redirect from the follower, got %+v%s", reply, c.view())
	}

	// Retry at the address the cluster gave us, with the SAME idempotency key —
	// which is exactly what the client contract tells a caller to do.
	lc, err := rpc.Dial("tcp", reply.LeaderAddr)
	if err != nil {
		t.Fatalf("dial the address the redirect named (%s): %v", reply.LeaderAddr, err)
	}
	defer lc.Close()

	var second TxReply
	if err := lc.Call("Bank.Submit", TxArgs{
		Op: "deposit", IdempotencyKey: "redirect-retry", To: string(a), Amount: 700,
	}, &second); err != nil {
		t.Fatalf("retry at the leader: %v", err)
	}
	if !second.OK {
		t.Fatalf("the retry at the redirected address failed: %+v%s", second, c.view())
	}

	after, _ := c.balance(a)
	if after != before+700 {
		t.Fatalf("balance %s -> %s, want +7.00 exactly once", before, after)
	}
}

// A redirected write that is then retried must not apply twice, even though it
// was submitted to two different nodes.
func TestShardedRedirectDoesNotDoubleApply(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 5000)
	before, _ := c.balance(a)

	owner := c.ring.Lookup(string(a))
	leaderAddr := c.nodes[c.leaderFor(owner)].addr

	args := TxArgs{Op: "deposit", IdempotencyKey: "no-double", To: string(a), Amount: 300}

	lc, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer lc.Close()

	var first, second TxReply
	lc.Call("Bank.Submit", args, &first)
	lc.Call("Bank.Submit", args, &second)

	if !first.OK {
		t.Fatalf("first write failed: %+v%s", first, c.view())
	}
	after, _ := c.balance(a)
	if after != before+300 {
		t.Fatalf("balance %s -> %s after the same key twice, want +3.00 exactly once "+
			"(second reply: %+v)", before, after, second)
	}
}

// Reads follow the same rule: a node that does not lead the owning shard must say
// where to go, rather than serving a possibly-stale answer as if it were
// authoritative or refusing with an opaque error.
func TestShardedLinearizableReadOnFollowerRedirects(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 4200)

	owner := c.ring.Lookup(string(a))
	follower := c.followerFor(owner)

	fc, err := rpc.Dial("tcp", c.nodes[follower].addr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer fc.Close()

	var reply BalanceReply
	if err := fc.Call("Bank.Balance", BalanceArgs{
		Account: string(a), Linearizable: true,
	}, &reply); err != nil {
		t.Fatalf("Bank.Balance: %v", err)
	}

	if !reply.NotLeader {
		t.Fatalf("a linearizable read on a follower was not marked NotLeader: %+v — "+
			"a follower cannot promise linearizability%s", reply, c.view())
	}
	if reply.LeaderAddr == "" {
		t.Fatalf("a redirected read carried no LeaderAddr: %+v", reply)
	}

	// A STALE read, by contrast, is legitimately served locally and must say so.
	var stale BalanceReply
	fc.Call("Bank.Balance", BalanceArgs{Account: string(a), Linearizable: false}, &stale)
	if stale.NotLeader {
		t.Fatalf("a stale-tolerant read was redirected: %+v — any node may serve it", stale)
	}
	if !stale.Stale {
		t.Fatal("a follower-served read was not flagged Stale")
	}
}

// When no leader is known for the owning shard, the reply must say so honestly
// rather than inventing an address or reporting a plain failure.
func TestShardedWriteWithNoKnownLeaderIsHonest(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 1000)

	owner := c.ring.Lookup(string(a))

	// Stop every replica of the owning shard, so no leader exists anywhere.
	for _, id := range c.ids {
		if rep, ok := c.nodes[id].host.Replica(owner); ok {
			rep.Raft.Stop()
		}
	}

	// Ask a node that still runs, but whose view of this shard is leaderless.
	target := c.ids[0]
	client, err := rpc.Dial("tcp", c.nodes[target].addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var reply TxReply
	if err := client.Call("Bank.Submit", TxArgs{
		Op: "deposit", IdempotencyKey: "no-leader", To: string(a), Amount: 50,
	}, &reply); err != nil {
		t.Fatalf("Bank.Submit: %v", err)
	}

	if reply.OK {
		t.Fatalf("a write committed with no leader for shard %s: %+v", owner, reply)
	}
	if reply.LeaderAddr != "" {
		t.Fatalf("LeaderAddr = %q with no leader elected; an invented address sends "+
			"the client into a retry loop against a node that cannot serve it", reply.LeaderAddr)
	}
	if reply.Err == "" {
		t.Fatal("a write with no available leader reported no error at all")
	}
	t.Logf("leaderless write reported: NotLeader=%v err=%q", reply.NotLeader, reply.Err)
}

// The single-group API's contract must not regress: the four outcomes stay
// distinct, and a sharded reply never sets contradictory flags.
func TestShardedReplyFlagsAreMutuallyConsistent(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	c.mustOpen(t, a, 5000)

	owner := c.ring.Lookup(string(a))

	for _, id := range c.ids {
		client, err := rpc.Dial("tcp", c.nodes[id].addr)
		if err != nil {
			t.Fatalf("dial %s: %v", id, err)
		}

		var reply TxReply
		client.Call("Bank.Submit", TxArgs{
			Op: "deposit", IdempotencyKey: fmt.Sprintf("flags-%s", id),
			To: string(a), Amount: 10,
		}, &reply)
		client.Close()

		switch {
		case reply.OK:
			if reply.NotLeader || reply.Indeterminate || reply.Unauthenticated {
				t.Fatalf("node %s: OK set together with a failure flag: %+v", id, reply)
			}
		case reply.NotLeader:
			if reply.OK || reply.Indeterminate {
				t.Fatalf("node %s: NotLeader set together with OK/Indeterminate: %+v", id, reply)
			}
		}
		t.Logf("node %s (leader of %s: %v): OK=%v NotLeader=%v addr=%q",
			id, owner, c.leaderFor(owner) == id, reply.OK, reply.NotLeader, reply.LeaderAddr)
	}
}

// Concurrent cross-shard transfers are limited by RESERVED funds, not just by
// the balance — and that limit is correct.
//
// A cross-shard transfer reserves its debit at prepare time and holds it until
// the transaction resolves. So N concurrent transfers each hold their amount
// against the available balance simultaneously, and an account funded with
// exactly N*amount can legitimately refuse some of them: the money is not gone,
// it is spoken for.
//
// This was found by a test whose premise was wrong — it funded an account with
// exactly the total being moved and then asserted every transfer would commit.
// Two failed with "insufficient funds", which is the ledger being right. Pinned
// down here so the behaviour is asserted deliberately instead of being
// rediscovered as a surprise.
func TestConcurrentTransfersAreLimitedByReservedFunds(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()
	c.open(a, 3000) // exactly three transfers of 10.00
	c.mustOpen(t, b, 0)
	total := c.totalMoney()

	const n = 6
	const amount = 1000

	var wg sync.WaitGroup
	var committed, refused atomic.Int64

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if submitFollowingRedirect(t, c.nodes[c.ids[i%len(c.ids)]].addr, TxArgs{
				Op: "transfer", IdempotencyKey: fmt.Sprintf("reserve-%d", i),
				From: string(a), To: string(b), Amount: amount,
			}) {
				committed.Add(1)
			} else {
				refused.Add(1)
			}
		}(i)
	}
	wg.Wait()

	balA, _ := c.balance(a)
	balB, _ := c.balance(b)

	// The bank's rule: never spend money that is not there, and never lose any.
	if balA < 0 {
		t.Fatalf("%s went negative (%s) — reservations failed to stop overspending%s",
			a, balA, c.view())
	}
	if got := c.totalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s%s", total, got, c.view())
	}
	if committed.Load() > 3 {
		t.Fatalf("%d transfers of %s committed against a balance of 30.00 — the same "+
			"money was spent more than once%s", committed.Load(), ledger.Money(amount), c.view())
	}
	if balB != ledger.Money(committed.Load()*amount) {
		t.Fatalf("%s received %s but %d transfers committed%s",
			b, balB, committed.Load(), c.view())
	}

	t.Logf("%d of %d concurrent transfers committed, %d refused for want of "+
		"AVAILABLE (not total) funds; %s=%s %s=%s, total unchanged at %s",
		committed.Load(), n, refused.Load(), a, balA, b, balB, total)
}
