package rpc

import (
	"strings"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Regression tests for the operational audit findings.
//
// Each of these passed the previous suite. They are the difference between a
// system that looks correct and one that is safe to deploy.

// C1: a shut-down node must not keep acking replication.
//
// Previously Server.Close closed only the listener, and raft.Server had no idea
// it had been stopped — so a node taken out for a rolling restart kept voting and
// acking over the leader's cached connection. The leader counted it toward the
// majority, committed, and told the customer their money moved, against a quorum
// that no longer existed. The entry survived on one disk.
func TestStoppedNodesCannotFormQuorum(t *testing.T) {
	c := startCluster(t, 3)
	leaderID, leaderAddr := c.waitLeader(3 * time.Second)

	var r TxReply
	call(t, leaderAddr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "acct", Amount: 10000}, &r)
	if !r.OK {
		t.Fatalf("setup write failed: %s", r.Err)
	}

	// Shut down both followers exactly as node/main.go does on SIGTERM.
	for _, id := range c.ids {
		if id != leaderID {
			c.servers[id].Stop()
			c.rpcs[id].Close()
		}
	}
	time.Sleep(300 * time.Millisecond)

	var phantom TxReply
	call(t, leaderAddr, "Bank.Submit",
		TxArgs{Op: "deposit", IdempotencyKey: "phantom", To: "acct", Amount: 5000}, &phantom)

	if phantom.OK {
		t.Fatal("PHANTOM QUORUM: the lone leader committed a write and acknowledged " +
			"it to the client with no live quorum")
	}
	if !phantom.Indeterminate {
		t.Errorf("a write that could not commit should be reported INDETERMINATE, "+
			"not a plain failure; got err=%q", phantom.Err)
	}
}

// C2: an idempotency key must be bound to the request that first used it.
//
// Previously a reused key returned the FIRST request's result for a completely
// different operation — a withdrawal from one account came back ok=true carrying
// another account's balance, so the client recorded a debit that never happened.
func TestIdempotencyKeyIsBoundToItsRequest(t *testing.T) {
	c := startCluster(t, 3)
	_, addr := c.waitLeader(3 * time.Second)

	for _, open := range []TxArgs{
		{Op: "open", IdempotencyKey: "k1", To: "alice", Amount: 100000},
		{Op: "open", IdempotencyKey: "k2", To: "bob", Amount: 50000},
	} {
		var r TxReply
		call(t, addr, "Bank.Submit", open, &r)
		if !r.OK {
			t.Fatalf("setup %v failed: %s", open, r.Err)
		}
	}

	var first TxReply
	call(t, addr, "Bank.Submit",
		TxArgs{Op: "withdraw", IdempotencyKey: "w1", From: "alice", Amount: 1000}, &first)
	if !first.OK {
		t.Fatalf("first withdrawal failed: %s", first.Err)
	}

	// Same key, different account, different amount.
	var replay TxReply
	call(t, addr, "Bank.Submit",
		TxArgs{Op: "withdraw", IdempotencyKey: "w1", From: "bob", Amount: 49999}, &replay)

	if replay.OK {
		t.Fatal("FORGED SUCCESS: a withdrawal from bob returned OK using alice's " +
			"cached result; bob's money never moved")
	}
	if !replay.Conflict {
		t.Errorf("expected Conflict to be set, got err=%q", replay.Err)
	}

	// And a genuine retry of the ORIGINAL request must still work.
	var genuine TxReply
	call(t, addr, "Bank.Submit",
		TxArgs{Op: "withdraw", IdempotencyKey: "w1", From: "alice", Amount: 1000}, &genuine)
	if !genuine.OK {
		t.Fatalf("a genuine retry was rejected: %s", genuine.Err)
	}
	if genuine.Balance != first.Balance {
		t.Fatalf("retry returned balance %d, want the original %d",
			genuine.Balance, first.Balance)
	}

	// Bob must be untouched throughout.
	var bal BalanceReply
	call(t, addr, "Bank.Balance", BalanceArgs{Account: "bob", Linearizable: true}, &bal)
	if bal.Balance != 50000 {
		t.Fatalf("bob = %d, want 50000 — the forged replay moved real money", bal.Balance)
	}
}

// C3: -peers parsing must reject input that silently corrupts quorum math.
func TestPeerListValidation(t *testing.T) {
	bad := []struct {
		in  string
		why string
	}{
		{"n1=127.0.0.1:9001,n1=127.0.0.1:9002,n2=127.0.0.1:9003",
			"duplicate id inflates quorum size while a real node is never contacted"},
		{"n1=,n2=127.0.0.1:9002", "empty address"},
		{"=127.0.0.1:9001,n2=127.0.0.1:9002", "empty node id"},
		{"n1=127.0.0.1,n2=127.0.0.1:9002", "address with no port"},
		{"n1=127.0.0.1:9001=oops,n2=127.0.0.1:9002", "more than one '='"},
		{"", "no peers at all"},
	}

	for _, tc := range bad {
		if _, _, err := ParsePeers(tc.in); err == nil {
			t.Errorf("accepted invalid -peers %q (%s)", tc.in, tc.why)
		}
	}

	// A well-formed list must still parse, with whitespace trimmed on both sides.
	addrs, ids, err := ParsePeers(" n1 = 127.0.0.1:9001 , n2=127.0.0.1:9002 ,n3=127.0.0.1:9003")
	if err != nil {
		t.Fatalf("rejected a valid peer list: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("parsed %d ids, want 3", len(ids))
	}
	if got := addrs[raft.NodeID("n1")]; got != "127.0.0.1:9001" {
		t.Fatalf("n1 address = %q, want 127.0.0.1:9001 (whitespace not trimmed)", got)
	}
	for id := range addrs {
		if strings.TrimSpace(string(id)) != string(id) {
			t.Fatalf("node id %q carries whitespace", id)
		}
	}
}
