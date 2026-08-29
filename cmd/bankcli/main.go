// Command bankcli drives a core-bank cluster from the shell.
//
// Exit codes matter here: this is an operational tool, and a banking tool that
// exits 0 on a failed transaction cannot be used in any script or recovery
// procedure. Every failure path exits non-zero.
//
//	bankcli <addr> status
//	bankcli <addr> tx <op> <key> <from> <amount> <to>
//	bankcli <addr> bal <account> <lin|stale>
//
// Amounts are integer minor units (cents): 50000 is 500.00.
package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"

	crpc "github.com/homura/core-bank/rpc"
)

const usage = `usage:
  bankcli <addr> status
  bankcli <addr> tx <op> <key> <from> <amount> <to>     op: open|deposit|withdraw|transfer
  bankcli <addr> bal <account> <lin|stale>

amounts are integer minor units (cents): 50000 = 500.00
use "" for an unused from/to field, e.g. tx open o1 "" 50000 alice`

func main() {
	// Explicit arity checks: indexing os.Args directly panicked on a typo.
	if len(os.Args) < 3 {
		fail(usage)
	}
	addr, cmd := os.Args[1], os.Args[2]

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fail("dial %s: %v", addr, err)
	}
	defer client.Close()

	switch cmd {
	case "status":
		doStatus(client)
	case "tx":
		if len(os.Args) != 8 {
			fail("tx needs 5 arguments\n\n%s", usage)
		}
		doTx(client, os.Args[3], os.Args[4], os.Args[5], os.Args[6], os.Args[7])
	case "bal":
		if len(os.Args) != 5 {
			fail("bal needs 2 arguments\n\n%s", usage)
		}
		doBal(client, os.Args[3], os.Args[4])
	default:
		fail("unknown command %q\n\n%s", cmd, usage)
	}
}

func doStatus(c *rpc.Client) {
	var r crpc.StatusReply
	if err := c.Call("Bank.Status", struct{}{}, &r); err != nil {
		fail("status: %v", err)
	}
	fmt.Printf("%-4s %-9s term=%-3d commit=%-3d applied=%-3d log=%-3d leader=%s balances=%v\n",
		r.ID, r.Role, r.Term, r.CommitIndex, r.LastApplied, r.LogLength, r.LeaderID, r.Balances)
}

func doTx(c *rpc.Client, op, key, from, amountArg, to string) {
	amount, err := strconv.ParseInt(amountArg, 10, 64)
	if err != nil {
		// Previously the Sscanf error was ignored, so "abc" silently became 0.
		fail("bad amount %q: want an integer number of cents", amountArg)
	}

	var r crpc.TxReply
	if err := c.Call("Bank.Submit", crpc.TxArgs{
		Op: op, IdempotencyKey: key, From: from, To: to, Amount: amount,
	}, &r); err != nil {
		fail("submit: %v", err)
	}

	fmt.Printf("ok=%v err=%q balance=%d notLeader=%v leaderAddr=%s conflict=%v indeterminate=%v\n",
		r.OK, r.Err, r.Balance, r.NotLeader, r.LeaderAddr, r.Conflict, r.Indeterminate)

	switch {
	case r.OK:
		return
	case r.NotLeader:
		// Distinct code: the caller should retry at LeaderAddr with the same key.
		os.Exit(3)
	case r.Indeterminate:
		// The outcome is UNKNOWN, not failed — the entry may still commit. A script
		// must retry with the SAME idempotency key, never reissue under a new one.
		os.Exit(4)
	case r.Conflict:
		// The key was used for a different request. Retrying unchanged cannot help.
		os.Exit(5)
	default:
		os.Exit(1)
	}
}

func doBal(c *rpc.Client, account, mode string) {
	if mode != "lin" && mode != "stale" {
		fail("read mode must be 'lin' or 'stale', got %q", mode)
	}
	var r crpc.BalanceReply
	if err := c.Call("Bank.Balance", crpc.BalanceArgs{
		Account: account, Linearizable: mode == "lin",
	}, &r); err != nil {
		fail("balance: %v", err)
	}

	fmt.Printf("balance=%d found=%v stale=%v err=%q\n", r.Balance, r.Found, r.Stale, r.Err)
	if r.Err != "" {
		os.Exit(1)
	}
	if !r.Found {
		os.Exit(2)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
