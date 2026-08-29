package main

import (
	"fmt"
	"net/rpc"
	"os"

	crpc "github.com/homura/core-bank/rpc"
)

func main() {
	addr := os.Args[1]
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println("DIAL ERROR:", err)
		os.Exit(1)
	}
	defer c.Close()

	switch os.Args[2] {
	case "status":
		var r crpc.StatusReply
		if err := c.Call("Bank.Status", struct{}{}, &r); err != nil {
			fmt.Println("ERR:", err)
			return
		}
		fmt.Printf("%-4s %-9s term=%-3d commit=%-3d applied=%-3d log=%-3d leader=%s balances=%v\n",
			r.ID, r.Role, r.Term, r.CommitIndex, r.LastApplied, r.LogLength, r.LeaderID, r.Balances)
	case "tx":
		var amt int64
		fmt.Sscanf(os.Args[6], "%d", &amt)
		var r crpc.TxReply
		if err := c.Call("Bank.Submit", crpc.TxArgs{
			Op: os.Args[3], IdempotencyKey: os.Args[4], From: os.Args[5], To: os.Args[7], Amount: amt,
		}, &r); err != nil {
			fmt.Println("ERR:", err)
			return
		}
		fmt.Printf("ok=%v err=%q balance=%d notLeader=%v leaderAddr=%s\n",
			r.OK, r.Err, r.Balance, r.NotLeader, r.LeaderAddr)
	case "bal":
		var r crpc.BalanceReply
		if err := c.Call("Bank.Balance", crpc.BalanceArgs{Account: os.Args[3], Linearizable: os.Args[4] == "lin"}, &r); err != nil {
			fmt.Println("ERR:", err)
			return
		}
		fmt.Printf("balance=%d found=%v stale=%v err=%q\n", r.Balance, r.Found, r.Stale, r.Err)
	}
}
