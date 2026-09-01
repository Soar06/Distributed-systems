package demo

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/shard"
)

// Live resharding from the demo (theory: learn/READING_LIST.md §23).
//
// Distinct from Resize, and the difference is the whole point. Resize tears the
// cluster down and rebuilds it on a new topology — honest as a teaching control,
// but no data moves and nothing is served during the change. Reshard moves real
// accounts between two running Raft groups while both keep committing, which is
// the actual problem.

// ReshardAccount moves the arc that owns one account onto another shard.
//
// Account-oriented rather than vnode-oriented because that is the question a
// person actually has: "what happens if Vu's data moves?" The vnodes are derived
// from the account, so the demo never has to expose a ring index.
func (c *Cluster) ReshardAccount(account ledger.AccountID, to shard.ID) (shard.ReshardStatus, error) {
	c.mu.Lock()
	sc := c.sc
	c.mu.Unlock()

	from := sc.Coordinator.ShardFor(account)
	if from == to {
		return shard.ReshardStatus{}, fmt.Errorf("%s already lives on %s", account, to)
	}
	if _, ok := sc.Groups[to]; !ok {
		return shard.ReshardStatus{}, fmt.Errorf("unknown shard %s", to)
	}

	_, vnode := sc.Ring.LookupVNode(string(account))
	if vnode < 0 {
		return shard.ReshardStatus{}, fmt.Errorf("%s maps to no ring position", account)
	}

	id := fmt.Sprintf("move-%s-%d", account, time.Now().UnixNano())
	c.logEvent(Event{
		Kind: KindRaft, Account: string(account), Shard: string(from), Outcome: "ok",
		Text: fmt.Sprintf("resharding %s: %s -> %s (vnode %d) — writes to this range "+
			"are refused until cutover", account, from, to, vnode),
	})

	st, err := sc.Coordinator.Reshard(shard.ReshardPlan{
		ID: id, From: from, To: to, VNodes: []int{vnode},
		FreezeTimeout: 10 * time.Second,
	})
	if err != nil {
		c.logEvent(Event{
			Kind: KindRaft, Account: string(account), Shard: string(from), Outcome: "refused",
			Text: fmt.Sprintf("reshard of %s ABORTED in %s: %v — ownership never left %s, "+
				"so nothing was lost", account, st.Phase, err, from),
		})
		return st, err
	}

	c.logEvent(Event{
		Kind: KindRaft, Account: string(account), Shard: string(to), Outcome: "ok",
		Text: fmt.Sprintf("reshard done: %d account(s) moved %s -> %s, frozen for %v",
			st.Moved, from, to, st.Frozen.Round(time.Millisecond)),
	})
	return st, nil
}

// Migrations reports in-flight moves for the UI.
func (c *Cluster) Migrations() []shard.ReshardStatus {
	c.mu.Lock()
	sc := c.sc
	c.mu.Unlock()
	return sc.Coordinator.Migrations()
}
