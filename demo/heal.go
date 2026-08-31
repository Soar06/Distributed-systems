package demo

import (
	"fmt"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Re-replication policy: restoring a shard's replication factor after a machine
// holding one of its replicas dies.
//
// This file decides WHEN to heal and WHERE the new replica goes. The mechanism —
// creating the replica and running the §6 configuration change — is
// sim/rereplicate.go. Theory and sources: READING_LIST.md §21.
//
// THE PRECONDITION, AND WHY IT IS NOT NEGOTIABLE
//
// Healing requires the shard to still have a MAJORITY. A new replica is filled
// by the ordinary catch-up path (AppendEntries, or InstallSnapshot per §7), and
// both are driven by a leader, which cannot exist without a majority. So:
//
//	RF=3, lose 1 -> 2 alive -> majority holds -> leader exists -> heals
//	RF=3, lose 2 -> 1 alive -> no majority   -> no leader     -> REFUSED
//
// A shard below majority is not healable by any mechanism. Its data is not gone
// — it is on disk in the dead machines' logs — but it is unreadable until enough
// of them return. The tempting "fix" is to give the shard a fresh empty replica
// set on healthy machines so it becomes writable again and the dashboard turns
// green. That does not recover anything: it DISCARDS every balance in the shard
// and reports success. For a ledger that is the worst available outcome, and it
// is an AP choice inside a system that has committed to CP.
//
// So `Heal` refuses below-majority shards and says why, rather than papering
// over the loss.

// UnderReplicated describes one shard that has fewer live replicas than its
// configured replication factor.
type UnderReplicated struct {
	Shard shard.ID `json:"shard"`

	// Live is how many of this shard's replicas are on machines that are up.
	Live int `json:"live"`

	// Configured is the replication factor the shard should have.
	Configured int `json:"configured"`

	// Quorum is the number of replicas needed to commit.
	Quorum int `json:"quorum"`

	// Healable is whether a majority survives. When false, the shard must wait
	// for its machines to come back — see the note above.
	Healable bool `json:"healable"`

	// Dead lists the machines holding a replica that are currently down.
	Dead []raft.NodeID `json:"dead"`

	// Spares lists machines that hold no replica of this shard and are up, so
	// they can receive one.
	Spares []raft.NodeID `json:"spares"`
}

// UnderReplicatedShards reports every shard not currently at full replication.
//
// Read-only: it decides nothing and changes nothing, so the UI can show the
// cluster's health without triggering repairs as a side effect of rendering.
func (c *Cluster) UnderReplicatedShards() []UnderReplicated {
	c.mu.Lock()
	sc := c.sc
	rf := c.replicationFactor
	n := c.nodes
	crashed := make(map[raft.NodeID]bool, len(c.crashed))
	for k, v := range c.crashed {
		crashed[k] = v
	}
	c.mu.Unlock()

	var out []UnderReplicated
	for _, sid := range sc.Ring.Shards() {
		holders := sc.Holders(sid)

		var dead []raft.NodeID
		live := 0
		held := make(map[raft.NodeID]bool, len(holders))
		for _, h := range holders {
			held[h] = true
			if crashed[h] {
				dead = append(dead, h)
			} else {
				live++
			}
		}
		if len(dead) == 0 && len(holders) >= rf {
			continue // fully replicated
		}

		// Quorum is over the CONFIGURED replica set, not the live one. A shard with
		// 3 replicas needs 2 regardless of how many are currently up — computing it
		// from the survivors would let a single node declare itself a majority of
		// itself.
		quorum := len(holders)/2 + 1

		var spares []raft.NodeID
		for i := range n {
			id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
			if !held[id] && !crashed[id] {
				spares = append(spares, id)
			}
		}

		out = append(out, UnderReplicated{
			Shard:      sid,
			Live:       live,
			Configured: rf,
			Quorum:     quorum,
			Healable:   live >= quorum,
			Dead:       dead,
			Spares:     spares,
		})
	}
	return out
}

// HealResult records what one healing attempt did to one shard.
type HealResult struct {
	Shard   shard.ID    `json:"shard"`
	Added   raft.NodeID `json:"added,omitempty"`
	Removed raft.NodeID `json:"removed,omitempty"`
	Skipped string      `json:"skipped,omitempty"`
}

// Heal restores the replication factor of every shard that can safely be healed.
//
// Returns one result per under-replicated shard, including the ones it refused,
// so a caller can tell "nothing needed doing" apart from "something needed doing
// and could not be done".
func (c *Cluster) Heal() []HealResult {
	var results []HealResult

	for _, u := range c.UnderReplicatedShards() {
		// The refusal that keeps this honest. Below majority there is no leader and
		// therefore nothing to copy from; the only way to make the shard writable
		// again would be to invent empty state and silently zero its balances.
		if !u.Healable {
			msg := fmt.Sprintf(
				"below quorum (%d/%d live, need %d) - cannot re-replicate without a "+
					"majority to copy from; revive a machine holding it",
				u.Live, u.Configured, u.Quorum)
			results = append(results, HealResult{Shard: u.Shard, Skipped: msg})
			c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard), Outcome: "refused",
				Text: fmt.Sprintf("%s NOT healed: %s", u.Shard, msg)})
			continue
		}

		if len(u.Dead) == 0 {
			continue
		}
		if len(u.Spares) == 0 {
			msg := "no spare machine available to hold a new replica"
			results = append(results, HealResult{Shard: u.Shard, Skipped: msg})
			c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard), Outcome: "refused",
				Text: fmt.Sprintf("%s NOT healed: %s", u.Shard, msg)})
			continue
		}

		dead := u.Dead[0]
		spare := u.Spares[0]

		c.mu.Lock()
		sc := c.sc
		c.mu.Unlock()

		// ADD BEFORE REMOVE. Removing first would pass through a moment with the
		// configuration already shrunk to RF-1, making the shard less redundant
		// than it started; adding first means redundancy only ever goes up. §6's
		// one-change-at-a-time rule is respected because each call waits for its
		// own configuration entry.
		if err := sc.AddReplica(u.Shard, spare); err != nil {
			results = append(results, HealResult{Shard: u.Shard, Skipped: err.Error()})
			c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard), Outcome: "refused",
				Text: fmt.Sprintf("%s heal failed: %v", u.Shard, err)})
			continue
		}
		c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard), Node: string(spare), Outcome: "ok",
			Text: fmt.Sprintf("%s re-replicated onto %s (was %d/%d live)", u.Shard, spare, u.Live, u.Configured)})

		res := HealResult{Shard: u.Shard, Added: spare}

		// Retiring the dead replica is what actually returns the shard to RF. Left
		// in the configuration it would keep counting toward quorum forever: a
		// 4-replica group needs 3 to commit, so a permanently dead member makes the
		// shard LESS available than before healing.
		if err := sc.RemoveReplica(u.Shard, dead); err != nil {
			// The add already succeeded, so the shard is more redundant than it was.
			// Report the partial result rather than pretending the whole thing failed.
			res.Skipped = fmt.Sprintf("added %s but could not retire %s: %v", spare, dead, err)
			c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard),
				Text: fmt.Sprintf("%s: %s", u.Shard, res.Skipped)})
		} else {
			res.Removed = dead
			c.logEvent(Event{Kind: KindRaft, Shard: string(u.Shard), Node: string(dead), Outcome: "ok",
				Text: fmt.Sprintf("%s retired dead replica %s", u.Shard, dead)})
		}

		results = append(results, res)
	}

	return results
}
