package demo

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// Watching consensus so the UI can show WHY leadership moved.
//
// Every other event in this demo is caused by something a user did: a click, a
// transaction, a kill. Elections are not. They are the cluster reacting to
// failure on its own, and without a watcher they happen silently — the dashboard
// simply shows a different leader on the next frame, with no record that an
// election occurred, which term it was, or which machine won.
//
// That silence is exactly what makes re-election hard to learn from. The point of
// §5.2 is that a follower stops hearing heartbeats, times out, and campaigns; the
// UI should be able to show that sequence rather than only its outcome.
//
// This is a passive OBSERVER. It reads role and term through the same public
// accessors the snapshot already uses, and never writes Raft state. Polling
// rather than a callback keeps raft/ free of demo concerns — a notification hook
// inside the consensus loop would put UI wiring on the critical path, which is
// the observer effect this project has already measured twice (§20).

// leaderState is the last observed leadership of one shard.
type leaderState struct {
	leader raft.NodeID
	term   raft.Term

	// hadQuorum is tracked because a shard can lose the ability to commit without
	// any leadership change — see sampleLeadership.
	hadQuorum bool
}

// watchRaft polls each shard's leadership and records changes as events.
//
// The interval is short relative to an election timeout (100-200ms) so a
// leadership change is caught in the term it happened, but not so short that
// polling itself becomes load.
func (c *Cluster) watchRaft(every time.Duration, done <-chan struct{}) {
	seen := make(map[shard.ID]leaderState)

	// Sample immediately so the healthy baseline is recorded without waiting a
	// full tick. Without this, a failure injected in the first interval has no
	// prior state to be a transition FROM, and is correctly but unhelpfully
	// silent.
	c.sampleLeadership(seen)
	c.markSampled()

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			c.sampleLeadership(seen)
		}
	}
}

// sampleLeadership compares current leadership against the last observation and
// emits an event for anything that changed.
func (c *Cluster) sampleLeadership(seen map[shard.ID]leaderState) {
	c.mu.Lock()
	sc := c.sc
	c.mu.Unlock()

	for _, sid := range sc.Ring.Shards() {
		g, ok := sc.Groups[sid]
		if !ok {
			continue
		}

		// Find the current leader and its term by asking each replica for its own
		// view. Two servers can report Leader at once — a stale one in an old term
		// and the real one in a new term — so the highest term wins, the same rule
		// Raft itself uses.
		var (
			leader raft.NodeID
			term   raft.Term
		)
		for _, nid := range sc.Holders(sid) {
			srv := g.Nodes[nid]
			if srv == nil {
				continue
			}
			t := srv.CurrentTerm()
			if srv.Role() == raft.Leader && (leader == "" || t > term) {
				leader, term = nid, t
			}
		}

		// Quorum is tracked alongside leadership because losing it is invisible
		// otherwise. A partitioned leader keeps reporting Leader in the same term —
		// it has not heard a higher one and never will while isolated — so a shard
		// can stop committing with NO leadership change at all. Watching only for
		// leader/term transitions therefore produced an empty log for exactly the
		// failure the demo is about.
		live := 0
		for _, nid := range sc.Holders(sid) {
			if !c.isCrashed(nid) {
				live++
			}
		}
		quorum := len(sc.Holders(sid))/2 + 1
		hasQuorum := live >= quorum

		prev, had := seen[sid]
		if !had {
			seen[sid] = leaderState{leader: leader, term: term, hadQuorum: hasQuorum}
			continue
		}

		if prev.hadQuorum != hasQuorum {
			if hasQuorum {
				c.logEvent(Event{
					Kind: KindRaft, Shard: string(sid), Outcome: "ok",
					Text: fmt.Sprintf("%s REGAINED quorum (%d/%d live, needs %d) — writes can commit again",
						sid, live, len(sc.Holders(sid)), quorum),
				})
			} else {
				c.logEvent(Event{
					Kind: KindRaft, Shard: string(sid), Outcome: "refused",
					Text: fmt.Sprintf("%s LOST quorum (%d/%d live, needs %d) — writes are refused; "+
						"%s may still call itself leader but cannot commit",
						sid, live, len(sc.Holders(sid)), quorum, leader),
				})
			}
		}

		if prev.leader == leader && prev.term == term {
			seen[sid] = leaderState{leader: leader, term: term, hadQuorum: hasQuorum}
			continue
		}
		seen[sid] = leaderState{leader: leader, term: term, hadQuorum: hasQuorum}

		switch {
		case leader == "" && prev.leader != "":
			// Losing a leader without gaining one is the visible start of an
			// election, and the state a shard sits in while it cannot commit.
			c.logEvent(Event{
				Kind: KindRaft, Shard: string(sid), Node: string(prev.leader),
				Outcome: "refused",
				Text: fmt.Sprintf("%s LOST its leader (%s, term %d) — campaigning; "+
					"no writes can commit until one is elected",
					sid, prev.leader, prev.term),
			})

		case leader != "" && prev.leader == "":
			c.logEvent(Event{
				Kind: KindRaft, Shard: string(sid), Node: string(leader), Outcome: "ok",
				Text: fmt.Sprintf("%s ELECTED %s as leader in term %d — writes can commit again",
					sid, leader, term),
			})

		case leader != prev.leader:
			c.logEvent(Event{
				Kind: KindRaft, Shard: string(sid), Node: string(leader), Outcome: "ok",
				Text: fmt.Sprintf("%s leadership moved %s → %s (term %d → %d)",
					sid, prev.leader, leader, prev.term, term),
			})

		default:
			// Same leader, higher term: it won a fresh election rather than simply
			// continuing. Worth recording, because a term bump with no leader change
			// means something disrupted the cluster and the incumbent won anyway.
			c.logEvent(Event{
				Kind: KindRaft, Shard: string(sid), Node: string(leader),
				Text: fmt.Sprintf("%s re-elected %s in a new term (%d → %d)",
					sid, leader, prev.term, term),
			})
		}
	}
}

// isCrashed reports whether the operator has killed this machine.
func (c *Cluster) isCrashed(id raft.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashed[id]
}

// markSampled records that the watcher has taken at least one observation.
func (c *Cluster) markSampled() {
	c.mu.Lock()
	c.sampled = true
	c.mu.Unlock()
}

// watcherSampled reports whether the watcher has a baseline to compare against.
//
// Exposed for tests: a failure injected before the first sample has no prior
// state to transition from, so a test that races the watcher would otherwise
// look like a missing event.
func (c *Cluster) watcherSampled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sampled
}
