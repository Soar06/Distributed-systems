package demo

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/homura/core-bank/raft"
)

// Simulated per-machine health, and what it does to leadership.
//
// Health here is INVENTED — a stand-in for the signals a real operator would
// have (CPU pressure, disk latency, GC pauses). It is not measured from anything.
// What is real is the effect: a healthier machine campaigns sooner, so it usually
// wins the election, without any change to who is ALLOWED to win.
//
// That distinction is the whole reason this is safe. Raft's election has two
// parts and only one is a guarantee:
//
//   - eligibility — a candidate missing committed entries is refused a vote
//     (§5.4.1). Untouched here.
//   - which eligible one wins — arbitrary by design, decided by whoever's
//     randomized timer fired first. That is what health replaces.
//
// The case worth watching in the UI: a machine can report HIGH health while
// being ineligible, because it was partitioned and is idle and fast precisely
// BECAUSE it missed writes. Health says "prefer it"; safety says "never". Safety
// wins, and the machine sits there looking best and unable to lead.

// healthState is one machine's health plus whether the operator has pinned it.
type healthState struct {
	level  raft.NodeHealth
	locked bool
}

// SetHealth sets a machine's health, optionally locking it against auto-drift.
func (c *Cluster) SetHealth(id raft.NodeID, level raft.NodeHealth, lock bool) error {
	if !c.knowsMachine(id) {
		return fmt.Errorf("unknown machine %s", id)
	}

	c.mu.Lock()
	if c.health == nil {
		c.health = make(map[raft.NodeID]*healthState)
	}
	c.health[id] = &healthState{level: level, locked: lock}
	c.mu.Unlock()

	c.applyHealth(id, level)

	if lock {
		c.logEvent(Event{Kind: KindControl, Node: string(id),
			Text: fmt.Sprintf("%s health LOCKED at %s", id, level)})
	} else {
		c.logEvent(Event{Kind: KindControl, Node: string(id),
			Text: fmt.Sprintf("%s health set to %s (will drift again)", id, level)})
	}
	return nil
}

// UnlockHealth returns a machine to automatic drift.
func (c *Cluster) UnlockHealth(id raft.NodeID) error {
	c.mu.Lock()
	st, ok := c.health[id]
	if ok {
		st.locked = false
	}
	c.mu.Unlock()

	if !ok {
		return fmt.Errorf("unknown machine %s", id)
	}
	c.logEvent(Event{Kind: KindControl, Node: string(id),
		Text: fmt.Sprintf("%s health unlocked", id)})
	return nil
}

// HealthOf reports a machine's health and whether it is locked.
func (c *Cluster) HealthOf(id raft.NodeID) (raft.NodeHealth, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.health[id]; ok {
		return st.level, st.locked
	}
	return raft.HealthNormal, false
}

// applyHealth pushes a level onto every replica the machine hosts.
//
// Health belongs to the MACHINE, but Raft state is per (machine, shard) — so one
// setting fans out to every group that machine participates in. A machine under
// CPU pressure is slow for all of them, not for one.
func (c *Cluster) applyHealth(id raft.NodeID, level raft.NodeHealth) {
	c.mu.Lock()
	sc := c.sc
	c.mu.Unlock()

	for _, sid := range sc.Ring.Shards() {
		g := sc.Groups[sid]
		if srv, ok := g.Nodes[id]; ok {
			srv.SetNodeHealth(level)
		}
	}
}

// knowsMachine reports whether the id names a machine in this cluster.
func (c *Cluster) knowsMachine(id raft.NodeID) bool {
	c.mu.Lock()
	sc, n := c.sc, c.nodes
	c.mu.Unlock()

	for _, sid := range sc.Ring.Shards() {
		if _, ok := sc.Groups[sid].Nodes[id]; ok {
			return true
		}
	}
	// A machine holding no shard is still a machine, and with a fixed replication
	// factor a large cluster has some. Checking the configured range keeps those
	// settable rather than reporting them unknown.
	for i := range n {
		if id == raft.NodeID(fmt.Sprintf("node-%d", i+1)) {
			return true
		}
	}
	return false
}

// driftHealth re-rolls unlocked machines' health on an interval.
//
// Every 5s by default, which is slow enough to watch and fast enough that a
// leadership change is visibly caused by it rather than looking random.
func (c *Cluster) driftHealth(every time.Duration, done <-chan struct{}) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			c.mu.Lock()
			n := c.nodes
			locked := make(map[raft.NodeID]bool, len(c.health))
			for id, st := range c.health {
				locked[id] = st.locked
			}
			c.mu.Unlock()

			for i := range n {
				id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
				if locked[id] {
					continue // pinned by the operator
				}

				// Weighted toward normal: a cluster where every machine is constantly
				// at an extreme teaches nothing, because leadership would thrash.
				var level raft.NodeHealth
				switch r := rnd.Intn(10); {
				case r < 2:
					level = raft.HealthLow
				case r < 5:
					level = raft.HealthHigh
				default:
					level = raft.HealthNormal
				}

				c.mu.Lock()
				if c.health == nil {
					c.health = make(map[raft.NodeID]*healthState)
				}
				prev, existed := c.health[id]
				c.health[id] = &healthState{level: level}
				c.mu.Unlock()

				c.applyHealth(id, level)

				// Logged only on a change, so the event feed shows health causing a
				// leadership move rather than a wall of identical lines.
				if !existed || prev.level != level {
					c.logEvent(Event{Kind: KindDrift, Node: string(id),
						Text: fmt.Sprintf("%s health drifted to %s", id, level)})
				}
			}
		}
	}
}
