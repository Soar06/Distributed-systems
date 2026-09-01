package demo

import (
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// The co-located topology: N machines, each hosting a replica of EVERY shard.
//
// This is what cmd/shardnode -hosts runs and what CockroachDB does with Ranges.
// The demo used to give each shard its own machines, which is easier to follow
// but locks the two dimensions together — it makes "which machines hold this
// account?" trivially "the three named after that shard", and hides that losing
// one machine costs a replica of everything at once.

// Every machine must host every shard, and hold a copy of every account.
func TestEveryMachineHostsEveryShard(t *testing.T) {
	c := newTestCluster(t, 3, 3)

	for _, name := range []string{"alice", "bob", "carol"} {
		if _, err := c.Open(ledger.AccountID(name), 1000); err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
	}

	v := c.Snapshot()
	if len(v.Machines) != 3 {
		t.Fatalf("%d machines, want 3", len(v.Machines))
	}

	for _, m := range v.Machines {
		if len(m.Replicas) != 3 {
			t.Fatalf("%s hosts %d shard replicas, want 3 — in a co-located cluster a "+
				"machine carries a slice of every shard", m.ID, len(m.Replicas))
		}
	}

	// Leadership must be a property of the (machine, shard) pair, not of the
	// machine: a machine can lead one shard while following another.
	leaders := map[string]string{}
	for _, m := range v.Machines {
		for _, r := range m.Replicas {
			if r.Role == "Leader" {
				if prev, dup := leaders[r.Shard]; dup {
					t.Fatalf("shard %s has two leaders: %s and %s", r.Shard, prev, m.ID)
				}
				leaders[r.Shard] = m.ID
			}
		}
	}
	if len(leaders) != 3 {
		t.Fatalf("%d shards have leaders, want 3", len(leaders))
	}
}

// THE property this topology exists to show: killing one machine costs one
// replica of EVERY shard simultaneously.
func TestKillingOneMachineAffectsEveryShard(t *testing.T) {
	c := newTestCluster(t, 3, 3)

	if _, err := c.Open("alice", 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	victim := c.Snapshot().Machines[0].ID
	if err := c.Kill(raft.NodeID(victim)); err != nil {
		t.Fatalf("kill %s: %v", victim, err)
	}

	v := c.Snapshot()
	for _, m := range v.Machines {
		if m.ID != victim {
			continue
		}
		if !m.Crashed {
			t.Fatalf("%s is not reported crashed", victim)
		}
		for _, r := range m.Replicas {
			if r.Role != "Down" {
				t.Fatalf("%s still reports %s for %s after the machine was killed — "+
					"a machine is reachable or it is not, and when it dies every group "+
					"it hosts loses that replica together", victim, r.Role, r.Shard)
			}
		}
	}

	// Every shard lost a replica, and every shard survives it: 2 of 3 remain.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v = c.Snapshot()
		allReady := true
		for _, s := range v.Shards {
			if !s.Ready {
				allReady = false
			}
		}
		if allReady {
			t.Logf("killed %s: every shard lost one replica and all still commit", victim)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("shards did not all recover after losing one of three machines%v",
		func() []string {
			var out []string
			for _, s := range v.Shards {
				if !s.Ready {
					out = append(out, s.ID+": "+s.Reason)
				}
			}
			return out
		}())
}

// An account's replicas must be locatable: which shard owns it, and which
// machines hold that shard. This is what the UI's highlight proves visually.
func TestAccountReplicasAreLocatable(t *testing.T) {
	c := newTestCluster(t, 3, 3)

	if _, err := c.Open("alice", 4200); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Waited for, not sampled immediately: a follower applies the entry a moment
	// after the leader commits it, so an instant read legitimately finds fewer
	// copies. Replication is asynchronous by design — what must be true is that it
	// CONVERGES, not that it is instantaneous.
	var owner string
	holders := 0
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		v := c.Snapshot()

		owner = ""
		for _, p := range v.Ring {
			if p.Account == "alice" {
				owner = p.Shard
			}
		}
		if owner == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		holders = 0
		for _, m := range v.Machines {
			for _, r := range m.Replicas {
				if r.Shard == owner {
					if _, ok := r.Accounts["alice"]; ok {
						holders++
					}
				}
			}
		}
		if holders >= 3 {
			t.Logf("alice is owned by %s and copied on all %d machines", owner, holders)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	if owner == "" {
		t.Fatal("alice has no ring placement")
	}
	t.Fatalf("only %d machines hold a copy of alice after 5s; a replicated account "+
		"must converge onto every replica, or it is not replicated at all", holders)
}
