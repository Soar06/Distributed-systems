package demo

import (
	"fmt"

	"github.com/homura/core-bank/ledger"
)

// SeedAccounts opens a few accounts spread across shards.
//
// Chosen so the demo has something to show immediately: at least two accounts on
// DIFFERENT shards, because a cross-shard transfer is the interesting case and
// picking two names at random can easily land both on one shard.
func (c *Cluster) SeedAccounts() error {
	want := map[string]ledger.Money{
		"alice": 100_000,
		"bob":   50_000,
		"carol": 25_000,
		"dave":  10_000,
	}

	// Deterministic order, so two runs of the demo look the same.
	for _, name := range []string{"alice", "bob", "carol", "dave"} {
		if _, err := c.Open(ledger.AccountID(name), want[name]); err != nil {
			return fmt.Errorf("seed %s: %w", name, err)
		}
	}

	// Report the placement, so it is obvious from the log whether a cross-shard
	// transfer is possible without opening more accounts.
	placement := make(map[string][]string)
	for name := range want {
		sid := string(c.sc.Coordinator.ShardFor(ledger.AccountID(name)))
		placement[sid] = append(placement[sid], name)
	}
	if len(placement) < 2 {
		c.logf("NOTE: every seeded account landed on one shard, so no cross-shard " +
			"transfer is possible with these four; open another account to get one")
	}
	return nil
}
