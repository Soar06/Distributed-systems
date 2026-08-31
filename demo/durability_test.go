package demo

import (
	"os"
	"testing"
	"time"
)

// Durability of the demo cluster.
//
// The storage layer (storage/) was implemented and tested long before the demo
// used it: demo.New built every replica with no storage at all, so the UI cluster
// was pure RAM and a restart lost everything. These tests hold the wiring in
// place, because "the WAL exists" and "the demo actually writes to it" are
// different claims and only the second is what a user sees.

// Balances survive the whole process going away and coming back.
func TestClusterWithStorageSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// First life: open an account and move money.
	c, err := NewWithStorage(1, 3, 3, 20260904, dir)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})

	if _, err := c.Open("dave", 10_000); err != nil {
		t.Fatalf("opening dave: %v", err)
	}
	if _, err := c.Transact("deposit", "d1", "", "dave", 500); err != nil {
		t.Fatalf("depositing: %v", err)
	}

	// Stop everything, exactly as killing the process would.
	c.Stop()

	// Second life: same directory, brand new cluster objects.
	c2, err := NewWithStorage(1, 3, 3, 20260904, dir)
	if err != nil {
		t.Fatalf("restarting from %s: %v", dir, err)
	}
	defer c2.Stop()

	waitUntil(t, 10*time.Second, "a leader after restart", func() bool {
		return c2.Snapshot().Shards[0].CanWrite
	})

	// 10000 + 500, rebuilt by replaying each replica's log. Not a cached balance:
	// the ledger is a fold over the log, so this passing means the log itself
	// survived and was replayed in order.
	waitUntil(t, 10*time.Second, "the replayed balance", func() bool {
		return c2.Snapshot().Shards[0].Accounts["dave"] == 10_500
	})

	if got := c2.Snapshot().Shards[0].Accounts["dave"]; got != 10_500 {
		t.Fatalf("dave=%d after restart, want 10500", got)
	}
}

// The control: with no data directory the cluster is in memory, and a restart
// starts empty. Asserted so "it persisted" can never pass by accident — if this
// test and the one above both passed with the same wiring, neither would be
// telling us anything.
func TestClusterWithoutStorageStartsEmpty(t *testing.T) {
	c, err := New(1, 3, 3, 20260905)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})
	if _, err := c.Open("dave", 10_000); err != nil {
		t.Fatalf("opening dave: %v", err)
	}
	c.Stop()

	c2, err := New(1, 3, 3, 20260905)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Stop()
	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c2.Snapshot().Shards[0].CanWrite
	})

	if _, ok := c2.Snapshot().Shards[0].Accounts["dave"]; ok {
		t.Fatal("an in-memory cluster must not resurrect state from a previous run")
	}
}

// Each (machine, shard) replica gets its own log.
//
// node-1 hosting shard-0 and shard-1 runs two independent Raft groups. Sharing
// one file between them would interleave two logs into one and corrupt both on
// replay, so this asserts the separation rather than trusting the filename
// convention to stay right.
func TestEachReplicaGetsItsOwnLog(t *testing.T) {
	dir := t.TempDir()

	c, err := NewWithStorage(2, 3, 3, 20260906, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	waitUntil(t, 10*time.Second, "leaders", func() bool {
		v := c.Snapshot()
		for _, s := range v.Shards {
			if !s.CanWrite {
				return false
			}
		}
		return true
	})

	// 3 machines x 2 shards = 6 replicas, each with a .wal and a .applied.
	entries, err := readDirNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	wals := 0
	for _, name := range entries {
		if len(name) > 4 && name[len(name)-4:] == ".wal" {
			wals++
		}
	}
	if wals != 6 {
		t.Fatalf("found %d .wal files in %s, want 6 (3 machines x 2 shards): %v",
			wals, dir, entries)
	}
}

// readDirNames lists the entries in a directory.
func readDirNames(dir string) ([]string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(des))
	for _, de := range des {
		names = append(names, de.Name())
	}
	return names, nil
}
