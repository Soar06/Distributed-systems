package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/shard"
)

// End-to-end compaction against a real sharded cluster with real money (G3).
//
// raft/snapshot_test.go proves the mechanism against a trivial state machine, and
// shard/snapshot_test.go proves the encoding preserves 2PC promises. This file
// proves the two work TOGETHER on a running cluster with durable storage — which
// is the gap that matters, because every bug this project has found lived in the
// space between "the unit tests pass" and "the whole thing works".
//
// Per RULES.md rule 3: normal (compact and keep serving), restart (a compacted
// node restarts identical), failure (compaction while a 2PC transaction is in
// doubt), and money conservation asserted throughout.

// compactAll compacts every node of every shard, returning how many compacted.
func compactAll(t *testing.T, sc *ShardCluster, threshold int) int {
	t.Helper()
	n := 0
	for _, sid := range sc.Ring.Shards() {
		g := sc.Groups[sid]
		for _, nid := range g.IDs {
			did, err := g.Nodes[nid].MaybeCompact(threshold)
			if err != nil {
				t.Fatalf("compact %s/%s: %v", sid, nid, err)
			}
			if did {
				n++
			}
		}
	}
	return n
}

// The normal path: a cluster that has done real work compacts, and keeps serving
// with balances intact.
func TestClusterCompactsAndKeepsServing(t *testing.T) {
	dir := t.TempDir()
	sc := NewShardClusterWithStorage(t, 2, 3, 41, dir)
	sc.Start()
	defer sc.Stop()
	if !sc.WaitForLeaders(5 * time.Second) {
		t.Fatalf("no leaders%s", sc.View())
	}

	a, b := findCrossShardPair(sc)
	if err := sc.Open(a, 100000); err != nil {
		t.Fatalf("open %s: %v", a, err)
	}
	if err := sc.Open(b, 1000); err != nil {
		t.Fatalf("open %s: %v", b, err)
	}
	total := sc.TotalMoney()

	// Enough single-shard work to make compaction worthwhile.
	for i := range 30 {
		key := fmt.Sprintf("dep-%d", i)
		sid := sc.Coordinator.ShardFor(a)
		if _, _, err := sc.Groups[sid].Propose(shard.Command{
			Op: shard.OpSingle,
			Ledger: ledger.Command{
				Op: ledger.OpDeposit, IdempotencyKey: key, To: a, Amount: 100,
			},
		}, 3*time.Second); err != nil {
			t.Fatalf("deposit %d: %v", i, err)
		}
	}

	balBefore, _ := sc.Balance(a)
	totalBefore := sc.TotalMoney()

	if n := compactAll(t, sc, 5); n == 0 {
		t.Fatalf("nothing compacted after 30 deposits%s", sc.View())
	}

	// Balances unchanged: compaction is housekeeping, never a state change.
	if got, _ := sc.Balance(a); got != balBefore {
		t.Fatalf("%s = %s after compaction, want %s — compaction changed a balance",
			a, got, balBefore)
	}
	if got := sc.TotalMoney(); got != totalBefore {
		t.Fatalf("money conservation violated by compaction: %s -> %s", totalBefore, got)
	}

	// And the cluster still works afterwards.
	sid := sc.Coordinator.ShardFor(a)
	if _, _, err := sc.Groups[sid].Propose(shard.Command{
		Op: shard.OpSingle,
		Ledger: ledger.Command{
			Op: ledger.OpDeposit, IdempotencyKey: "after-compaction", To: a, Amount: 500,
		},
	}, 3*time.Second); err != nil {
		t.Fatalf("deposit after compaction: %v — the log is unmatched at its own "+
			"boundary and replication has stalled%s", err, sc.View())
	}
	if got, _ := sc.Balance(a); got != balBefore+500 {
		t.Fatalf("%s = %s after a post-compaction deposit, want %s", a, got, balBefore+500)
	}
	if got := sc.TotalMoney(); got != total+3000+500 {
		t.Fatalf("total = %s, want %s", got, total+3000+500)
	}
}

// A compacted node must restart with identical state.
//
// This is where a snapshot that missed a field shows up as money: the log prefix
// is gone, so anything the snapshot failed to capture is simply not there.
func TestCompactedClusterRestartsIdentical(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var wantA, wantB, wantTotal ledger.Money

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 42, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders")
		}

		a, b = findCrossShardPair(sc)
		sc.Open(a, 50000)
		sc.Open(b, 5000)

		// A committed cross-shard transfer, so 2PC records exist in the snapshot.
		if _, err := sc.Coordinator.Transfer("done-tx", ledger.Command{
			Op: ledger.OpTransfer, IdempotencyKey: "done-tx", From: a, To: b, Amount: 7000,
		}); err != nil {
			t.Fatalf("transfer: %v", err)
		}

		for i := range 25 {
			key := fmt.Sprintf("w-%d", i)
			sid := sc.Coordinator.ShardFor(a)
			sc.Groups[sid].Propose(shard.Command{
				Op: shard.OpSingle,
				Ledger: ledger.Command{
					Op: ledger.OpWithdraw, IdempotencyKey: key, From: a, Amount: 100,
				},
			}, 3*time.Second)
		}

		if n := compactAll(t, sc, 5); n == 0 {
			t.Fatalf("nothing compacted%s", sc.View())
		}

		wantA, _ = sc.Balance(a)
		wantB, _ = sc.Balance(b)
		wantTotal = sc.TotalMoney()
	}()

	// Restart over the same files. The log prefix is gone, so everything must come
	// from the snapshots.
	sc := NewShardClusterWithStorage(t, 2, 3, 42, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore after compaction: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	if got, _ := sc.Balance(a); got != wantA {
		t.Fatalf("%s = %s after restarting a COMPACTED cluster, want %s — the snapshot "+
			"did not capture everything the discarded log held%s", a, got, wantA, sc.View())
	}
	if got, _ := sc.Balance(b); got != wantB {
		t.Fatalf("%s = %s after restart, want %s%s", b, got, wantB, sc.View())
	}
	if got := sc.TotalMoney(); got != wantTotal {
		t.Fatalf("money conservation violated across compaction+restart: %s -> %s",
			wantTotal, got)
	}

	// The committed transfer's idempotency must survive too, or a retry after
	// compaction moves the money a second time.
	if _, err := sc.Coordinator.Transfer("done-tx", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "done-tx", From: a, To: b, Amount: 7000,
	}); err != nil {
		t.Logf("retry of a compacted transfer reported: %v", err)
	}
	if got := sc.TotalMoney(); got != wantTotal {
		t.Fatalf("a retried transfer after compaction moved money: %s -> %s", wantTotal, got)
	}
	if got, _ := sc.Balance(a); got != wantA {
		t.Fatalf("%s = %s after a retried transfer, want %s — compaction lost the "+
			"idempotency record", a, got, wantA)
	}
}

// THE case G3's design named first: compaction must not destroy an in-doubt 2PC
// promise.
//
// A prepared participant is holding customer funds against an unretractable
// promise. Compaction discards the log entry that recorded the vote. If the
// snapshot does not carry it forward, a restarted node forgets the promise and
// the reserved money becomes spendable again — the exact failure the 2PC
// durability work eliminated, reintroduced by housekeeping.
func TestCompactionPreservesInDoubtPromiseAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var total ledger.Money
	var debitShard shard.ID

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 43, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders")
		}

		a, b = findCrossShardPair(sc)
		sc.Open(a, 20000)
		sc.Open(b, 1000)
		total = sc.TotalMoney()

		// Some traffic, so there is a log worth compacting.
		for i := range 20 {
			key := fmt.Sprintf("noise-%d", i)
			sid := sc.Coordinator.ShardFor(a)
			sc.Groups[sid].Propose(shard.Command{
				Op: shard.OpSingle,
				Ledger: ledger.Command{
					Op: ledger.OpDeposit, IdempotencyKey: key, To: a, Amount: 10,
				},
			}, 3*time.Second)
		}
		total = sc.TotalMoney()

		// Now prepare a cross-shard transfer and stop — leaving it in doubt.
		debitShard, _ = prepareCrossShard(t, sc, "held", a, b, 5000)

		if avail, _ := sc.Groups[debitShard].Machine().State.Available(a); avail != total-1000-5000 {
			t.Logf("available while prepared: %s", avail)
		}

		// Compact WITH the transaction in doubt: the prepare entry is discarded.
		if n := compactAll(t, sc, 5); n == 0 {
			t.Fatalf("nothing compacted%s", sc.View())
		}

		// Still in doubt, still holding funds, immediately after compaction.
		if sc.InDoubt() == 0 {
			t.Fatalf("compaction resolved an in-doubt transaction — a participant may "+
				"not release its own promise%s", sc.View())
		}
	}()

	// Restart. The prepare entry no longer exists anywhere but the snapshot.
	sc := NewShardClusterWithStorage(t, 2, 3, 43, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	rec, ok := sc.Groups[debitShard].Machine().Tx("held")
	if !ok {
		t.Fatalf("the promise was LOST across compaction+restart — the snapshot did "+
			"not carry the 2PC record, so this shard forgot an unretractable "+
			"commitment and freed money it had already promised%s", sc.View())
	}
	if rec.Phase != shard.PhasePrepared {
		t.Fatalf("phase = %v after compaction+restart, want Prepared", rec.Phase)
	}

	// The reservation survived: the money is held, not spendable.
	avail, _ := sc.Groups[debitShard].Machine().State.Available(a)
	bal, _ := sc.Balance(a)
	if bal-avail != 5000 {
		t.Fatalf("balance %s minus available %s = %s, want 50.00 reserved — the "+
			"reservation did not survive compaction%s", bal, avail, bal-avail, sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated across compaction+restart: %s -> %s",
			total, got)
	}

	// And recovery can still resolve it, which is the point of keeping the promise.
	if _, err := sc.Coordinator.RecoverInDoubt(); err != nil {
		t.Fatalf("recovery after compaction: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sc.InDoubt() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := sc.InDoubt(); n != 0 {
		t.Fatalf("%d still in doubt after recovery%s", n, sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated by recovery: %s -> %s", total, got)
	}
}

// Compaction must be repeatable: a cluster that compacts, works, and compacts
// again stays correct. A one-shot compaction that corrupts the second attempt
// would pass every test above.
func TestRepeatedCompactionStaysCorrect(t *testing.T) {
	dir := t.TempDir()
	sc := NewShardClusterWithStorage(t, 2, 3, 44, dir)
	sc.Start()
	defer sc.Stop()
	if !sc.WaitForLeaders(5 * time.Second) {
		t.Fatalf("no leaders")
	}

	a, _ := findCrossShardPair(sc)
	sc.Open(a, 10000)
	sid := sc.Coordinator.ShardFor(a)

	expected := ledger.Money(10000)
	for round := range 4 {
		for i := range 15 {
			key := fmt.Sprintf("r%d-d%d", round, i)
			if _, _, err := sc.Groups[sid].Propose(shard.Command{
				Op: shard.OpSingle,
				Ledger: ledger.Command{
					Op: ledger.OpDeposit, IdempotencyKey: key, To: a, Amount: 100,
				},
			}, 3*time.Second); err != nil {
				t.Fatalf("round %d deposit %d: %v", round, i, err)
			}
			expected += 100
		}

		compactAll(t, sc, 5)

		if got, _ := sc.Balance(a); got != expected {
			t.Fatalf("round %d: %s = %s, want %s — a later compaction corrupted state%s",
				round, a, got, expected, sc.View())
		}
	}
	t.Logf("4 rounds of work-then-compact; balance exactly %s throughout", expected)
}
