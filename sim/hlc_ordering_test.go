package sim

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/homura/core-bank/hlc"
	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/shard"
)

// Cross-shard event ordering via HLC (Phase 3).
//
// hlc/hlc_test.go proves the clock algorithm. This proves it delivers the thing
// it exists for: a total order over transactions that span shards.
//
// The gap being closed: Transaction.Seq is a per-shard counter, so Seq=7 on
// shard-0 and Seq=7 on shard-1 are unrelated numbers. Without a shared clock the
// system cannot answer questions any bank asks — show a customer's transactions
// in order when their accounts span shards, or what the books looked like at
// 14:32.
//
// Per RULES.md rule 3: normal (ordering across shards), determinism (every
// replica of a shard records the SAME timestamp, which is what makes it safe at
// all), retry (a retried request does not corrupt the order), and durability (the
// timestamp survives a snapshot).

// txsOnShard collects a shard leader's transaction history.
func txsOnShard(sc *ShardCluster, sid shard.ID) []ledger.Transaction {
	return sc.Groups[sid].Machine().State.History()
}

// Every transaction across every shard must be orderable by timestamp.
func TestTransactionsAcrossShardsAreTotallyOrdered(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 71)

	// Traffic spread over all three shards, interleaved.
	var accounts []ledger.AccountID
	for i := range 6 {
		a := ledger.AccountID(fmt.Sprintf("ord-%d", i))
		if err := sc.Open(a, 10000); err != nil {
			t.Fatalf("open %s: %v", a, err)
		}
		accounts = append(accounts, a)
	}
	for i := range 12 {
		a := accounts[i%len(accounts)]
		sid := sc.Coordinator.ShardFor(a)
		if _, _, err := sc.Groups[sid].Propose(shard.Command{
			Op: shard.OpSingle,
			Ledger: ledger.Command{
				Op: ledger.OpDeposit, IdempotencyKey: fmt.Sprintf("ord-dep-%d", i),
				To: a, Amount: 100, Timestamp: sc.Coordinator.Clock().Now(),
			},
		}, 3*time.Second); err != nil {
			t.Fatalf("deposit %d: %v", i, err)
		}
	}

	// Gather every transaction from every shard.
	type stamped struct {
		shard shard.ID
		tx    ledger.Transaction
	}
	var all []stamped
	for _, sid := range sc.Ring.Shards() {
		for _, tx := range txsOnShard(sc, sid) {
			all = append(all, stamped{sid, tx})
		}
	}
	if len(all) < 12 {
		t.Fatalf("only %d transactions recorded across all shards", len(all))
	}

	stampedCount := 0
	for _, s := range all {
		if !s.tx.Timestamp.IsZero() {
			stampedCount++
		}
	}
	if stampedCount == 0 {
		t.Fatal("no transaction carries an HLC timestamp; cross-shard ordering is " +
			"impossible and Seq alone cannot supply it — Seq=7 on two shards are " +
			"unrelated numbers")
	}

	// Sorting by timestamp must produce a strict total order: no two DIFFERENT
	// transactions may share one, or the order is ambiguous exactly where it
	// matters.
	sort.Slice(all, func(i, j int) bool { return all[i].tx.Timestamp.Less(all[j].tx.Timestamp) })

	seen := make(map[hlc.Timestamp]string)
	for _, s := range all {
		if s.tx.Timestamp.IsZero() {
			continue
		}
		key := fmt.Sprintf("%s/%s", s.shard, s.tx.IdempotencyKey)
		if prev, dup := seen[s.tx.Timestamp]; dup && prev != key {
			t.Fatalf("two different transactions share timestamp %v: %s and %s. "+
				"A shared timestamp makes the order ambiguous precisely where an audit "+
				"needs it to be definite", s.tx.Timestamp, prev, key)
		}
		seen[s.tx.Timestamp] = key
	}

	t.Logf("%d transactions across %d shards, %d stamped, all distinctly ordered",
		len(all), len(sc.Ring.Shards()), stampedCount)
}

// THE determinism property: every replica of a shard must record the SAME
// timestamp for a transaction.
//
// This is what makes stamping safe at all. If the timestamp were read during
// Apply rather than carried in the command, two replicas would produce different
// state from the same log — the ledger would diverge across a shard's own
// replicas, and the divergence would be invisible until someone compared them.
func TestEveryReplicaRecordsTheSameTimestamp(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 72)

	a, _ := findCrossShardPair(sc)
	if err := sc.Open(a, 10000); err != nil {
		t.Fatalf("open: %v", err)
	}

	sid := sc.Coordinator.ShardFor(a)
	if _, _, err := sc.Groups[sid].Propose(shard.Command{
		Op: shard.OpSingle,
		Ledger: ledger.Command{
			Op: ledger.OpDeposit, IdempotencyKey: "same-ts", To: a, Amount: 500,
			Timestamp: sc.Coordinator.Clock().Now(),
		},
	}, 3*time.Second); err != nil {
		t.Fatalf("deposit: %v", err)
	}

	// Wait for every replica to apply it, then compare.
	g := sc.Groups[sid]
	deadline := time.Now().Add(5 * time.Second)
	var stamps map[string]hlc.Timestamp

	for time.Now().Before(deadline) {
		stamps = make(map[string]hlc.Timestamp)
		complete := true
		for _, nid := range g.IDs {
			found := false
			for _, tx := range g.SMs[nid].State.History() {
				if tx.IdempotencyKey == "same-ts" {
					stamps[string(nid)] = tx.Timestamp
					found = true
				}
			}
			if !found {
				complete = false
			}
		}
		if complete {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(stamps) < 2 {
		t.Fatalf("only %d replicas applied the transaction; cannot compare", len(stamps))
	}

	var first hlc.Timestamp
	var firstNode string
	for node, ts := range stamps {
		if firstNode == "" {
			first, firstNode = ts, node
			continue
		}
		if !ts.Equal(first) {
			t.Fatalf("replicas disagree about a transaction's timestamp: %s has %v, "+
				"%s has %v. The timestamp must be carried IN the command and applied "+
				"identically everywhere; reading a clock during Apply makes the ledger "+
				"diverge across a shard's own replicas", firstNode, first, node, ts)
		}
	}
	t.Logf("all %d replicas recorded timestamp %v", len(stamps), first)
}

// Both legs of a cross-shard transfer must carry the same timestamp: they are one
// transaction, recorded in two different Raft logs.
func TestBothLegsOfACrossShardTransferShareATimestamp(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 73)

	a, b := findCrossShardPair(sc)
	if err := sc.Open(a, 20000); err != nil {
		t.Fatalf("open %s: %v", a, err)
	}
	if err := sc.Open(b, 1000); err != nil {
		t.Fatalf("open %s: %v", b, err)
	}

	if _, err := sc.Coordinator.Transfer("xshard-ts", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "xshard-ts", From: a, To: b, Amount: 5000,
	}); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	debitShard := sc.Coordinator.ShardFor(a)
	creditShard := sc.Coordinator.ShardFor(b)

	find := func(sid shard.ID, account ledger.AccountID) (hlc.Timestamp, bool) {
		for _, tx := range txsOnShard(sc, sid) {
			for _, e := range tx.Entries {
				if e.Account == account && !tx.Timestamp.IsZero() {
					return tx.Timestamp, true
				}
			}
		}
		return hlc.Timestamp{}, false
	}

	debitTS, okD := find(debitShard, a)
	creditTS, okC := find(creditShard, b)

	if !okD {
		t.Fatalf("the DEBIT leg on %s carries no HLC timestamp. The internal 2PC legs "+
			"are exactly the transactions Phase 3 exists to order — they live in "+
			"different Raft logs by construction, so Seq cannot relate them", debitShard)
	}
	if !okC {
		t.Fatalf("the CREDIT leg on %s carries no HLC timestamp", creditShard)
	}

	// Identical, not merely orderable: the two legs are ONE transaction, booked in
	// two different Raft logs. A shared timestamp is what lets an auditor see them
	// as a single event rather than two coincidental ones.
	if !debitTS.Equal(creditTS) {
		t.Fatalf("the legs of one transfer carry different timestamps: debit %v on "+
			"%s, credit %v on %s. They are one transaction and must be stamped as one",
			debitTS, debitShard, creditTS, creditShard)
	}
	t.Logf("both legs of the cross-shard transfer share timestamp %v (%s and %s)",
		debitTS, debitShard, creditShard)
}

// A retried request must not corrupt the order.
//
// The retry carries a FRESH timestamp — it is a new attempt — but it must return
// the original result and record nothing new, so the history stays exactly as it
// was.
func TestRetryDoesNotCorruptTimestampOrder(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 74)

	a, _ := findCrossShardPair(sc)
	if err := sc.Open(a, 10000); err != nil {
		t.Fatalf("open: %v", err)
	}

	sid := sc.Coordinator.ShardFor(a)
	propose := func() {
		sc.Groups[sid].Propose(shard.Command{
			Op: shard.OpSingle,
			Ledger: ledger.Command{
				Op: ledger.OpDeposit, IdempotencyKey: "retry-ts", To: a, Amount: 700,
				Timestamp: sc.Coordinator.Clock().Now(),
			},
		}, 3*time.Second)
	}

	propose()
	before := txsOnShard(sc, sid)
	balBefore, _ := sc.Balance(a)

	// The same idempotency key, a fresh timestamp.
	propose()

	after := txsOnShard(sc, sid)
	balAfter, _ := sc.Balance(a)

	if balAfter != balBefore {
		t.Fatalf("a retry moved money: %s -> %s", balBefore, balAfter)
	}
	if len(after) != len(before) {
		t.Fatalf("a retry appended %d extra transactions to history; the retry must "+
			"return the original result and record nothing", len(after)-len(before))
	}

	// And history remains sorted by timestamp.
	for i := 1; i < len(after); i++ {
		if after[i].Timestamp.IsZero() || after[i-1].Timestamp.IsZero() {
			continue
		}
		if after[i].Timestamp.Less(after[i-1].Timestamp) {
			t.Fatalf("history is out of timestamp order at %d: %v before %v",
				i, after[i-1].Timestamp, after[i].Timestamp)
		}
	}
}

// Timestamps must survive compaction.
//
// A snapshot that dropped them would silently destroy cross-shard ordering — the
// exact class of bug G3's design named: anything the snapshot fails to capture is
// gone the moment the log prefix is discarded, and no Raft correctness catches it.
func TestTimestampsSurviveCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()

	var a ledger.AccountID
	var want []hlc.Timestamp
	var sid shard.ID

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 75, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders")
		}

		a, _ = findCrossShardPair(sc)
		if err := sc.Open(a, 50000); err != nil {
			t.Fatalf("open: %v", err)
		}
		sid = sc.Coordinator.ShardFor(a)

		for i := range 20 {
			sc.Groups[sid].Propose(shard.Command{
				Op: shard.OpSingle,
				Ledger: ledger.Command{
					Op: ledger.OpDeposit, IdempotencyKey: fmt.Sprintf("snap-ts-%d", i),
					To: a, Amount: 100, Timestamp: sc.Coordinator.Clock().Now(),
				},
			}, 3*time.Second)
		}

		if n := compactAll(t, sc, 5); n == 0 {
			t.Fatalf("nothing compacted%s", sc.View())
		}

		for _, tx := range txsOnShard(sc, sid) {
			want = append(want, tx.Timestamp)
		}
	}()

	sc := NewShardClusterWithStorage(t, 2, 3, 75, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	got := txsOnShard(sc, sid)
	if len(got) != len(want) {
		t.Fatalf("history is %d transactions after compaction+restart, want %d",
			len(got), len(want))
	}
	for i := range want {
		if !got[i].Timestamp.Equal(want[i]) {
			t.Fatalf("transaction %d has timestamp %v after compaction+restart, want "+
				"%v — the snapshot dropped the ordering", i, got[i].Timestamp, want[i])
		}
	}
	t.Logf("%d timestamps survived compaction and restart unchanged", len(got))
}
