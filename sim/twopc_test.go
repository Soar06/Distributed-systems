package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/shard"
)

// Phase 2 tests: sharding and cross-shard 2PC.
//
// Per RULES.md rule 3, and the "match the real world and the real paper"
// standard: the coordinator is Raft-replicated exactly as Spanner describes, and
// the blocking problem is DEMONSTRATED rather than hidden.
//
// The governing invariant throughout: no scenario may create or destroy a cent.

// findCrossShardPair returns two accounts that hash to different shards, so a
// transfer between them genuinely requires 2PC.
func findCrossShardPair(sc *ShardCluster) (ledger.AccountID, ledger.AccountID) {
	for i := range 200 {
		for j := i + 1; j < 200; j++ {
			a := ledger.AccountID(fmt.Sprintf("acct-%d", i))
			b := ledger.AccountID(fmt.Sprintf("acct-%d", j))
			if sc.Coordinator.ShardFor(a) != sc.Coordinator.ShardFor(b) {
				return a, b
			}
		}
	}
	return "", ""
}

func findSameShardPair(sc *ShardCluster) (ledger.AccountID, ledger.AccountID) {
	for i := range 200 {
		for j := i + 1; j < 200; j++ {
			a := ledger.AccountID(fmt.Sprintf("acct-%d", i))
			b := ledger.AccountID(fmt.Sprintf("acct-%d", j))
			if sc.Coordinator.ShardFor(a) == sc.Coordinator.ShardFor(b) {
				return a, b
			}
		}
	}
	return "", ""
}

func newRunningCluster(t *testing.T, nShards, nPerShard int, seed int64) *ShardCluster {
	t.Helper()
	sc := NewShardCluster(nShards, nPerShard, seed)
	sc.Start()
	t.Cleanup(sc.Stop)
	if !sc.WaitForLeaders(5 * time.Second) {
		t.Fatalf("not every shard elected a leader%s", sc.View())
	}
	return sc
}

// --- Placement ------------------------------------------------------------

func TestAccountsSpreadAcrossShards(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 1)

	used := make(map[shard.ID]int)
	for i := range 60 {
		a := ledger.AccountID(fmt.Sprintf("acct-%d", i))
		used[sc.Coordinator.ShardFor(a)]++
	}
	if len(used) < 3 {
		t.Fatalf("accounts landed on only %d shards, want 3: %v", len(used), used)
	}
	t.Logf("account distribution: %v", used)
}

// --- Intra-shard transfers (no 2PC needed) -------------------------------

func TestSameShardTransferUsesSingleCommit(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 2)

	a, b := findSameShardPair(sc)
	if a == "" {
		t.Skip("no same-shard pair found for this ring")
	}
	if err := sc.Open(a, 10000); err != nil {
		t.Fatalf("open %s: %v", a, err)
	}
	if err := sc.Open(b, 5000); err != nil {
		t.Fatalf("open %s: %v", b, err)
	}
	total := sc.TotalMoney()

	res, err := sc.Coordinator.Transfer("tx-same", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "tx-same",
		From: a, To: b, Amount: 3000,
	})
	if err != nil {
		t.Fatalf("same-shard transfer failed: %v%s", err, sc.View())
	}
	if !res.OK {
		t.Fatalf("transfer not OK: %s", res.Err)
	}

	if got, _ := sc.Balance(a); got != 7000 {
		t.Fatalf("%s = %s, want 70.00", a, got)
	}
	if got, _ := sc.Balance(b); got != 8000 {
		t.Fatalf("%s = %s, want 80.00", b, got)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s", total, got)
	}
	// A same-shard transfer must not have used 2PC at all.
	if n := sc.InDoubt(); n != 0 {
		t.Fatalf("%d in-doubt transactions after a single-shard transfer", n)
	}
}

// --- Cross-shard transfers (2PC) -----------------------------------------

func TestCrossShardTransferCommits(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 3)

	a, b := findCrossShardPair(sc)
	if a == "" {
		t.Fatal("no cross-shard pair found")
	}
	t.Logf("%s on %s, %s on %s", a, sc.Coordinator.ShardFor(a), b, sc.Coordinator.ShardFor(b))

	sc.Open(a, 10000)
	sc.Open(b, 5000)
	total := sc.TotalMoney()

	res, err := sc.Coordinator.Transfer("tx-cross", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "tx-cross",
		From: a, To: b, Amount: 3000,
	})
	if err != nil {
		t.Fatalf("cross-shard transfer failed: %v%s", err, sc.View())
	}
	if !res.OK {
		t.Fatalf("transfer not OK: %s", res.Err)
	}

	// Both halves must have landed, on two independent Raft groups.
	if got, _ := sc.Balance(a); got != 7000 {
		t.Fatalf("debit side %s = %s, want 70.00%s", a, got, sc.View())
	}
	if got, _ := sc.Balance(b); got != 8000 {
		t.Fatalf("credit side %s = %s, want 80.00%s", b, got, sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money created or destroyed: %s -> %s", total, got)
	}
	if n := sc.InDoubt(); n != 0 {
		t.Fatalf("%d transactions left in doubt after a clean commit", n)
	}
}

// A NO vote must abort cleanly, with the reservation released.
func TestCrossShardTransferAbortsOnInsufficientFunds(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 4)

	a, b := findCrossShardPair(sc)
	sc.Open(a, 1000)
	sc.Open(b, 1000)
	total := sc.TotalMoney()

	_, err := sc.Coordinator.Transfer("tx-broke", ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "tx-broke",
		From: a, To: b, Amount: 999999,
	})
	if err == nil {
		t.Fatal("transfer beyond the balance succeeded")
	}

	// Nothing may have moved, and no funds may stay reserved.
	if got, _ := sc.Balance(a); got != 1000 {
		t.Fatalf("%s = %s after an aborted transfer, want 10.00", a, got)
	}
	if got, _ := sc.Balance(b); got != 1000 {
		t.Fatalf("%s = %s after an aborted transfer, want 10.00", b, got)
	}
	if r := sc.Groups[sc.Coordinator.ShardFor(a)].Machine().State.Reserved(a); r != 0 {
		t.Fatalf("%s still has %s reserved after abort", a, r)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated on abort: %s -> %s", total, got)
	}
}

// --- Reservations: the double-spend guard --------------------------------

// Money promised to an in-flight cross-shard transfer must not be spendable
// locally. Without this, the same funds could be committed twice — once by the
// transfer and once by a local withdrawal.
func TestReservedFundsCannotBeSpentTwice(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 5)

	a, b := findCrossShardPair(sc)
	sc.Open(a, 10000)
	sc.Open(b, 0)
	total := sc.TotalMoney()

	st := sc.Groups[sc.Coordinator.ShardFor(a)].Machine().State

	// Reserve nearly everything, as a prepare would.
	if res := st.PrepareDebit("inflight", a, 9000); !res.OK {
		t.Fatalf("prepare failed: %s", res.Err)
	}

	// The balance is unchanged — the money is still there...
	if bal, _ := st.Balance(a); bal != 10000 {
		t.Fatalf("balance changed at prepare time: %s, want 100.00", bal)
	}
	// ...but only 10.00 is actually available.
	if avail, _ := st.Available(a); avail != 1000 {
		t.Fatalf("available = %s, want 10.00", avail)
	}
	// And conservation still holds mid-flight: reserved money is not lost.
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("reservation changed the total: %s -> %s", total, got)
	}

	// A withdrawal beyond the available amount must be refused.
	res := st.Apply(ledger.Command{
		Op: ledger.OpWithdraw, IdempotencyKey: "sneaky", From: a, Amount: 5000,
	})
	if res.OK {
		t.Fatal("spent money that was already promised to an in-flight transfer")
	}

	// Within the available amount, it must succeed.
	res = st.Apply(ledger.Command{
		Op: ledger.OpWithdraw, IdempotencyKey: "fine", From: a, Amount: 500,
	})
	if !res.OK {
		t.Fatalf("withdrawal within available funds refused: %s", res.Err)
	}
}

func TestAbortReleasesReservation(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 6)
	a, _ := findCrossShardPair(sc)
	sc.Open(a, 10000)

	st := sc.Groups[sc.Coordinator.ShardFor(a)].Machine().State
	st.PrepareDebit("tx", a, 4000)

	if avail, _ := st.Available(a); avail != 6000 {
		t.Fatalf("available = %s after reserve, want 60.00", avail)
	}
	st.AbortTx("tx")
	if avail, _ := st.Available(a); avail != 10000 {
		t.Fatalf("available = %s after abort, want 100.00 — reservation leaked", avail)
	}
}

// --- Idempotency ----------------------------------------------------------

// Delivering the decision twice must not apply it twice. Real networks do this.
func TestDuplicateDecisionDeliveryIsIdempotent(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 7)
	a, b := findCrossShardPair(sc)
	sc.Open(a, 10000)
	sc.Open(b, 0)
	total := sc.TotalMoney()

	debitShard := sc.Coordinator.ShardFor(a)
	creditShard := sc.Coordinator.ShardFor(b)
	cmd := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "dup", From: a, To: b, Amount: 2500}

	// Prepare both sides.
	sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "dup", Ledger: cmd, Debit: true,
		Participants: []shard.ID{debitShard, creditShard},
	}, 3*time.Second)
	sc.Groups[creditShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "dup", Ledger: cmd, Debit: false,
		Participants: []shard.ID{debitShard, creditShard},
	}, 3*time.Second)

	// Deliver the commit outcome three times to each side.
	for range 3 {
		sc.Groups[debitShard].Propose(shard.Command{
			Op: shard.OpOutcome, TxID: "dup", Ledger: cmd, Commit: true, Debit: true,
		}, 3*time.Second)
		sc.Groups[creditShard].Propose(shard.Command{
			Op: shard.OpOutcome, TxID: "dup", Ledger: cmd, Commit: true, Debit: false,
		}, 3*time.Second)
	}

	if got, _ := sc.Balance(a); got != 7500 {
		t.Fatalf("debit applied more than once: %s = %s, want 75.00", a, got)
	}
	if got, _ := sc.Balance(b); got != 2500 {
		t.Fatalf("credit applied more than once: %s = %s, want 25.00", b, got)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("duplicate delivery changed the total: %s -> %s", total, got)
	}
}

// --- THE blocking problem (§12) ------------------------------------------

// 2PC is a blocking protocol. If the coordinator dies after participants voted
// YES but before the decision reaches them, those participants are stuck: they
// promised, so they may not abort unilaterally, and they were never told to
// commit.
//
// This test PROVES the window exists, then proves recovery ends it. Hiding this
// would misrepresent 2PC — it is inherent (§12, Bernstein ch.7), not a bug.
func TestCoordinatorCrashLeavesParticipantInDoubtThenRecovers(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 8)

	a, b := findCrossShardPair(sc)
	sc.Open(a, 10000)
	sc.Open(b, 1000)
	total := sc.TotalMoney()

	debitShard := sc.Coordinator.ShardFor(a)
	creditShard := sc.Coordinator.ShardFor(b)
	cmd := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "blocked", From: a, To: b, Amount: 3000}
	parts := []shard.ID{debitShard, creditShard}

	// Phase 1 completes: both participants vote YES and log prepare records.
	if _, _, err := sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "blocked", Ledger: cmd, Debit: true, Participants: parts,
	}, 3*time.Second); err != nil {
		t.Fatalf("debit prepare: %v", err)
	}
	if _, _, err := sc.Groups[creditShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "blocked", Ledger: cmd, Debit: false, Participants: parts,
	}, 3*time.Second); err != nil {
		t.Fatalf("credit prepare: %v", err)
	}

	// ...and now the coordinator dies before logging or delivering any decision.

	// The participants are BLOCKED. This is the in-doubt state.
	inDoubt := sc.InDoubt()
	if inDoubt == 0 {
		t.Fatalf("expected in-doubt transactions after prepare with no decision%s", sc.View())
	}
	t.Logf("in-doubt transactions while the coordinator is down: %d%s", inDoubt, sc.View())

	// Critically: the funds are held, not lost. Conservation still holds.
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money changed while blocked: %s -> %s", total, got)
	}
	if avail, _ := sc.Groups[debitShard].Machine().State.Available(a); avail != 7000 {
		t.Fatalf("available = %s while in doubt, want 70.00 (funds should be held)", avail)
	}

	// RECOVERY. No decision was ever logged, so no participant can have committed
	// — aborting is the safe resolution, and it is what recovery must choose.
	resolved, err := sc.Coordinator.RecoverInDoubt()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	t.Logf("recovery resolved %d transactions", resolved)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sc.InDoubt() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := sc.InDoubt(); n != 0 {
		t.Fatalf("%d transactions still in doubt after recovery%s", n, sc.View())
	}

	// Aborted cleanly: balances untouched, reservations released, money intact.
	if got, _ := sc.Balance(a); got != 10000 {
		t.Fatalf("%s = %s after recovery-abort, want 100.00", a, got)
	}
	if got, _ := sc.Balance(b); got != 1000 {
		t.Fatalf("%s = %s after recovery-abort, want 10.00", b, got)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated across the whole episode: %s -> %s", total, got)
	}
}

// If the coordinator DID log its commit decision before dying, recovery must
// COMMIT, not abort. The decision is durable in the coordinator's Raft log, which
// is exactly why logging it through Raft matters.
func TestRecoveryHonoursDurableCommitDecision(t *testing.T) {
	sc := newRunningCluster(t, 2, 3, 9)

	a, b := findCrossShardPair(sc)
	sc.Open(a, 10000)
	sc.Open(b, 1000)
	total := sc.TotalMoney()

	debitShard := sc.Coordinator.ShardFor(a)
	creditShard := sc.Coordinator.ShardFor(b)
	cmd := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "durable", From: a, To: b, Amount: 2000}
	parts := []shard.ID{debitShard, creditShard}

	// Both prepare.
	sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "durable", Ledger: cmd, Debit: true, Participants: parts,
	}, 3*time.Second)
	sc.Groups[creditShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "durable", Ledger: cmd, Debit: false, Participants: parts,
	}, 3*time.Second)

	// The coordinator logs COMMIT through its own Raft group — then dies before
	// telling anyone.
	sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpDecision, TxID: "durable", Ledger: cmd,
		Commit: true, Debit: true, Participants: parts,
	}, 3*time.Second)

	// Recovery must find that decision and commit, not abort.
	if _, err := sc.Coordinator.RecoverInDoubt(); err != nil {
		t.Fatalf("recovery: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sc.InDoubt() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if got, _ := sc.Balance(a); got != 8000 {
		t.Fatalf("%s = %s, want 80.00 — recovery did not honour the durable COMMIT decision%s",
			a, got, sc.View())
	}
	if got, _ := sc.Balance(b); got != 3000 {
		t.Fatalf("%s = %s, want 30.00 — credit side did not commit%s", b, got, sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s", total, got)
	}
}

// --- Chaos: conservation under repeated cross-shard load -----------------

// The bottom line for a bank: run many cross-shard transfers, some failing, and
// the total must be exactly unchanged.
func TestManyCrossShardTransfersConserveMoney(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping longer chaos run in -short mode")
	}
	sc := newRunningCluster(t, 3, 3, 10)

	var accounts []ledger.AccountID
	for i := range 6 {
		a := ledger.AccountID(fmt.Sprintf("acct-%d", i))
		if err := sc.Open(a, 10000); err != nil {
			t.Fatalf("open %s: %v", a, err)
		}
		accounts = append(accounts, a)
	}
	total := sc.TotalMoney()
	t.Logf("starting total: %s across %d shards", total, len(sc.Groups))

	committed, aborted := 0, 0
	for i := range 30 {
		from := accounts[i%len(accounts)]
		to := accounts[(i*3+1)%len(accounts)]
		if from == to {
			continue
		}
		_, err := sc.Coordinator.Transfer(shard.TxID(fmt.Sprintf("bulk-%d", i)), ledger.Command{
			Op: ledger.OpTransfer, IdempotencyKey: fmt.Sprintf("bulk-%d", i),
			From: from, To: to, Amount: ledger.Money(500 + (i*37)%2000),
		})
		if err == nil {
			committed++
		} else {
			aborted++
		}

		// The invariant must hold after EVERY transfer, not merely at the end.
		if got := sc.TotalMoney(); got != total {
			t.Fatalf("money conservation violated after transfer %d: %s -> %s%s",
				i, total, got, sc.View())
		}
	}

	t.Logf("%d committed, %d aborted%s", committed, aborted, sc.View())
	if committed == 0 {
		t.Fatal("no cross-shard transfer committed — the protocol is not working")
	}

	// Resolve anything left blocked, then check conservation one final time.
	sc.Coordinator.RecoverInDoubt()
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("final conservation check failed: %s -> %s", total, got)
	}
}

// --- Sharding and write capacity: what this harness can and cannot show ---

// NOW.md's claim is that write scaling comes from sharding rather than from
// adding replicas. Phase 1 proved the second half by measurement (3 nodes
// 119.9 tx/s vs 5 nodes 105.9 tx/s over real TCP).
//
// The first half is NOT measurable in this in-process simulator, and this test
// documents why rather than pretending otherwise.
//
// Measured evidence: a single shard doing a fixed workload runs at ~169k tx/s in
// a 1-shard cluster and ~55k tx/s in a 4-shard cluster — while the other three
// shards sit completely IDLE. Idle Raft groups cannot slow another group's
// commits; there is no shared lock, no shared network, and no shared state
// machine between groups (each node has its own). The slowdown is the simulator
// itself: every node runs a 5ms ticker goroutine and spawns a goroutine per peer
// per replication round, so total scheduling overhead grows with node count
// regardless of which nodes are doing useful work.
//
// In other words the harness measures itself, not the system. Reporting a
// throughput ratio from it would be reporting an artifact.
//
// What this test DOES verify is the structural property that makes sharding scale
// in reality: shards commit independently, so a write to one shard does not enter
// another shard's log at all. That is checkable and meaningful here. The
// throughput claim itself belongs to a real multi-process benchmark over TCP
// (rpc/), which is where Phase 1's numbers came from — noted in DESIGN.md as
// outstanding.
func TestShardsCommitIndependently(t *testing.T) {
	sc := newRunningCluster(t, 3, 3, 11)

	// One account on each of two different shards.
	a, b := findCrossShardPair(sc)
	sc.Open(a, 100000)
	sc.Open(b, 100000)
	shardA := sc.Coordinator.ShardFor(a)
	shardB := sc.Coordinator.ShardFor(b)

	logLen := func(sid shard.ID) int {
		g := sc.Groups[sid]
		return len(g.Nodes[g.leader()].LogEntries())
	}

	beforeA, beforeB := logLen(shardA), logLen(shardB)

	// Twenty single-shard writes, all to shard A only.
	for i := range 20 {
		key := fmt.Sprintf("indep-%d", i)
		if _, err := sc.Coordinator.Transfer(shard.TxID(key), ledger.Command{
			Op: ledger.OpDeposit, IdempotencyKey: key, To: a, Amount: 1,
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	afterA, afterB := logLen(shardA), logLen(shardB)
	grewA, grewB := afterA-beforeA, afterB-beforeB

	t.Logf("20 writes to %s: %s log grew by %d entries, %s log grew by %d",
		shardA, shardA, grewA, shardB, grewB)

	if grewA < 20 {
		t.Fatalf("%s log grew by only %d entries for 20 writes", shardA, grewA)
	}
	// The point: shard B's log did NOT take those writes. Its leader committed
	// nothing on their behalf, which is exactly why adding shards adds capacity
	// while adding replicas does not.
	if grewB > 0 {
		t.Fatalf("%s log grew by %d entries from writes addressed to %s — "+
			"shards are not committing independently", shardB, grewB, shardA)
	}
}
