package sim

import (
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/shard"
)

// Durability of 2PC state across a restart.
//
// DESIGN.md §11: "Nothing about the 2PC state lives in memory. That is the whole
// point: an in-memory coordinator cannot survive the failure the protocol exists
// to handle." These tests hold the implementation to that sentence.
//
// The property under test, stated as a bank would state it: a participant that
// voted YES has made an unretractable promise. It reserved the customer's funds.
// If the process dies and comes back, the promise and the reservation must both
// still be there — because the coordinator may commit that transaction at any
// moment, and it can only do so safely if every yes-voter still remembers.
//
// Per RULES.md rule 3 these cover multiple flows: the normal restart, the
// restart-then-commit path, the restart-then-abort path, and the retry path
// (a decision delivered again after recovery).

// openWithRetry opens an account, retrying while the owning shard has no leader.
//
// Setup, not the property under test. Under a full parallel -race run the
// simulator's tight election timers (60-120ms) can lose an election to CPU
// starvation between WaitForLeaders returning and the write being proposed —
// the same §5.2 timing-violated-by-the-machine effect the sharded throughput
// benchmark measured. A transient leaderless moment during setup must not be
// reported as a failure of recovery.
func openWithRetry(t *testing.T, sc *ShardCluster, account ledger.AccountID,
	amount ledger.Money, timeout time.Duration) error {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := sc.Open(account, amount); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return lastErr
}

// waitForBalance waits until the shard leader reports the expected balance.
//
// Necessary because ShardCluster.Balance reads the CURRENT LEADER's state
// machine, and after a restart a node that had not yet applied the newest entries
// can win the election. It then serves a stale balance until replication catches
// it up — which is correct Raft behaviour, not a bug: a follower's state is
// allowed to lag, and this is precisely why §8 makes a linearizable read confirm
// with a majority first rather than reading local state.
//
// A test that asserts a balance immediately after an election is asserting
// against whichever replica happened to win, so it must either wait for
// convergence or perform a linearizable read. This waits.
func waitForBalance(t *testing.T, sc *ShardCluster, account ledger.AccountID,
	want ledger.Money, timeout time.Duration) ledger.Money {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var got ledger.Money
	for time.Now().Before(deadline) {
		got, _ = sc.Balance(account)
		if got == want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return got
}

// prepareCrossShard drives phase 1 of 2PC by hand and returns the two shards
// involved. Driving it by hand — rather than through Coordinator.Transfer — is
// what lets the test stop the protocol precisely between prepare and decision,
// which is the window where a crash is dangerous.
func prepareCrossShard(t *testing.T, sc *ShardCluster, txID shard.TxID,
	from, to ledger.AccountID, amount ledger.Money) (debitShard, creditShard shard.ID) {
	t.Helper()

	debitShard = sc.Coordinator.ShardFor(from)
	creditShard = sc.Coordinator.ShardFor(to)
	cmd := ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: string(txID),
		From: from, To: to, Amount: amount,
	}
	parts := []shard.ID{debitShard, creditShard}

	res, _, err := sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: txID, Ledger: cmd, Debit: true,
		Participants: parts, Coordinator: debitShard,
	}, 3*time.Second)
	if err != nil || !res.OK {
		t.Fatalf("debit prepare: err=%v res=%+v", err, res)
	}

	res, _, err = sc.Groups[creditShard].Propose(shard.Command{
		Op: shard.OpPrepare, TxID: txID, Ledger: cmd, Debit: false,
		Participants: parts, Coordinator: debitShard,
	}, 3*time.Second)
	if err != nil || !res.OK {
		t.Fatalf("credit prepare: err=%v res=%+v", err, res)
	}
	return debitShard, creditShard
}

// A participant that voted YES must still be prepared, and must still be holding
// the reserved funds, after every node in the cluster restarts.
//
// This is the core durability property. Before the harness persisted shard
// groups, the restarted cluster came back with empty ledgers: the account did not
// exist, the promise was gone, and the reservation had evaporated — meaning the
// coordinator could commit a transaction whose debit leg no longer had any funds
// held for it.
func TestPreparedParticipantSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var debitShard, creditShard shard.ID
	var total ledger.Money

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 21, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders%s", sc.View())
		}

		a, b = findCrossShardPair(sc)
		if err := sc.Open(a, 10000); err != nil {
			t.Fatalf("open %s: %v", a, err)
		}
		if err := sc.Open(b, 1000); err != nil {
			t.Fatalf("open %s: %v", b, err)
		}
		total = sc.TotalMoney()

		debitShard, creditShard = prepareCrossShard(t, sc, "survive", a, b, 3000)

		// Precondition: prepared, funds held, nothing committed.
		if avail, _ := sc.Groups[debitShard].Machine().State.Available(a); avail != 7000 {
			t.Fatalf("available before crash = %s, want 70.00", avail)
		}
		if n := sc.InDoubt(); n == 0 {
			t.Fatalf("expected an in-doubt transaction before the crash%s", sc.View())
		}
	}()

	// Every node in both shards is now dead. Bring the whole cluster back over the
	// same files — the same thing that happens when the processes are restarted.
	sc := NewShardClusterWithStorage(t, 2, 3, 21, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	// The promise survived.
	rec, ok := sc.Groups[debitShard].Machine().Tx("survive")
	if !ok {
		t.Fatalf("debit shard forgot transaction 'survive' across the restart — "+
			"a YES vote is an unretractable promise and must be durable%s", sc.View())
	}
	if rec.Phase != shard.PhasePrepared {
		t.Fatalf("debit shard phase = %v after restart, want Prepared", rec.Phase)
	}
	if _, ok := sc.Groups[creditShard].Machine().Tx("survive"); !ok {
		t.Fatalf("credit shard forgot transaction 'survive' across the restart%s", sc.View())
	}

	// The reservation survived: the money is still held, not spendable.
	if avail, _ := sc.Groups[debitShard].Machine().State.Available(a); avail != 7000 {
		t.Fatalf("available after restart = %s, want 70.00 — the reservation did not survive, "+
			"so the promised money became spendable again%s", avail, sc.View())
	}
	if bal, _ := sc.Balance(a); bal != 10000 {
		t.Fatalf("balance after restart = %s, want 100.00 — reserved money is held, not gone", bal)
	}

	// Still in doubt, and still conserving money.
	if n := sc.InDoubt(); n == 0 {
		t.Fatalf("transaction is no longer in doubt after restart; a restarted participant "+
			"must not resolve its own promise%s", sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated across the restart: %s -> %s", total, got)
	}
}

// Having survived the restart, the promise must still be honourable: recovery
// finds the durable COMMIT decision and both legs apply.
//
// This is the flow that matters most in a bank. The coordinator logged COMMIT,
// then everything died. The customer's money has been promised. On restart the
// system must complete the transfer, not abort it — the decision is already made
// and is durable in the coordinator's own Raft log.
func TestRestartedParticipantHonoursDurableCommit(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var total ledger.Money

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 22, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders%s", sc.View())
		}

		a, b = findCrossShardPair(sc)
		sc.Open(a, 10000)
		sc.Open(b, 1000)
		total = sc.TotalMoney()

		debitShard, creditShard := prepareCrossShard(t, sc, "durable-restart", a, b, 2000)
		cmd := ledger.Command{
			Op: ledger.OpTransfer, IdempotencyKey: "durable-restart",
			From: a, To: b, Amount: 2000,
		}

		// The coordinator logs COMMIT through its own Raft group, then everything
		// dies before the outcome reaches anyone.
		if _, _, err := sc.Groups[debitShard].Propose(shard.Command{
			Op: shard.OpDecision, TxID: "durable-restart", Ledger: cmd, Commit: true,
			Debit: true, Participants: []shard.ID{debitShard, creditShard},
			Coordinator: debitShard,
		}, 3*time.Second); err != nil {
			t.Fatalf("decision: %v", err)
		}
	}()

	sc := NewShardClusterWithStorage(t, 2, 3, 22, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	// Recovery must find the durable decision and finish the job.
	if _, err := sc.Coordinator.RecoverInDoubt(); err != nil {
		t.Fatalf("recovery after restart: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sc.InDoubt() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if got, _ := sc.Balance(a); got != 8000 {
		t.Fatalf("%s = %s after restart+recovery, want 80.00 — the durable COMMIT was not "+
			"honoured across the restart%s", a, got, sc.View())
	}
	if got, _ := sc.Balance(b); got != 3000 {
		t.Fatalf("%s = %s after restart+recovery, want 30.00 — credit leg did not apply%s",
			b, got, sc.View())
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s", total, got)
	}
}

// The mirror case: no decision was ever logged, so recovery after the restart
// must ABORT and release the held funds. A restarted participant must not invent
// a commit any more than it may invent an abort.
func TestRestartedParticipantAbortsWhenNoDecisionWasLogged(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var total ledger.Money

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 23, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders%s", sc.View())
		}

		a, b = findCrossShardPair(sc)
		sc.Open(a, 10000)
		sc.Open(b, 1000)
		total = sc.TotalMoney()

		prepareCrossShard(t, sc, "no-decision", a, b, 4000)
	}()

	sc := NewShardClusterWithStorage(t, 2, 3, 23, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	// The funds are still held on restart — the participant may not release them
	// on its own initiative, only through recovery.
	if avail, _ := sc.Balance(a); avail != 10000 {
		t.Fatalf("balance after restart = %s, want 100.00", avail)
	}

	if _, err := sc.Coordinator.RecoverInDoubt(); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sc.InDoubt() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if n := sc.InDoubt(); n != 0 {
		t.Fatalf("%d transactions still in doubt after restart+recovery%s", n, sc.View())
	}
	if got, _ := sc.Balance(a); got != 10000 {
		t.Fatalf("%s = %s after recovery-abort, want 100.00", a, got)
	}
	if got, _ := sc.Balance(b); got != 1000 {
		t.Fatalf("%s = %s after recovery-abort, want 10.00", b, got)
	}
	// The reservation is released: the money is spendable again.
	avail, _ := sc.Groups[sc.Coordinator.ShardFor(a)].Machine().State.Available(a)
	if avail != 10000 {
		t.Fatalf("available after abort = %s, want 100.00 — reservation not released", avail)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s", total, got)
	}
}

// Retry flow: after a restart, a decision that is delivered twice must apply
// once. Replay reconstructs the transaction record, and a redelivered outcome
// must hit the terminal-phase guard rather than debiting a second time.
func TestRedeliveredOutcomeAfterRestartIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	var a, b ledger.AccountID
	var total ledger.Money
	var debitShard, creditShard shard.ID

	func() {
		sc := NewShardClusterWithStorage(t, 2, 3, 24, dir)
		sc.Start()
		defer sc.Stop()
		if !sc.WaitForLeaders(5 * time.Second) {
			t.Fatalf("no leaders%s", sc.View())
		}

		a, b = findCrossShardPair(sc)
		sc.Open(a, 10000)
		sc.Open(b, 1000)
		total = sc.TotalMoney()

		debitShard, creditShard = prepareCrossShard(t, sc, "twice", a, b, 2500)
		cmd := ledger.Command{
			Op: ledger.OpTransfer, IdempotencyKey: "twice", From: a, To: b, Amount: 2500,
		}
		parts := []shard.ID{debitShard, creditShard}

		// Decision logged AND both outcomes applied, before the crash.
		sc.Groups[debitShard].Propose(shard.Command{
			Op: shard.OpDecision, TxID: "twice", Ledger: cmd, Commit: true, Debit: true,
			Participants: parts, Coordinator: debitShard,
		}, 3*time.Second)
		sc.Groups[debitShard].Propose(shard.Command{
			Op: shard.OpOutcome, TxID: "twice", Ledger: cmd, Commit: true, Debit: true,
		}, 3*time.Second)
		sc.Groups[creditShard].Propose(shard.Command{
			Op: shard.OpOutcome, TxID: "twice", Ledger: cmd, Commit: true, Debit: false,
		}, 3*time.Second)

		if got, _ := sc.Balance(a); got != 7500 {
			t.Fatalf("%s = %s before crash, want 75.00", a, got)
		}
	}()

	sc := NewShardClusterWithStorage(t, 2, 3, 24, dir)
	if err := sc.RestoreAll(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	sc.startRestored(t, 5*time.Second)

	// Replay must reproduce the committed balances exactly.
	//
	// Waiting for convergence rather than reading immediately: after the restart a
	// replica that had not yet applied the outcome entry can win the election and
	// serve a stale balance until replication catches it up. That is correct Raft
	// behaviour — followers are allowed to lag — so the assertion is that the
	// cluster CONVERGES on the committed state, not that it is instantaneous.
	if got := waitForBalance(t, sc, a, 7500, 3*time.Second); got != 7500 {
		t.Fatalf("%s = %s after restart, want 75.00 — replay did not reproduce the "+
			"committed transfer%s", a, got, sc.View())
	}
	if got := waitForBalance(t, sc, b, 3500, 3*time.Second); got != 3500 {
		t.Fatalf("%s = %s after restart, want 35.00", b, got)
	}

	// Now redeliver both outcomes. The transaction is terminal; nothing may move.
	cmd := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "twice", From: a, To: b, Amount: 2500}
	sc.Groups[debitShard].Propose(shard.Command{
		Op: shard.OpOutcome, TxID: "twice", Ledger: cmd, Commit: true, Debit: true,
	}, 3*time.Second)
	sc.Groups[creditShard].Propose(shard.Command{
		Op: shard.OpOutcome, TxID: "twice", Ledger: cmd, Commit: true, Debit: false,
	}, 3*time.Second)

	if got := waitForBalance(t, sc, a, 7500, 3*time.Second); got != 7500 {
		t.Fatalf("%s = %s after redelivery, want 75.00 — the transfer was applied twice", a, got)
	}
	if got := waitForBalance(t, sc, b, 3500, 3*time.Second); got != 3500 {
		t.Fatalf("%s = %s after redelivery, want 35.00 — the credit was applied twice", b, got)
	}
	if got := sc.TotalMoney(); got != total {
		t.Fatalf("money conservation violated: %s -> %s", total, got)
	}
}

// Recovery must not race the apply loop.
//
// A freshly elected leader — especially one that has just restarted and is still
// replaying its log — may not yet have APPLIED the prepare entries that put
// transactions in doubt. A recovery scan at that instant sees an empty in-doubt
// set, reports success, and leaves real blocked transactions behind holding
// customer funds.
//
// This was a live bug, found while landing snapshotting: the single
// RecoverInDoubt() call in the restart tests resolved one shard and silently
// skipped the other, intermittently, in roughly 2 runs out of 10. The
// intermittency is what made it dangerous — it looked like flakiness, and the
// obvious "fix" of retrying in the test would have hidden a real defect in
// recovery rather than fixing it.
//
// Coordinator.RecoverInDoubt now commits a no-op through each group and waits for
// it to apply before scanning, so everything ordered before the scan has applied.
// This test runs the whole restart-and-recover cycle repeatedly, because a race
// that reproduces 2 times in 10 is not disproved by a single green run.
func TestRecoveryIsNotRacedByTheApplyLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the restart cycle repeatedly; skipped under -short")
	}

	// 18 rounds: the underlying race reproduced in roughly 2 runs of 10, so a
	// handful of rounds could pass by luck. 18 makes a silent regression very
	// unlikely while keeping the cost reasonable — each round builds and restarts
	// two 6-node clusters, and this runs alongside every other test under -race.
	const rounds = 18
	for round := range rounds {
		dir := t.TempDir()
		var a, b ledger.AccountID
		var total ledger.Money

		func() {
			sc := NewShardClusterWithStorage(t, 2, 3, int64(100+round), dir)
			sc.Start()
			defer sc.Stop()
			if !sc.WaitForLeaders(5 * time.Second) {
				t.Fatalf("round %d: no leaders", round)
			}
			a, b = findCrossShardPair(sc)
			if err := openWithRetry(t, sc, a, 10000, 5*time.Second); err != nil {
				t.Fatalf("round %d: open %s: %v", round, a, err)
			}
			if err := openWithRetry(t, sc, b, 1000, 5*time.Second); err != nil {
				t.Fatalf("round %d: open %s: %v", round, b, err)
			}
			total = sc.TotalMoney()
			prepareCrossShard(t, sc, "racy", a, b, 4000)
		}()

		sc := NewShardClusterWithStorage(t, 2, 3, int64(100+round), dir)
		if err := sc.RestoreAll(); err != nil {
			t.Fatalf("round %d: restore: %v", round, err)
		}
		sc.Start()
		if !sc.WaitForLeaders(5 * time.Second) {
			sc.Stop()
			t.Fatalf("round %d: no leaders after restart", round)
		}

		// ONE recovery call, made as soon as leaders exist — deliberately the most
		// hostile timing, and exactly what the production leadership-change hook
		// does. It must resolve every shard, not whichever one happens to be ready.
		if _, err := sc.Coordinator.RecoverInDoubt(); err != nil {
			sc.Stop()
			t.Fatalf("round %d: recovery: %v", round, err)
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && sc.InDoubt() > 0 {
			time.Sleep(10 * time.Millisecond)
		}

		remaining := sc.InDoubt()
		balA, _ := sc.Balance(a)
		got := sc.TotalMoney()
		view := sc.View()
		sc.Stop()

		if remaining != 0 {
			t.Fatalf("round %d: %d transaction(s) still in doubt after a single "+
				"post-restart recovery — the scan raced the apply loop%s", round, remaining, view)
		}
		if balA != 10000 {
			t.Fatalf("round %d: %s = %s after recovery-abort, want 100.00", round, a, balA)
		}
		if got != total {
			t.Fatalf("round %d: money conservation violated: %s -> %s", round, total, got)
		}
	}
	t.Logf("%d restart-and-recover rounds, every shard resolved by a single "+
		"recovery call each time", rounds)
}
