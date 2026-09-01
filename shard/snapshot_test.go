package shard

import (
	"testing"

	"github.com/homura/core-bank/ledger"
)

// Snapshot round-trip tests for the shard state machine (G3).
//
// This is the test G3's design named as the first thing to write, because it is
// where compaction can create a money bug: a shard replica's state is its ledger
// PLUS its 2PC promises, and a snapshot that captures only the balances would
// silently destroy the rest the moment the log prefix is discarded.
//
// The property, stated as a bank would: a participant that voted YES is holding
// the customer's funds against an unretractable promise. Compaction is an
// internal housekeeping operation. It must be completely invisible — the promise,
// the reservation, and the audit trail all survive it unchanged.

// prepared builds a machine with one prepared (in-doubt) cross-shard debit.
func prepared(t *testing.T) *Machine {
	t.Helper()
	m := NewMachine("shard-0", ledger.New())

	apply(t, m, Command{Op: OpSingle, Ledger: ledger.Command{
		Op: ledger.OpOpenAccount, IdempotencyKey: "open-alice", To: "alice", Amount: 10000,
	}})
	apply(t, m, Command{
		Op: OpPrepare, TxID: "tx-1", Debit: true,
		Participants: []ID{"shard-0", "shard-1"}, Coordinator: "shard-0",
		Ledger: ledger.Command{
			Op: ledger.OpTransfer, IdempotencyKey: "tx-1",
			From: "alice", To: "bob", Amount: 3000,
		},
	})
	return m
}

func apply(t *testing.T, m *Machine, cmd Command) ledger.Result {
	t.Helper()
	res, ok := m.Apply(cmd.Encode()).(ledger.Result)
	if !ok {
		t.Fatalf("apply returned %T, want ledger.Result", res)
	}
	return res
}

// A snapshot must carry the 2PC promise and the reservation it holds.
//
// Without txs in the snapshot, a compacted node forgets it ever voted; without
// reserves, the money it promised becomes spendable again. Either one lets the
// same money be committed twice.
func TestSnapshotPreservesPreparedPromiseAndReservation(t *testing.T) {
	src := prepared(t)

	// Preconditions: promised, and holding the funds.
	rec, ok := src.Tx("tx-1")
	if !ok || rec.Phase != PhasePrepared {
		t.Fatalf("setup: tx-1 is %+v, want Prepared", rec)
	}
	if avail, _ := src.State.Available("alice"); avail != 7000 {
		t.Fatalf("setup: available = %s, want 70.00", avail)
	}

	data, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a completely fresh machine, as a node that lost its log prefix
	// and came back from the snapshot alone would.
	dst := NewMachine("shard-0", ledger.New())
	if err := dst.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	got, ok := dst.Tx("tx-1")
	if !ok {
		t.Fatal("the restored machine forgot transaction tx-1 — a YES vote is an " +
			"unretractable promise and compaction must not erase it")
	}
	if got.Phase != PhasePrepared {
		t.Fatalf("restored phase = %v, want Prepared", got.Phase)
	}
	if got.Coordinator != "shard-0" {
		t.Fatalf("restored coordinator = %q, want shard-0 — an in-doubt participant "+
			"must know which group to ask for the outcome", got.Coordinator)
	}
	if len(got.Participants) != 2 {
		t.Fatalf("restored participants = %v, want 2", got.Participants)
	}
	if got.Cmd.To != "bob" || got.Cmd.Amount != 3000 {
		t.Fatalf("restored command = %+v; committing the credit leg needs the account "+
			"and amount, which exist nowhere else once the log entry is gone", got.Cmd)
	}

	// The reservation survived: the money is held, not spendable, and not lost.
	if avail, _ := dst.State.Available("alice"); avail != 7000 {
		t.Fatalf("available after restore = %s, want 70.00 — the reservation did not "+
			"survive, so promised money became spendable again", avail)
	}
	if bal, _ := dst.State.Balance("alice"); bal != 10000 {
		t.Fatalf("balance after restore = %s, want 100.00 — reserved money is held, not gone", bal)
	}

	// And it is still in doubt: a restored participant may not resolve itself.
	if n := len(dst.InDoubt()); n != 1 {
		t.Fatalf("InDoubt() = %d after restore, want 1", n)
	}
}

// A restored participant must still be able to COMMIT the promise it kept.
//
// Preserving the record is not enough if the restored state cannot act on it:
// the outcome must apply exactly as it would have without compaction.
func TestRestoredParticipantCanStillCommit(t *testing.T) {
	src := prepared(t)
	data, _ := src.Snapshot()

	dst := NewMachine("shard-0", ledger.New())
	if err := dst.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	res := apply(t, dst, Command{
		Op: OpOutcome, TxID: "tx-1", Commit: true, Debit: true,
		Ledger: ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx-1",
			From: "alice", To: "bob", Amount: 3000},
	})
	if !res.OK {
		t.Fatalf("committing a restored promise failed: %+v", res)
	}

	if bal, _ := dst.State.Balance("alice"); bal != 7000 {
		t.Fatalf("alice = %s after commit, want 70.00", bal)
	}
	if avail, _ := dst.State.Available("alice"); avail != 7000 {
		t.Fatalf("available = %s after commit, want 70.00 — the reservation should be "+
			"consumed, not still held", avail)
	}
	if n := len(dst.InDoubt()); n != 0 {
		t.Fatalf("InDoubt() = %d after commit, want 0", n)
	}
}

// The mirror: a restored participant must still be able to ABORT, releasing the
// funds it was holding.
func TestRestoredParticipantCanStillAbort(t *testing.T) {
	src := prepared(t)
	data, _ := src.Snapshot()

	dst := NewMachine("shard-0", ledger.New())
	dst.RestoreSnapshot(data)

	apply(t, dst, Command{
		Op: OpOutcome, TxID: "tx-1", Commit: false, Debit: true,
		Ledger: ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx-1",
			From: "alice", To: "bob", Amount: 3000},
	})

	if bal, _ := dst.State.Balance("alice"); bal != 10000 {
		t.Fatalf("alice = %s after abort, want 100.00 — an abort must not move money", bal)
	}
	if avail, _ := dst.State.Available("alice"); avail != 10000 {
		t.Fatalf("available = %s after abort, want 100.00 — the reservation was not released", avail)
	}
}

// A decided-but-unapplied transaction must survive with its decision intact.
//
// This is the state recovery cares most about: the coordinator logged COMMIT, so
// the outcome is settled, but this shard has not applied it yet. Losing the
// decision across compaction would make recovery abort a transaction that the
// cluster has already durably committed.
func TestSnapshotPreservesDurableDecision(t *testing.T) {
	m := prepared(t)
	apply(t, m, Command{
		Op: OpDecision, TxID: "tx-1", Commit: true, Debit: true,
		Participants: []ID{"shard-0", "shard-1"}, Coordinator: "shard-0",
		Ledger: ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx-1",
			From: "alice", To: "bob", Amount: 3000},
	})

	data, _ := m.Snapshot()
	dst := NewMachine("shard-0", ledger.New())
	if err := dst.RestoreSnapshot(data); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	commit, known := dst.Decision("tx-1")
	if !known {
		t.Fatal("the restored machine lost a DURABLE COMMIT decision; recovery would " +
			"then abort a transaction the cluster already committed")
	}
	if !commit {
		t.Fatal("the restored decision says abort, but COMMIT was logged")
	}
}

// A transaction that voted NO must stay aborted, and its id must stay unusable.
//
// A restored machine that forgot the NO would let the same transaction id be
// re-prepared, so one id could carry both an abort and a later commit.
func TestSnapshotPreservesNoVoteAndSingleUseIDs(t *testing.T) {
	m := NewMachine("shard-0", ledger.New())
	apply(t, m, Command{Op: OpSingle, Ledger: ledger.Command{
		Op: ledger.OpOpenAccount, IdempotencyKey: "open-poor", To: "poor", Amount: 100,
	}})
	// Prepare for more than the account holds: a NO vote.
	res := apply(t, m, Command{
		Op: OpPrepare, TxID: "tx-no", Debit: true, Coordinator: "shard-0",
		Ledger: ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx-no",
			From: "poor", To: "rich", Amount: 99999},
	})
	if res.OK {
		t.Fatalf("setup: prepare should have voted NO, got %+v", res)
	}

	data, _ := m.Snapshot()
	dst := NewMachine("shard-0", ledger.New())
	dst.RestoreSnapshot(data)

	rec, ok := dst.Tx("tx-no")
	if !ok || rec.Phase != PhaseAborted {
		t.Fatalf("restored record = %+v (ok=%v), want Aborted — a restart must not turn "+
			"a NO into a maybe", rec, ok)
	}

	// The id must still be single-use.
	again := apply(t, dst, Command{
		Op: OpPrepare, TxID: "tx-no", Debit: true, Coordinator: "shard-0",
		Ledger: ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx-no",
			From: "poor", To: "rich", Amount: 50},
	})
	if again.OK {
		t.Fatal("an aborted transaction id was reusable after restore; one id could " +
			"then carry both an abort and a later commit")
	}
}

// Single-shard idempotency results must survive, or a retried request after
// compaction executes a second time.
func TestSnapshotPreservesIdempotencyResults(t *testing.T) {
	m := NewMachine("shard-0", ledger.New())
	apply(t, m, Command{Op: OpSingle, Ledger: ledger.Command{
		Op: ledger.OpOpenAccount, IdempotencyKey: "open-x", To: "x", Amount: 5000,
	}})
	apply(t, m, Command{Op: OpSingle, Ledger: ledger.Command{
		Op: ledger.OpWithdraw, IdempotencyKey: "w-1", From: "x", Amount: 1000,
	}})

	data, _ := m.Snapshot()
	dst := NewMachine("shard-0", ledger.New())
	dst.RestoreSnapshot(data)

	if _, ok := dst.Result("w-1"); !ok {
		t.Fatal("the restored machine lost the idempotency result for w-1; a retry " +
			"would withdraw a second time")
	}

	// Re-applying the same command must not move money again.
	before, _ := dst.State.Balance("x")
	apply(t, dst, Command{Op: OpSingle, Ledger: ledger.Command{
		Op: ledger.OpWithdraw, IdempotencyKey: "w-1", From: "x", Amount: 1000,
	}})
	after, _ := dst.State.Balance("x")
	if after != before {
		t.Fatalf("a retried withdrawal after restore moved money: %s -> %s", before, after)
	}
}

// Snapshots must be deterministic: the same state must encode to the same bytes.
//
// Go randomizes map iteration, so an unsorted encoding would differ run to run.
// That is harmless for correctness but makes snapshots impossible to compare —
// and comparing them is exactly how a test proves compaction changed nothing.
func TestSnapshotEncodingIsDeterministic(t *testing.T) {
	m := prepared(t)

	first, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for i := range 20 {
		again, err := m.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot %d: %v", i, err)
		}
		if string(again) != string(first) {
			t.Fatalf("snapshot %d differs from the first for identical state; map "+
				"iteration order is leaking into the encoding", i)
		}
	}
}

// A snapshot must round-trip to a byte-identical snapshot. If restoring loses or
// reorders anything, re-snapshotting exposes it.
func TestSnapshotRoundTripIsStable(t *testing.T) {
	src := prepared(t)
	first, _ := src.Snapshot()

	dst := NewMachine("shard-0", ledger.New())
	if err := dst.RestoreSnapshot(first); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	second, err := dst.Snapshot()
	if err != nil {
		t.Fatalf("re-Snapshot: %v", err)
	}

	if string(first) != string(second) {
		t.Fatalf("snapshot -> restore -> snapshot is not stable (%d vs %d bytes); "+
			"something was lost or altered in the round trip", len(first), len(second))
	}
}

// A truncated snapshot must be refused outright, leaving the machine untouched.
//
// A half-restored shard machine — one snapshot's promises against another's
// balances — is worse than one that refused the snapshot, because it looks valid.
func TestTruncatedSnapshotIsRefusedAndLeavesStateIntact(t *testing.T) {
	src := prepared(t)
	data, _ := src.Snapshot()

	dst := prepared(t) // has its own valid state to protect
	before, _ := dst.Snapshot()

	for _, cut := range []int{1, len(data) / 3, len(data) / 2, len(data) - 1} {
		if err := dst.RestoreSnapshot(data[:cut]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes was accepted", cut, len(data))
		}
	}

	after, _ := dst.Snapshot()
	if string(before) != string(after) {
		t.Fatal("a refused snapshot still modified the machine; a failed restore must " +
			"leave the previous state exactly as it was")
	}
}
