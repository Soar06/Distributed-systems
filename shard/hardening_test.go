package shard

import (
	"testing"

	"github.com/homura/core-bank/ledger"
)

// Regression tests for money bugs found in the production-readiness audit.
//
// Every one of these passed the old suite. The unifying defect was that
// "this shouldn't happen" branches returned success or guessed, instead of
// failing loudly — for a ledger, that default is inverted.

func newTestMachine(t *testing.T, balances map[ledger.AccountID]ledger.Money) (*Machine, *ledger.State) {
	t.Helper()
	st := ledger.New()
	for id, amt := range balances {
		res := st.Apply(ledger.Command{
			Op: ledger.OpOpenAccount, IdempotencyKey: "open-" + string(id),
			To: id, Amount: amt,
		})
		if !res.OK {
			t.Fatalf("open %s: %s", id, res.Err)
		}
	}
	return NewMachine("shard-0", st), st
}

func total(st *ledger.State) ledger.Money {
	var sum ledger.Money
	for _, b := range st.Balances() {
		sum += b
	}
	return sum
}

// An outcome for a transaction this shard never prepared must be REFUSED.
//
// Previously a record was fabricated and committed: the credit side credited
// unconditionally while the debit side silently no-opped, so a lost prepare
// created money outright.
func TestOutcomeWithoutPrepareIsRefused(t *testing.T) {
	m, st := newTestMachine(t, map[ledger.AccountID]ledger.Money{"alice": 1000})
	before := total(st)

	transfer := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "ghost",
		From: "nobody", To: "alice", Amount: 777}

	res := m.Apply(Command{Op: OpOutcome, TxID: "ghost", Ledger: transfer,
		Commit: true, Debit: false}.Encode())

	r, _ := res.(ledger.Result)
	if r.OK {
		t.Fatal("applied an outcome for a transaction that was never prepared")
	}
	if got := total(st); got != before {
		t.Fatalf("money changed: %s -> %s (delta %s)", before, got, got-before)
	}
}

// CommitDebit must report failure when it holds no reservation, rather than
// claiming success for work it did not do.
func TestCommitDebitWithoutReservationFails(t *testing.T) {
	_, st := newTestMachine(t, map[ledger.AccountID]ledger.Money{"alice": 1000})
	before := total(st)

	res := st.CommitDebit("never-prepared")
	if res.OK {
		t.Fatal("CommitDebit reported success with no reservation held")
	}
	if got := total(st); got != before {
		t.Fatalf("money changed: %s -> %s", before, got)
	}
}

// A prepare must not resurrect or overwrite a transaction that already reached a
// terminal phase, which previously erased durable COMMIT decisions.
func TestPrepareCannotEraseADecision(t *testing.T) {
	m, _ := newTestMachine(t, map[ledger.AccountID]ledger.Money{"alice": 10000})

	transfer := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx1",
		From: "alice", To: "bob", Amount: 2000}
	parts := []ID{"shard-0", "shard-1"}

	m.Apply(Command{Op: OpPrepare, TxID: "tx1", Ledger: transfer, Debit: true,
		Participants: parts, Coordinator: "shard-0"}.Encode())
	m.Apply(Command{Op: OpDecision, TxID: "tx1", Ledger: transfer, Commit: true,
		Debit: true, Participants: parts, Coordinator: "shard-0"}.Encode())

	commit, known := m.Decision("tx1")
	if !known || !commit {
		t.Fatalf("setup: decision should be known COMMIT, got known=%v commit=%v", known, commit)
	}

	// A late/duplicate prepare arrives for the same id.
	m.Apply(Command{Op: OpPrepare, TxID: "tx1", Ledger: transfer, Debit: true,
		Participants: parts, Coordinator: "shard-0"}.Encode())

	commit, known = m.Decision("tx1")
	if !known {
		t.Fatal("the durable COMMIT decision was erased by a later prepare")
	}
	if !commit {
		t.Fatal("the decision flipped from COMMIT to ABORT")
	}
}

// A transaction id is single-use: an aborted id must not be reusable.
func TestAbortedTransactionIDCannotBeReused(t *testing.T) {
	m, _ := newTestMachine(t, map[ledger.AccountID]ledger.Money{"alice": 100})

	// A prepare that votes NO (insufficient funds) is terminal.
	tooMuch := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx2",
		From: "alice", To: "bob", Amount: 999999}
	m.Apply(Command{Op: OpPrepare, TxID: "tx2", Ledger: tooMuch, Debit: true,
		Participants: []ID{"shard-0", "shard-1"}, Coordinator: "shard-0"}.Encode())

	rec, ok := m.Tx("tx2")
	if !ok || rec.Phase != PhaseAborted {
		t.Fatalf("setup: expected Aborted, got %v (ok=%v)", rec.Phase, ok)
	}

	// Retrying the same id with an affordable amount must be refused.
	affordable := ledger.Command{Op: ledger.OpTransfer, IdempotencyKey: "tx2",
		From: "alice", To: "bob", Amount: 50}
	res := m.Apply(Command{Op: OpPrepare, TxID: "tx2", Ledger: affordable, Debit: true,
		Participants: []ID{"shard-0", "shard-1"}, Coordinator: "shard-0"}.Encode())

	r, _ := res.(ledger.Result)
	if r.OK {
		t.Fatal("an aborted transaction id was reused; ids must be single-use")
	}
	rec, _ = m.Tx("tx2")
	if rec.Phase != PhaseAborted {
		t.Fatalf("phase changed from Aborted to %v", rec.Phase)
	}
}

// A duplicate prepare must not rebind a reservation to a different account or a
// larger amount.
func TestPrepareCannotRebindReservation(t *testing.T) {
	_, st := newTestMachine(t, map[ledger.AccountID]ledger.Money{
		"alice": 1000, "bob": 1000,
	})

	if res := st.PrepareDebit("tx3", "alice", 100); !res.OK {
		t.Fatalf("first prepare failed: %s", res.Err)
	}

	// Same txID, different account AND a far larger amount.
	res := st.PrepareDebit("tx3", "bob", 999999)
	if res.OK {
		t.Fatal("a duplicate prepare rebound the reservation to a different " +
			"account and amount; the coordinator would credit the larger sum " +
			"against the smaller hold")
	}

	if r := st.Reserved("alice"); r != 100 {
		t.Fatalf("alice's reservation changed to %s, want 1.00", r)
	}
	if r := st.Reserved("bob"); r != 0 {
		t.Fatalf("bob has %s reserved, want 0", r)
	}
}

// The Coordinator must be carried explicitly, not inferred from Participants[0].
func TestCoordinatorFieldRoundTrips(t *testing.T) {
	cmd := Command{
		Op: OpPrepare, TxID: "tx4",
		Participants: []ID{"shard-2", "shard-5"},
		Coordinator:  "shard-2",
	}
	got, err := DecodeCommand(cmd.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Coordinator != "shard-2" {
		t.Fatalf("Coordinator = %q, want shard-2", got.Coordinator)
	}
}

// A client must not be able to write into the internal 2PC key namespace, which
// would let it mark a real transfer leg as already applied.
func TestClientCannotForgeInternalIdempotencyKey(t *testing.T) {
	_, st := newTestMachine(t, map[ledger.AccountID]ledger.Money{"alice": 1000})

	res := st.Apply(ledger.Command{
		Op: ledger.OpDeposit, IdempotencyKey: "!2pc!:tx9:credit",
		To: "alice", Amount: 1,
	})
	if res.OK {
		t.Fatal("a client key in the reserved internal namespace was accepted")
	}
}

// Overflow must be refused rather than wrapping a balance negative.
func TestDepositOverflowIsRefused(t *testing.T) {
	_, st := newTestMachine(t, map[ledger.AccountID]ledger.Money{"rich": 0})

	// Fill the account to near the maximum.
	huge := ledger.Money(1<<63 - 1)
	if res := st.Apply(ledger.Command{Op: ledger.OpDeposit, IdempotencyKey: "fill",
		To: "rich", Amount: huge}); !res.OK {
		t.Fatalf("setup deposit failed: %s", res.Err)
	}

	res := st.Apply(ledger.Command{Op: ledger.OpDeposit, IdempotencyKey: "overflow",
		To: "rich", Amount: 1000})
	if res.OK {
		t.Fatal("a deposit that overflows the balance was accepted")
	}
	if bal, _ := st.Balance("rich"); bal < 0 {
		t.Fatalf("balance wrapped negative: %s", bal)
	}
}
