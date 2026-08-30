package ledger

import (
	"fmt"
	"sync"
	"testing"
)

// Tests per RULES.md rule 3: normal, failure, concurrent, and retry flows, with
// the domain invariants (double-entry, conservation of money, determinism)
// asserted directly rather than inferred from balances.

func open(t *testing.T, s *State, id AccountID, amt Money) {
	t.Helper()
	r := s.Apply(Command{Op: OpOpenAccount, IdempotencyKey: "open-" + string(id), To: id, Amount: amt})
	if !r.OK {
		t.Fatalf("open %s: %s", id, r.Err)
	}
}

// --- Normal flows ---------------------------------------------------------

func TestDepositWithdrawTransfer(t *testing.T) {
	s := New()
	open(t, s, "alice", 10000) // $100.00
	open(t, s, "bob", 5000)

	if r := s.Apply(Command{Op: OpDeposit, IdempotencyKey: "d1", To: "alice", Amount: 2500}); !r.OK {
		t.Fatalf("deposit: %s", r.Err)
	}
	if r := s.Apply(Command{Op: OpWithdraw, IdempotencyKey: "w1", From: "alice", Amount: 500}); !r.OK {
		t.Fatalf("withdraw: %s", r.Err)
	}
	if r := s.Apply(Command{Op: OpTransfer, IdempotencyKey: "t1", From: "alice", To: "bob", Amount: 2000}); !r.OK {
		t.Fatalf("transfer: %s", r.Err)
	}

	if got, _ := s.Balance("alice"); got != 10000+2500-500-2000 {
		t.Fatalf("alice = %s, want 100.00", got)
	}
	if got, _ := s.Balance("bob"); got != 7000 {
		t.Fatalf("bob = %s, want 70.00", got)
	}
	if err := s.VerifyDoubleEntry(); err != nil {
		t.Fatalf("double-entry audit failed: %v", err)
	}
}

func TestMoneyFormatting(t *testing.T) {
	cases := []struct {
		m    Money
		want string
	}{
		{0, "0.00"}, {5, "0.05"}, {100, "1.00"}, {12345, "123.45"}, {-250, "-2.50"},
	}
	for _, c := range cases {
		if got := c.m.String(); got != c.want {
			t.Errorf("Money(%d) = %q, want %q", int64(c.m), got, c.want)
		}
	}
}

// --- Failure flows --------------------------------------------------------

func TestWithdrawRejectsInsufficientFunds(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)

	r := s.Apply(Command{Op: OpWithdraw, IdempotencyKey: "w1", From: "alice", Amount: 1001})
	if r.OK {
		t.Fatal("overdraft allowed")
	}
	if r.Err != ErrInsufficientFunds.Error() {
		t.Fatalf("err = %q, want %q", r.Err, ErrInsufficientFunds)
	}
	if got, _ := s.Balance("alice"); got != 1000 {
		t.Fatalf("balance changed on a failed withdrawal: %s", got)
	}
}

func TestTransferRejectsInsufficientFunds(t *testing.T) {
	s := New()
	open(t, s, "alice", 100)
	open(t, s, "bob", 0)

	r := s.Apply(Command{Op: OpTransfer, IdempotencyKey: "t1", From: "alice", To: "bob", Amount: 500})
	if r.OK {
		t.Fatal("transfer beyond balance allowed")
	}
	// Neither side may move — a partial transfer would create or destroy money.
	if a, _ := s.Balance("alice"); a != 100 {
		t.Fatalf("alice = %s, want 1.00", a)
	}
	if b, _ := s.Balance("bob"); b != 0 {
		t.Fatalf("bob = %s, want 0.00", b)
	}
}

func TestRejectsUnknownAccounts(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)

	cases := []Command{
		{Op: OpDeposit, IdempotencyKey: "a", To: "ghost", Amount: 100},
		{Op: OpWithdraw, IdempotencyKey: "b", From: "ghost", Amount: 100},
		{Op: OpTransfer, IdempotencyKey: "c", From: "alice", To: "ghost", Amount: 100},
		{Op: OpTransfer, IdempotencyKey: "d", From: "ghost", To: "alice", Amount: 100},
	}
	for i, c := range cases {
		if r := s.Apply(c); r.OK {
			t.Fatalf("case %d: operation on a nonexistent account succeeded", i)
		}
	}
	if got, _ := s.Balance("alice"); got != 1000 {
		t.Fatalf("alice's balance moved: %s", got)
	}
}

func TestRejectsNonPositiveAmounts(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)
	open(t, s, "bob", 1000)

	// A negative "deposit" would be a withdrawal in disguise, bypassing the
	// balance check.
	for _, c := range []Command{
		{Op: OpDeposit, IdempotencyKey: "n1", To: "alice", Amount: -100},
		{Op: OpWithdraw, IdempotencyKey: "n2", From: "alice", Amount: -100},
		{Op: OpTransfer, IdempotencyKey: "n3", From: "alice", To: "bob", Amount: -100},
		{Op: OpDeposit, IdempotencyKey: "n4", To: "alice", Amount: 0},
	} {
		if r := s.Apply(c); r.OK {
			t.Fatalf("accepted non-positive amount: %+v", c)
		}
	}
	if got, _ := s.Balance("alice"); got != 1000 {
		t.Fatalf("alice = %s, want 10.00", got)
	}
}

func TestRejectsSelfTransfer(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)
	if r := s.Apply(Command{Op: OpTransfer, IdempotencyKey: "t", From: "alice", To: "alice", Amount: 100}); r.OK {
		t.Fatal("self-transfer allowed")
	}
}

func TestRejectsMissingIdempotencyKey(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)
	if r := s.Apply(Command{Op: OpDeposit, To: "alice", Amount: 100}); r.OK {
		t.Fatal("accepted a command with no idempotency key")
	}
}

func TestRejectsDuplicateAccount(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)
	r := s.Apply(Command{Op: OpOpenAccount, IdempotencyKey: "again", To: "alice", Amount: 9999})
	if r.OK {
		t.Fatal("reopened an existing account")
	}
	if got, _ := s.Balance("alice"); got != 1000 {
		t.Fatalf("balance clobbered by duplicate open: %s", got)
	}
}

// --- Retry flow (idempotency) --------------------------------------------

// The core requirement: a client that retries because it did not hear a reply
// must not be charged twice.
func TestRetryDoesNotDoubleProcess(t *testing.T) {
	s := New()
	open(t, s, "alice", 10000)

	cmd := Command{Op: OpWithdraw, IdempotencyKey: "same-key", From: "alice", Amount: 3000}

	first := s.Apply(cmd)
	second := s.Apply(cmd)
	third := s.Apply(cmd)

	if !first.OK || !second.OK || !third.OK {
		t.Fatal("retries should return the original success")
	}
	if got, _ := s.Balance("alice"); got != 7000 {
		t.Fatalf("alice = %s, want 70.00 — retry was processed more than once", got)
	}
	if first.Balance != second.Balance || second.Balance != third.Balance {
		t.Fatalf("retries returned different balances: %s %s %s",
			first.Balance, second.Balance, third.Balance)
	}
	if n := len(s.History()); n != 2 { // open + one withdrawal
		t.Fatalf("history has %d transactions, want 2", n)
	}
}

// A retried command must return its ORIGINAL result even if replaying it now
// would fail. Re-evaluating on retry would make the answer depend on when the
// retry arrived — non-deterministic, and wrong.
func TestRetryReturnsOriginalResultEvenIfNowInvalid(t *testing.T) {
	s := New()
	open(t, s, "alice", 10000)

	cmd := Command{Op: OpWithdraw, IdempotencyKey: "k1", From: "alice", Amount: 6000}
	first := s.Apply(cmd)
	if !first.OK {
		t.Fatal("first withdrawal should succeed")
	}

	// Drain the account so a fresh evaluation of the same command would fail.
	s.Apply(Command{Op: OpWithdraw, IdempotencyKey: "k2", From: "alice", Amount: 4000})

	retry := s.Apply(cmd)
	if !retry.OK {
		t.Fatal("retry must return the original success, not re-evaluate")
	}
	if got, _ := s.Balance("alice"); got != 0 {
		t.Fatalf("alice = %s, want 0.00 — retry re-executed", got)
	}
}

// A failed command's failure is also recorded, so retrying it does not suddenly
// succeed later.
func TestRetryOfFailedCommandStaysFailed(t *testing.T) {
	s := New()
	open(t, s, "alice", 100)

	cmd := Command{Op: OpWithdraw, IdempotencyKey: "fail-key", From: "alice", Amount: 500}
	if r := s.Apply(cmd); r.OK {
		t.Fatal("should have failed")
	}

	s.Apply(Command{Op: OpDeposit, IdempotencyKey: "top-up", To: "alice", Amount: 10000})

	if r := s.Apply(cmd); r.OK {
		t.Fatal("a retried failed command succeeded after the balance changed")
	}
	if got, _ := s.Balance("alice"); got != 10100 {
		t.Fatalf("alice = %s, want 101.00", got)
	}
}

// --- Determinism ----------------------------------------------------------

// The property Raft depends on: same commands, same order, same final state.
func TestSameCommandSequenceProducesSameState(t *testing.T) {
	cmds := []Command{
		{Op: OpOpenAccount, IdempotencyKey: "o1", To: "a", Amount: 5000},
		{Op: OpOpenAccount, IdempotencyKey: "o2", To: "b", Amount: 3000},
		{Op: OpTransfer, IdempotencyKey: "t1", From: "a", To: "b", Amount: 1500},
		{Op: OpWithdraw, IdempotencyKey: "w1", From: "b", Amount: 4000},
		{Op: OpWithdraw, IdempotencyKey: "w2", From: "b", Amount: 999999}, // fails
		{Op: OpDeposit, IdempotencyKey: "d1", To: "a", Amount: 250},
	}

	run := func() (map[AccountID]Money, []Transaction) {
		s := New()
		for _, c := range cmds {
			s.Apply(c)
		}
		return s.Balances(), s.History()
	}

	b1, h1 := run()
	b2, h2 := run()

	if len(b1) != len(b2) {
		t.Fatalf("different account counts: %d vs %d", len(b1), len(b2))
	}
	for id, v := range b1 {
		if b2[id] != v {
			t.Fatalf("non-deterministic: %s = %s vs %s", id, v, b2[id])
		}
	}
	if len(h1) != len(h2) {
		t.Fatalf("different history lengths: %d vs %d", len(h1), len(h2))
	}
	for i := range h1 {
		if h1[i].Seq != h2[i].Seq || h1[i].Op != h2[i].Op {
			t.Fatalf("history diverged at %d", i)
		}
	}
}

// Order matters, and the ledger must reflect that: the same two commands in a
// different order legitimately produce different outcomes. This is exactly why
// Raft must agree on an ORDER, not merely on a set.
func TestOrderChangesOutcome(t *testing.T) {
	withdraw := Command{Op: OpWithdraw, IdempotencyKey: "w", From: "a", Amount: 8000}
	deposit := Command{Op: OpDeposit, IdempotencyKey: "d", To: "a", Amount: 5000}

	s1 := New()
	open(t, s1, "a", 5000)
	s1.Apply(withdraw) // fails: only 50.00 available
	s1.Apply(deposit)

	s2 := New()
	open(t, s2, "a", 5000)
	s2.Apply(deposit)
	s2.Apply(withdraw) // succeeds: 100.00 available

	b1, _ := s1.Balance("a")
	b2, _ := s2.Balance("a")
	if b1 == b2 {
		t.Fatalf("order had no effect (both %s) — the ledger is not order-sensitive", b1)
	}
	if b1 != 10000 {
		t.Fatalf("withdraw-then-deposit: got %s, want 100.00", b1)
	}
	if b2 != 2000 {
		t.Fatalf("deposit-then-withdraw: got %s, want 20.00", b2)
	}
}

// --- Conservation of money ------------------------------------------------

// No sequence of transfers may change the total. This is the "no lost or
// duplicated money" requirement in its purest form.
func TestTransfersConserveMoney(t *testing.T) {
	s := New()
	accounts := []AccountID{"a", "b", "c", "d"}
	for _, id := range accounts {
		open(t, s, id, 10000)
	}
	initial := s.TotalMoney()

	// A deterministic pseudo-random-looking set of transfers, some of which fail.
	for i := range 200 {
		from := accounts[i%len(accounts)]
		to := accounts[(i*7+3)%len(accounts)]
		if from == to {
			continue
		}
		s.Apply(Command{
			Op:             OpTransfer,
			IdempotencyKey: fmt.Sprintf("t%d", i),
			From:           from, To: to,
			Amount: Money(100 + (i*37)%5000),
		})
	}

	if got := s.TotalMoney(); got != initial {
		t.Fatalf("money was created or destroyed: %s -> %s", initial, got)
	}
	if err := s.VerifyDoubleEntry(); err != nil {
		t.Fatalf("double-entry audit failed: %v", err)
	}
}

func TestVerifyDoubleEntryDetectsTampering(t *testing.T) {
	s := New()
	open(t, s, "alice", 1000)
	open(t, s, "bob", 1000)
	s.Apply(Command{Op: OpTransfer, IdempotencyKey: "t", From: "alice", To: "bob", Amount: 300})

	if err := s.VerifyDoubleEntry(); err != nil {
		t.Fatalf("clean ledger failed audit: %v", err)
	}

	// Tamper with the cached balance without a corresponding transaction.
	s.mu.Lock()
	s.balances["alice"] += 5000
	s.mu.Unlock()

	if err := s.VerifyDoubleEntry(); err == nil {
		t.Fatal("audit did not detect a balance inconsistent with history")
	}
}

// --- Concurrent flow ------------------------------------------------------

// Concurrent withdrawals against one account: exactly the bank-app scenario from
// NOW.md. At most the available funds may be withdrawn, no matter the
// interleaving, and the balance must never go negative.
func TestConcurrentWithdrawalsCannotOverdraw(t *testing.T) {
	s := New()
	open(t, s, "shared", 10000) // $100.00

	const workers = 20
	const amount = Money(1000) // $10.00 each; only 10 can succeed

	var wg sync.WaitGroup
	results := make([]Result, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = s.Apply(Command{
				Op:             OpWithdraw,
				IdempotencyKey: fmt.Sprintf("w%d", i),
				From:           "shared",
				Amount:         amount,
			})
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for _, r := range results {
		if r.OK {
			succeeded++
		}
	}
	if succeeded != 10 {
		t.Fatalf("%d withdrawals succeeded, want exactly 10", succeeded)
	}
	bal, _ := s.Balance("shared")
	if bal != 0 {
		t.Fatalf("balance = %s, want 0.00", bal)
	}
	if bal < 0 {
		t.Fatal("balance went negative: overdraft under concurrency")
	}
	if err := s.VerifyDoubleEntry(); err != nil {
		t.Fatalf("double-entry audit failed after concurrent load: %v", err)
	}
}

func TestConcurrentRetriesOfSameKeyApplyOnce(t *testing.T) {
	s := New()
	open(t, s, "alice", 10000)

	cmd := Command{Op: OpWithdraw, IdempotencyKey: "one-key", From: "alice", Amount: 2500}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Apply(cmd)
		}()
	}
	wg.Wait()

	if got, _ := s.Balance("alice"); got != 7500 {
		t.Fatalf("alice = %s, want 75.00 — concurrent retries applied more than once", got)
	}
}

// --- Codec ----------------------------------------------------------------

func TestCommandRoundTrips(t *testing.T) {
	cases := []Command{
		{Op: OpDeposit, IdempotencyKey: "k1", To: "alice", Amount: 12345},
		{Op: OpWithdraw, IdempotencyKey: "k2", From: "bob", Amount: 1},
		{Op: OpTransfer, IdempotencyKey: "k3", From: "a", To: "b", Amount: 999999},
		{Op: OpOpenAccount, IdempotencyKey: "k4", To: "new", Amount: 0},
		{Op: OpDeposit, IdempotencyKey: "", To: "", Amount: 0},
	}
	for _, want := range cases {
		got, err := Decode(want.Encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip: got %+v, want %+v", got, want)
		}
	}
}

func TestEncodingIsStable(t *testing.T) {
	c := Command{Op: OpTransfer, IdempotencyKey: "key", From: "a", To: "b", Amount: 500}
	first := c.Encode()
	for range 50 {
		if got := c.Encode(); string(got) != string(first) {
			t.Fatal("encoding is not stable across calls — determinism hazard")
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, bad := range [][]byte{{}, {1}, {1, 5, 0}, {1, 200, 200, 1, 2, 3}} {
		if _, err := Decode(bad); err == nil {
			t.Fatalf("decoded garbage %v without error", bad)
		}
	}
}

func TestMachineAppliesThroughCodec(t *testing.T) {
	st := New()
	m := NewMachine(st)

	m.Apply(Command{Op: OpOpenAccount, IdempotencyKey: "o", To: "alice", Amount: 5000}.Encode())
	m.Apply(Command{Op: OpWithdraw, IdempotencyKey: "w", From: "alice", Amount: 2000}.Encode())

	if got, _ := st.Balance("alice"); got != 3000 {
		t.Fatalf("alice = %s, want 30.00", got)
	}
	if r, ok := m.Result("w"); !ok || !r.OK {
		t.Fatalf("result for key w = %+v, ok=%v", r, ok)
	}
}
