package ledger

import "fmt"

// Fund reservation, for cross-shard transfers (Phase 2).
//
// A cross-shard transfer cannot debit and credit atomically — the two accounts
// live in different Raft groups. Between prepare and commit, the money must be in
// a state where it can be neither spent twice nor lost:
//
//   - RESERVED at prepare: removed from the available balance, but still counted
//     in the account's total. A concurrent withdrawal cannot spend it.
//   - On COMMIT: the reservation becomes a real debit.
//   - On ABORT: the reservation is released back to available.
//
// The invariant that makes this safe: total money = sum of balances, and a
// reservation never changes that sum. Money is never in two places, and never in
// neither.

// Reserve holds funds for an in-flight distributed transaction.
type Reserve struct {
	TxID    string
	Account AccountID
	Amount  Money
}

// Available returns the balance that may be spent right now: the total balance
// minus anything reserved for in-flight transactions.
//
// This is the number a withdrawal is checked against. Checking against the raw
// balance instead would let the same money be spent twice — once by a local
// withdrawal and once by a cross-shard transfer that has already promised it.
func (s *State) Available(id AccountID) (Money, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.availableLocked(id)
}

func (s *State) availableLocked(id AccountID) (Money, bool) {
	bal, ok := s.balances[id]
	if !ok {
		return 0, false
	}
	return bal - s.reservedLocked(id), true
}

// reservedLocked sums outstanding reservations against an account.
// Caller must hold s.mu.
func (s *State) reservedLocked(id AccountID) Money {
	var total Money
	for _, r := range s.reserves {
		if r.Account == id {
			total += r.Amount
		}
	}
	return total
}

// Reserved returns the total currently reserved against an account.
func (s *State) Reserved(id AccountID) Money {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedLocked(id)
}

// PrepareDebit reserves funds for a distributed transaction — the participant's
// YES vote in 2PC.
//
// Returns false if the funds are not available. Voting yes is an unretractable
// promise, so this must only succeed when the money is genuinely there and can be
// held until the transaction resolves.
func (s *State) PrepareDebit(txID string, account AccountID, amount Money) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	if amount <= 0 {
		return Result{Err: ErrInvalidAmount.Error()}
	}
	if _, exists := s.balances[account]; !exists {
		return Result{Err: ErrNoSuchAccount.Error()}
	}

	// Idempotent: re-preparing an already-prepared transaction is a no-op, since
	// the prepare RPC may legitimately be retried.
	if _, held := s.reserves[txID]; held {
		avail, _ := s.availableLocked(account)
		return Result{OK: true, Balance: avail}
	}

	avail, _ := s.availableLocked(account)
	if avail < amount {
		return Result{Err: ErrInsufficientFunds.Error(), Balance: avail}
	}

	s.reserves[txID] = Reserve{TxID: txID, Account: account, Amount: amount}
	newAvail, _ := s.availableLocked(account)
	return Result{OK: true, Balance: newAvail}
}

// PrepareCredit is the credit side's vote. A credit cannot fail for lack of
// funds, so this only validates that the account exists — but it is still logged
// through Raft, because the participant must durably know it is part of this
// transaction before it can be told to commit.
func (s *State) PrepareCredit(txID string, account AccountID, amount Money) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	if amount <= 0 {
		return Result{Err: ErrInvalidAmount.Error()}
	}
	bal, exists := s.balances[account]
	if !exists {
		return Result{Err: ErrNoSuchAccount.Error()}
	}
	return Result{OK: true, Balance: bal}
}

// CommitDebit turns a reservation into a real debit.
func (s *State) CommitDebit(txID string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, held := s.reserves[txID]
	if !held {
		// Already committed (a duplicate delivery), or never prepared here.
		// Idempotent by design: the decision may be delivered more than once.
		return Result{OK: true}
	}

	s.balances[r.Account] -= r.Amount
	delete(s.reserves, txID)
	s.record(Command{
		Op:             OpTransfer,
		IdempotencyKey: txID + ":debit",
		From:           r.Account,
		Amount:         r.Amount,
	}, []Entry{{Account: r.Account, Amount: -r.Amount}})

	return Result{OK: true, Balance: s.balances[r.Account]}
}

// CommitCredit applies the credit side.
func (s *State) CommitCredit(txID string, account AccountID, amount Money) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := txID + ":credit"
	if prev, done := s.applied[key]; done {
		return prev // idempotent: the decision may arrive twice
	}

	if _, exists := s.balances[account]; !exists {
		return Result{Err: ErrNoSuchAccount.Error()}
	}
	s.balances[account] += amount
	s.record(Command{
		Op:             OpTransfer,
		IdempotencyKey: key,
		To:             account,
		Amount:         amount,
	}, []Entry{{Account: account, Amount: amount}})

	res := Result{OK: true, Balance: s.balances[account]}
	s.applied[key] = res
	return res
}

// AbortTx releases any reservation held for a transaction.
func (s *State) AbortTx(txID string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, held := s.reserves[txID]
	if !held {
		return Result{OK: true} // nothing reserved; idempotent
	}
	delete(s.reserves, txID)

	avail, _ := s.availableLocked(r.Account)
	return Result{OK: true, Balance: avail}
}

// InFlight returns the transaction ids with outstanding reservations. Used to
// find in-doubt transactions after a crash, and by the dashboard.
func (s *State) InFlight() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.reserves))
	for id := range s.reserves {
		out = append(out, id)
	}
	return out
}

// VerifyConservation checks that no money was created or destroyed.
//
// Reservations deliberately do NOT change the total: reserved money is still in
// the account, merely unavailable. If a reservation ever altered the total, a
// crash mid-transfer would leak or duplicate funds — the exact failure 2PC exists
// to prevent.
func (s *State) VerifyConservation(expected Money) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total Money
	for _, b := range s.balances {
		total += b
	}
	if total != expected {
		return fmt.Errorf("money conservation violated: total is %s, expected %s", total, expected)
	}
	return nil
}
