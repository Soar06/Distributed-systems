// Package ledger is the bank domain: accounts, double-entry transactions, and
// the deterministic state machine Raft replicates.
//
// The hard constraint here is DETERMINISM. Raft guarantees every node applies the
// same commands in the same order; that only produces the same state if applying
// a command is a pure function of (current state, command). So:
//
//   - No wall-clock time. No randomness. No map-iteration order.
//   - Validation happens at APPLY time against replicated state, never at request
//     time against a possibly-stale read. A withdrawal that looks fine on the
//     leader may still fail on apply — and it must fail identically everywhere.
//   - Money is integer minor units (cents). Never float64.
//
// This package knows nothing about Raft: no terms, no indices. That separation is
// what lets the ledger be tested with no consensus and Raft be tested with no
// ledger (context/DESIGN.md §7).
package ledger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Money is an amount in minor units (cents). Integer arithmetic only — binary
// floating point cannot represent 0.10 exactly, and a bank that loses fractions
// of a cent per transaction is a bank with a hole in it.
type Money int64

// String renders money as major units for display.
func (m Money) String() string {
	neg := m < 0
	v := m
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%d.%02d", v/100, v%100)
	if neg {
		return "-" + s
	}
	return s
}

// AccountID identifies an account.
type AccountID string

// Op is the kind of operation a command performs.
type Op uint8

const (
	OpDeposit Op = iota + 1
	OpWithdraw
	OpTransfer
	OpOpenAccount
)

func (o Op) String() string {
	switch o {
	case OpDeposit:
		return "Deposit"
	case OpWithdraw:
		return "Withdraw"
	case OpTransfer:
		return "Transfer"
	case OpOpenAccount:
		return "OpenAccount"
	default:
		return "Unknown"
	}
}

// Command is one operation to apply to the ledger. This is what goes inside a
// Raft log entry.
type Command struct {
	Op Op

	// IdempotencyKey dedupes retries. Networks retry; a client that does not hear
	// a reply cannot know whether the operation happened, so it resends. Without
	// this key the resend is a second withdrawal.
	IdempotencyKey string

	From   AccountID // empty for Deposit and OpenAccount
	To     AccountID // empty for Withdraw
	Amount Money
}

// Result is the outcome of applying a command.
type Result struct {
	OK      bool
	Err     string // non-empty when OK is false; a string so it serializes
	Balance Money  // resulting balance of the primary account, when meaningful
}

// Domain errors. These are expected outcomes, not bugs: a withdrawal exceeding
// the balance is the ledger working correctly.
var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrNoSuchAccount     = errors.New("no such account")
	ErrAccountExists     = errors.New("account already exists")
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrSameAccount       = errors.New("cannot transfer to the same account")
	ErrMissingKey        = errors.New("idempotency key required")
)

// Entry is one half of a double-entry pair: a single debit or credit against one
// account.
type Entry struct {
	Account AccountID
	Amount  Money // positive = credit (increase), negative = debit (decrease)
}

// Transaction is a completed, immutable ledger event: a matched set of entries
// that must sum to zero, plus the command that produced it.
//
// Double-entry means money is never created or destroyed by a transfer, only
// moved. Enforcing "entries sum to zero" makes that an invariant the code checks
// rather than a convention the code hopes for.
type Transaction struct {
	Seq            uint64 // monotonic, assigned at apply time — deterministic
	Op             Op
	IdempotencyKey string
	Entries        []Entry
}

// balances checks the double-entry invariant for a transaction.
//
// Deposits and withdrawals are the deliberate exception: money genuinely enters
// or leaves the system at the boundary. A full bank would balance those against a
// cash/vault account; modelling that is Phase 3 work (see context/DESIGN.md §6).
func (t Transaction) balances() bool {
	if t.Op != OpTransfer {
		return true
	}
	var sum Money
	for _, e := range t.Entries {
		sum += e.Amount
	}
	return sum == 0
}

// State is the ledger state machine. It implements raft.StateMachine via Apply.
type State struct {
	mu sync.RWMutex

	// balances is a cache of a fold over the transaction history — NOT the source
	// of truth. Any node must be able to rebuild it by replaying the log.
	balances map[AccountID]Money

	// applied maps idempotency key -> the result that key produced, so a retry
	// returns the original answer instead of performing the operation twice.
	applied map[string]Result

	// history is the append-only record of what happened. This is the
	// event-sourced ledger of DESIGN.md §6 in its Phase 1 form.
	history []Transaction

	seq uint64
}

// New returns an empty ledger.
func New() *State {
	return &State{
		balances: make(map[AccountID]Money),
		applied:  make(map[string]Result),
	}
}

// Apply executes a command against the ledger and returns its Result.
//
// This is the deterministic function Raft replicates. Given the same starting
// state and the same command, it must produce the same state and the same result
// on every node, forever.
func (s *State) Apply(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cmd.IdempotencyKey == "" {
		return Result{Err: ErrMissingKey.Error()}
	}

	// Retry path: return the original result without re-executing. This must come
	// before every other check, so a retried command that would now fail (because
	// the balance has since changed) still returns its original success.
	if prev, seen := s.applied[cmd.IdempotencyKey]; seen {
		return prev
	}

	res := s.applyLocked(cmd)
	s.applied[cmd.IdempotencyKey] = res
	return res
}

// applyLocked performs the operation. Caller must hold s.mu.
func (s *State) applyLocked(cmd Command) Result {
	switch cmd.Op {
	case OpOpenAccount:
		if _, exists := s.balances[cmd.To]; exists {
			return Result{Err: ErrAccountExists.Error()}
		}
		if cmd.Amount < 0 {
			return Result{Err: ErrInvalidAmount.Error()}
		}
		s.balances[cmd.To] = cmd.Amount
		s.record(cmd, []Entry{{Account: cmd.To, Amount: cmd.Amount}})
		return Result{OK: true, Balance: cmd.Amount}

	case OpDeposit:
		if cmd.Amount <= 0 {
			return Result{Err: ErrInvalidAmount.Error()}
		}
		if _, exists := s.balances[cmd.To]; !exists {
			return Result{Err: ErrNoSuchAccount.Error()}
		}
		s.balances[cmd.To] += cmd.Amount
		s.record(cmd, []Entry{{Account: cmd.To, Amount: cmd.Amount}})
		return Result{OK: true, Balance: s.balances[cmd.To]}

	case OpWithdraw:
		if cmd.Amount <= 0 {
			return Result{Err: ErrInvalidAmount.Error()}
		}
		bal, exists := s.balances[cmd.From]
		if !exists {
			return Result{Err: ErrNoSuchAccount.Error()}
		}
		// The check that makes concurrent withdrawals safe. Because this runs at
		// apply time, in log order, on every node, two racing withdrawals are
		// serialized by the log: the first sees the original balance, the second
		// sees the reduced one. Only one can win. This is linearizability doing
		// its job, not a lock.
		if bal < cmd.Amount {
			return Result{Err: ErrInsufficientFunds.Error(), Balance: bal}
		}
		s.balances[cmd.From] -= cmd.Amount
		s.record(cmd, []Entry{{Account: cmd.From, Amount: -cmd.Amount}})
		return Result{OK: true, Balance: s.balances[cmd.From]}

	case OpTransfer:
		if cmd.Amount <= 0 {
			return Result{Err: ErrInvalidAmount.Error()}
		}
		if cmd.From == cmd.To {
			return Result{Err: ErrSameAccount.Error()}
		}
		from, ok := s.balances[cmd.From]
		if !ok {
			return Result{Err: ErrNoSuchAccount.Error()}
		}
		if _, ok := s.balances[cmd.To]; !ok {
			return Result{Err: ErrNoSuchAccount.Error()}
		}
		if from < cmd.Amount {
			return Result{Err: ErrInsufficientFunds.Error(), Balance: from}
		}
		s.balances[cmd.From] -= cmd.Amount
		s.balances[cmd.To] += cmd.Amount
		// The matched debit and credit — the double-entry pair.
		s.record(cmd, []Entry{
			{Account: cmd.From, Amount: -cmd.Amount},
			{Account: cmd.To, Amount: cmd.Amount},
		})
		return Result{OK: true, Balance: s.balances[cmd.From]}

	default:
		return Result{Err: fmt.Sprintf("unknown op %d", cmd.Op)}
	}
}

// record appends a transaction to history. Caller must hold s.mu.
func (s *State) record(cmd Command, entries []Entry) {
	s.seq++
	t := Transaction{
		Seq:            s.seq,
		Op:             cmd.Op,
		IdempotencyKey: cmd.IdempotencyKey,
		Entries:        entries,
	}
	// A transfer whose entries do not sum to zero is a bug that would silently
	// create or destroy money. Fail loudly rather than persist it.
	if !t.balances() {
		panic(fmt.Sprintf("ledger: double-entry violation in %v: entries do not sum to zero", t))
	}
	s.history = append(s.history, t)
}

// Balance returns one account's balance and whether it exists.
func (s *State) Balance(id AccountID) (Money, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.balances[id]
	return b, ok
}

// Balances returns a snapshot of all balances.
func (s *State) Balances() map[AccountID]Money {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[AccountID]Money, len(s.balances))
	for k, v := range s.balances {
		out[k] = v
	}
	return out
}

// TotalMoney returns the sum of all balances.
//
// This is the conservation check: absent deposits and withdrawals, no sequence of
// transfers may change it. Chaos tests assert on this, because "no money was lost
// or created" is the one property a bank cannot compromise on.
func (s *State) TotalMoney() Money {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Sum in sorted key order. Integer addition is associative so order does not
	// affect the result, but iterating a Go map in random order inside a state
	// machine is a habit worth never forming.
	ids := make([]AccountID, 0, len(s.balances))
	for id := range s.balances {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var total Money
	for _, id := range ids {
		total += s.balances[id]
	}
	return total
}

// History returns a copy of the transaction history.
func (s *State) History() []Transaction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Transaction, len(s.history))
	copy(out, s.history)
	return out
}

// VerifyDoubleEntry replays the whole history and checks that the balances
// derived from it match the cached balances, and that every transfer balances.
//
// This is the audit: it proves the cache really is a fold over the log, which is
// the property DESIGN.md §6 requires and the one that makes the ledger
// event-sourced rather than merely logged.
func (s *State) VerifyDoubleEntry() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	derived := make(map[AccountID]Money)
	for _, t := range s.history {
		if !t.balances() {
			return fmt.Errorf("transaction %d (%v) does not balance", t.Seq, t.Op)
		}
		for _, e := range t.Entries {
			derived[e.Account] += e.Amount
		}
	}

	if len(derived) != len(s.balances) {
		return fmt.Errorf("derived %d accounts from history, cache holds %d",
			len(derived), len(s.balances))
	}
	for id, want := range s.balances {
		if got := derived[id]; got != want {
			return fmt.Errorf("account %s: history derives %s but cache holds %s",
				id, got, want)
		}
	}
	return nil
}
