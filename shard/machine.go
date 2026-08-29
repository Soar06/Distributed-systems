package shard

import (
	"sync"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// Machine is the state machine each shard's Raft group replicates.
//
// It applies both ordinary single-shard ledger commands and 2PC protocol steps.
// Every 2PC state transition arrives here as a committed Raft log entry — which is
// precisely what makes the protocol survive crashes. Nothing in here is
// in-memory-only state that a restart would lose: it is all derived by applying
// the log in order, exactly like balances.
type Machine struct {
	ID    ID
	State *ledger.State

	mu      sync.Mutex
	txs     map[TxID]*TxRecord
	results map[string]ledger.Result

	// byIndex records the result of the command applied at each Raft log index,
	// so a proposer can learn what its specific entry actually did. Replication
	// succeeding and the operation succeeding are different questions — a prepare
	// that votes NO replicates perfectly well.
	byIndex map[uint64]ledger.Result
}

// NewMachine builds a shard state machine.
func NewMachine(id ID, st *ledger.State) *Machine {
	return &Machine{
		ID:      id,
		State:   st,
		txs:     make(map[TxID]*TxRecord),
		results: make(map[string]ledger.Result),
		byIndex: make(map[uint64]ledger.Result),
	}
}

// Apply implements raft.StateMachine.
//
// Determinism requirement is unchanged from Phase 1: same log, same order, same
// resulting state on every replica of this shard.
func (m *Machine) Apply(data []byte) any {
	return m.apply(data)
}

// ApplyAt implements raft.IndexedStateMachine, recording the result against the
// log index so the proposer of that entry can read its actual outcome.
func (m *Machine) ApplyAt(index raft.Index, data []byte) any {
	res := m.apply(data)
	if r, ok := res.(ledger.Result); ok {
		m.mu.Lock()
		m.byIndex[uint64(index)] = r
		m.mu.Unlock()
	}
	return res
}

// AppliedResult returns the result recorded for the entry at the given log index.
func (m *Machine) AppliedResult(index raft.Index) ledger.Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.byIndex[uint64(index)]; ok {
		return r
	}
	return ledger.Result{OK: true}
}

func (m *Machine) apply(data []byte) any {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return ledger.Result{Err: err.Error()}
	}

	switch cmd.Op {
	case OpSingle:
		res := m.State.Apply(cmd.Ledger)
		m.recordResult(cmd.Ledger.IdempotencyKey, res)
		return res

	case OpPrepare:
		return m.applyPrepare(cmd)

	case OpDecision:
		return m.applyDecision(cmd)

	case OpOutcome:
		return m.applyOutcome(cmd)

	default:
		return ledger.Result{Err: "shard: unknown op"}
	}
}

// applyPrepare records this shard's YES vote and reserves the funds.
//
// Spanner: participants "log a prepare record through Paxos". Committing this
// entry IS the vote becoming durable — after this, the participant has made an
// unretractable promise and may not abort unilaterally.
func (m *Machine) applyPrepare(cmd Command) any {
	var res ledger.Result
	if cmd.Debit {
		res = m.State.PrepareDebit(string(cmd.TxID), cmd.Ledger.From, cmd.Ledger.Amount)
	} else {
		res = m.State.PrepareCredit(string(cmd.TxID), cmd.Ledger.To, cmd.Ledger.Amount)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if res.OK {
		m.txs[cmd.TxID] = &TxRecord{
			ID:           cmd.TxID,
			Phase:        PhasePrepared,
			Cmd:          cmd.Ledger,
			Debit:        cmd.Debit,
			Participants: cmd.Participants,
		}
	} else {
		// A NO vote is also recorded: the coordinator must be able to learn this
		// shard refused, and a restart must not turn a no into a maybe.
		m.txs[cmd.TxID] = &TxRecord{
			ID:      cmd.TxID,
			Phase:   PhaseAborted,
			Cmd:     cmd.Ledger,
			Debit:   cmd.Debit,
			Decided: true,
			Commit:  false,
		}
	}
	return res
}

// applyDecision records the COORDINATOR's commit-or-abort decision.
//
// Spanner: "The coordinator leader then logs a commit record through Paxos (or an
// abort if it timed out while waiting on the other participants)."
//
// Once this entry is committed in the coordinator's Raft group, the transaction's
// outcome is settled permanently. A coordinator crash after this point is
// survivable: the replacement leader reads the decision from its own log. This is
// the single line that turns 2PC from fragile into recoverable.
func (m *Machine) applyDecision(cmd Command) any {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.txs[cmd.TxID]
	if !ok {
		rec = &TxRecord{ID: cmd.TxID, Cmd: cmd.Ledger, Debit: cmd.Debit}
		m.txs[cmd.TxID] = rec
	}
	rec.Decided = true
	rec.Commit = cmd.Commit
	rec.Participants = cmd.Participants
	return ledger.Result{OK: true}
}

// applyOutcome applies the decision to this shard's balances.
//
// Spanner: "Each participant leader logs the transaction's outcome through Paxos."
// Idempotent, because the decision may legitimately be delivered more than once.
func (m *Machine) applyOutcome(cmd Command) any {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.txs[cmd.TxID]
	if !ok {
		rec = &TxRecord{ID: cmd.TxID, Cmd: cmd.Ledger, Debit: cmd.Debit}
		m.txs[cmd.TxID] = rec
	}

	// Terminal phases are final. The decision may legitimately be delivered more
	// than once — by a retry, a duplicate RPC, or recovery re-delivering it — and
	// applying it twice would debit twice.
	//
	// The whole check-and-apply runs under one lock so two concurrent deliveries
	// cannot both pass the guard.
	if rec.Phase == PhaseCommitted || rec.Phase == PhaseAborted {
		return ledger.Result{OK: true}
	}

	var res ledger.Result
	if cmd.Commit {
		if rec.Debit {
			res = m.State.CommitDebit(string(cmd.TxID))
		} else {
			res = m.State.CommitCredit(string(cmd.TxID), rec.Cmd.To, rec.Cmd.Amount)
		}
		rec.Phase = PhaseCommitted
	} else {
		res = m.State.AbortTx(string(cmd.TxID))
		rec.Phase = PhaseAborted
	}
	rec.Decided = true
	rec.Commit = cmd.Commit
	return res
}

// Tx returns the durable record for a transaction.
func (m *Machine) Tx(id TxID) (TxRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.txs[id]
	if !ok {
		return TxRecord{}, false
	}
	return *r, true
}

// InDoubt returns transactions this shard has prepared but not yet APPLIED.
//
// These are the blocked transactions: this shard voted yes, reserved the funds,
// and the outcome has not yet been applied to its balances. §12 — the blocking is
// inherent to 2PC, not a bug, and exposing it is what makes it observable.
//
// Note the condition is Phase == PhasePrepared, NOT "no decision known". A
// transaction whose decision has been logged but not yet applied is still
// unresolved *here*, and recovery must be able to find it — excluding it was a
// bug that left durable COMMIT decisions unapplied forever.
func (m *Machine) InDoubt() []TxID {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []TxID
	for id, r := range m.txs {
		if r.Phase == PhasePrepared {
			out = append(out, id)
		}
	}
	return out
}

// Decision returns a transaction's outcome if this shard knows it. An in-doubt
// participant asks the coordinator group this question during recovery.
func (m *Machine) Decision(id TxID) (commit bool, known bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.txs[id]
	if !ok || !r.Decided {
		return false, false
	}
	return r.Commit, true
}

func (m *Machine) recordResult(key string, res ledger.Result) {
	if key == "" {
		return
	}
	m.mu.Lock()
	m.results[key] = res
	m.mu.Unlock()
}

// Result returns a recorded single-shard result.
func (m *Machine) Result(key string) (ledger.Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.results[key]
	return r, ok
}
