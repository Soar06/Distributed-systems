package shard

import (
	"errors"
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
)

// The 2PC coordinator.
//
// Per Spanner (§12): "One of the participant groups is chosen as the coordinator:
// the participant leader of that group will be referred to as the coordinator
// leader." There is NO separate coordinator service — the debit shard takes the
// role. That matters: a standalone coordinator would be a new single point of
// failure, whereas a participant group is already Raft-replicated.

var (
	// ErrTxAborted means the transaction did not commit. Deterministic and final.
	ErrTxAborted = errors.New("shard: transaction aborted")

	// ErrInDoubt means the coordinator could not reach a decision within the
	// timeout. The transaction is NOT resolved — it is blocked. This is 2PC's
	// inherent blocking behavior, surfaced rather than papered over.
	ErrInDoubt = errors.New("shard: transaction in doubt; awaiting coordinator recovery")
)

// Group is one shard's Raft group, as the coordinator needs to see it.
type Group interface {
	// Propose replicates a command through this group's Raft log and waits for it
	// to be applied. Returns false if this node is not the group's leader.
	Propose(cmd Command, timeout time.Duration) (ledger.Result, bool, error)

	// Machine returns the shard's state machine, for reading decisions.
	Machine() *Machine

	// IsLeader reports whether this node leads the group.
	IsLeader() bool
}

// Coordinator drives a cross-shard transfer.
type Coordinator struct {
	ring   *Ring
	groups map[ID]Group

	// prepareTimeout bounds phase 1. A participant that does not vote in time is
	// treated as a NO — the coordinator aborts rather than blocking forever.
	prepareTimeout time.Duration

	// commitTimeout bounds phase 2 delivery.
	commitTimeout time.Duration
}

// NewCoordinator builds a coordinator over the given groups.
func NewCoordinator(ring *Ring, groups map[ID]Group) *Coordinator {
	return &Coordinator{
		ring:           ring,
		groups:         groups,
		prepareTimeout: 3 * time.Second,
		commitTimeout:  3 * time.Second,
	}
}

// ShardFor returns the shard owning an account.
func (c *Coordinator) ShardFor(account ledger.AccountID) ID {
	return c.ring.Lookup(string(account))
}

// Transfer moves money between accounts, choosing single-shard or 2PC as needed.
func (c *Coordinator) Transfer(txID TxID, cmd ledger.Command) (ledger.Result, error) {
	fromShard := c.ShardFor(cmd.From)
	toShard := c.ShardFor(cmd.To)

	// Same shard: an ordinary single-group Raft commit. No 2PC needed, and the
	// atomicity is exactly Phase 1's. Most transfers should land here, which is
	// why sharding by account is worth doing.
	if fromShard == toShard {
		return c.single(fromShard, cmd)
	}
	return c.twoPhase(txID, cmd, fromShard, toShard)
}

// single applies an intra-shard operation.
func (c *Coordinator) single(s ID, cmd ledger.Command) (ledger.Result, error) {
	g, ok := c.groups[s]
	if !ok {
		return ledger.Result{}, fmt.Errorf("shard: no group %s", s)
	}
	res, isLeader, err := g.Propose(Command{Op: OpSingle, Ledger: cmd}, c.commitTimeout)
	if err != nil {
		return ledger.Result{}, err
	}
	if !isLeader {
		return ledger.Result{}, fmt.Errorf("shard: %s is not led by this node", s)
	}
	return res, nil
}

// twoPhase runs the full protocol across two shards.
//
// The debit shard is the coordinator, following Spanner's "one of the participant
// groups is chosen as the coordinator."
func (c *Coordinator) twoPhase(txID TxID, cmd ledger.Command, fromShard, toShard ID) (ledger.Result, error) {
	debitGroup, ok := c.groups[fromShard]
	if !ok {
		return ledger.Result{}, fmt.Errorf("shard: no group %s", fromShard)
	}
	creditGroup, ok := c.groups[toShard]
	if !ok {
		return ledger.Result{}, fmt.Errorf("shard: no group %s", toShard)
	}

	participants := []ID{fromShard, toShard}

	// ---- Phase 1: PREPARE ----
	//
	// Each participant logs a prepare record through its own Raft group. A YES
	// vote is durable before it is reported, so a crashed participant wakes up
	// still bound by its promise.

	// The debit shard is the coordinator (Spanner: "one of the participant groups
	// is chosen as the coordinator"), named explicitly on every record so recovery
	// never has to infer it from slice position.
	coordinator := fromShard

	debitVote, isLeader, err := debitGroup.Propose(Command{
		Op: OpPrepare, TxID: txID, Ledger: cmd,
		Debit: true, Participants: participants, Coordinator: coordinator,
	}, c.prepareTimeout)
	if err != nil || !isLeader {
		// Could not even record our own vote: nothing was promised, so aborting is
		// safe and no participant is left holding anything.
		return ledger.Result{}, fmt.Errorf("shard: prepare failed on debit shard %s: %v (leader=%v)",
			fromShard, err, isLeader)
	}
	if !debitVote.OK {
		// The debit side voted NO — insufficient funds, most commonly. Abort, and
		// tell the credit side so it does not wait.
		c.decide(txID, cmd, participants, coordinator, false, debitGroup, creditGroup)
		return debitVote, ErrTxAborted
	}

	creditVote, isLeader, err := creditGroup.Propose(Command{
		Op: OpPrepare, TxID: txID, Ledger: cmd,
		Debit: false, Participants: participants, Coordinator: coordinator,
	}, c.prepareTimeout)
	if err != nil || !isLeader || !creditVote.OK {
		// The credit side did not vote yes. The debit side HAS reserved funds, so
		// they must be released — this is exactly what the abort path exists for.
		c.decide(txID, cmd, participants, coordinator, false, debitGroup, creditGroup)
		if !creditVote.OK && creditVote.Err != "" {
			return creditVote, ErrTxAborted
		}
		return ledger.Result{}, ErrTxAborted
	}

	// ---- Phase 2: DECIDE and APPLY ----
	//
	// Both voted yes, so the transaction commits. The decision is logged through
	// the coordinator's Raft group FIRST: once that entry commits, the outcome is
	// permanent even if the coordinator dies immediately afterwards.

	if err := c.decide(txID, cmd, participants, coordinator, true, debitGroup, creditGroup); err != nil {
		// The decision could not be recorded or delivered. Participants that voted
		// yes are now in doubt — correctly refusing to guess. Recovery resolves
		// them; see RecoverInDoubt.
		return ledger.Result{}, ErrInDoubt
	}

	avail, _ := debitGroup.Machine().State.Available(cmd.From)
	return ledger.Result{OK: true, Balance: avail}, nil
}

// decide logs the coordinator's decision and delivers it to participants.
func (c *Coordinator) decide(txID TxID, cmd ledger.Command, participants []ID,
	coordinator ID, commit bool, debitGroup, creditGroup Group) error {

	// Step 1: the coordinator records the decision in its OWN Raft log. This is
	// the durability point — after this the outcome cannot change, and a
	// replacement coordinator leader can recover it.
	if _, isLeader, err := debitGroup.Propose(Command{
		Op: OpDecision, TxID: txID, Ledger: cmd,
		Commit: commit, Debit: true, Participants: participants, Coordinator: coordinator,
	}, c.commitTimeout); err != nil || !isLeader {
		return fmt.Errorf("shard: could not log decision for %s: %v", txID, err)
	}

	// Step 2: each participant applies the outcome through its own Raft group.
	// Failures here are recoverable — the decision is already durable, so
	// re-delivery resolves any participant that missed it.
	var firstErr error
	if _, _, err := debitGroup.Propose(Command{
		Op: OpOutcome, TxID: txID, Ledger: cmd,
		Commit: commit, Debit: true, Participants: participants, Coordinator: coordinator,
	}, c.commitTimeout); err != nil {
		firstErr = err
	}
	if _, _, err := creditGroup.Propose(Command{
		Op: OpOutcome, TxID: txID, Ledger: cmd,
		Commit: commit, Debit: false, Participants: participants, Coordinator: coordinator,
	}, c.commitTimeout); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// RecoverInDoubt resolves transactions stuck in the prepared state.
//
// This is the answer to 2PC's blocking problem — not a way to avoid it, but the
// mechanism that ends the block. A participant that voted yes and never heard back
// asks the coordinator group for the decision. Because the decision was logged
// through Raft, a replacement coordinator leader still has it.
//
// If the coordinator genuinely never logged a decision, the transaction is aborted:
// safe, because no participant can have committed without a decision existing.
func (c *Coordinator) RecoverInDoubt() (resolved int, err error) {
	var blocked int
	var firstErr error

	for shardID, g := range c.groups {
		if !g.IsLeader() {
			continue // only a leader may propose on behalf of its group
		}

		for _, txID := range g.Machine().InDoubt() {
			rec, ok := g.Machine().Tx(txID)
			if !ok {
				continue
			}

			// Ask the coordinator group for the outcome.
			//
			// THREE outcomes must be distinguished, and conflating them destroys
			// money. "I cannot reach the coordinator" is not "no decision exists":
			// the coordinator's Raft group may hold a durable COMMIT that this node
			// simply cannot see. Aborting on that guess unilaterally reverses a
			// committed transaction — the debit stands and the credit never lands.
			coordID := rec.Coordinator
			if coordID == "" && len(rec.Participants) > 0 {
				// Legacy records written before Coordinator was explicit.
				coordID = rec.Participants[0]
			}
			if coordID == "" {
				// We do not know who to ask. Staying blocked is correct: 2PC is a
				// blocking protocol and this is exactly the case it blocks for.
				blocked++
				continue
			}

			cg, reachable := c.groups[coordID]
			if !reachable || !cg.IsLeader() {
				// Coordinator group unreachable or has no leader we can query. Stay
				// blocked and retry later. This is the inherent blocking of 2PC
				// (§12), not a failure to handle.
				blocked++
				continue
			}

			commit, known := cg.Machine().Decision(txID)
			if !known {
				// The coordinator group IS reachable and has NO decision recorded.
				// Only now is abort safe: with no decision in the coordinator's log,
				// no participant can have been told to commit.
				commit = false
			}

			if _, _, err := g.Propose(Command{
				Op: OpOutcome, TxID: txID, Ledger: rec.Cmd,
				Commit: commit, Debit: rec.Debit,
				Participants: rec.Participants, Coordinator: coordID,
			}, c.commitTimeout); err != nil {
				// Record and continue rather than abandoning the remaining shards:
				// one unrecoverable transaction must not block every other one.
				firstErr = fmt.Errorf("shard: recovering %s on %s: %w", txID, shardID, err)
				blocked++
				continue
			}
			resolved++
		}
	}

	if firstErr != nil {
		return resolved, firstErr
	}
	if blocked > 0 {
		// Report honestly that transactions remain unresolved. A caller that treats
		// a nil error as "everything is settled" would be wrong.
		return resolved, fmt.Errorf("%w: %d transaction(s) still blocked awaiting a reachable coordinator",
			ErrInDoubt, blocked)
	}
	return resolved, nil
}

// InDoubtCount reports how many transactions are currently blocked across all
// shards. Used by tests and by the dashboard to make the blocking window visible.
func (c *Coordinator) InDoubtCount() int {
	n := 0
	for _, g := range c.groups {
		n += len(g.Machine().InDoubt())
	}
	return n
}
