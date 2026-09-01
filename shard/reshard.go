package shard

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
)

// Driving a live reshard through its phases (§23).
//
// migration.go holds the state machine and the ownership rule; this file is the
// part that actually moves money between two Raft groups and decides when it is
// safe to flip.
//
// THE ORDER IS THE ALGORITHM
//
//	prepare  -> copy under live traffic, source still authoritative
//	freeze   -> refuse new writes for the moving arcs only
//	delta    -> ship whatever arrived during the copy
//	cutover  -> flip ownership; destination becomes authoritative
//	cleanup  -> source discards its copy, but ONLY after cutover committed
//
// Every step exists to preserve one invariant: at every instant, exactly one
// shard is authoritative for each key. Reordering any two of them breaks it.
//
// WHY CLEANUP IS LAST AND CONDITIONAL
//
// Discarding the source's copy while the cutover is still in doubt destroys the
// only surviving copy of that data. This project has already learned the general
// form of that lesson twice — an in-doubt 2PC decision must be resolved, not
// guessed — and it applies with more force here because the thing at stake is
// not one transaction but a whole key range.

// ReshardPlan describes a move to perform.
type ReshardPlan struct {
	ID   string
	From ID
	To   ID

	// VNodes are which of From's virtual nodes move to To. Moving all of them
	// empties the shard; moving a few rebalances it.
	VNodes []int

	// FreezeTimeout bounds the frozen window. If the final delta cannot be
	// shipped within it, the migration aborts rather than extending an
	// availability outage indefinitely.
	FreezeTimeout time.Duration
}

// ReshardStatus reports what a move did.
type ReshardStatus struct {
	ID       string             `json:"id"`
	From     ID                 `json:"from"`
	To       ID                 `json:"to"`
	Phase    string             `json:"phase"`
	VNodes   int                `json:"vnodes"`
	Moved    int                `json:"moved_accounts"`
	Frozen   time.Duration      `json:"frozen_for"`
	Err      string             `json:"err,omitempty"`
	Accounts []ledger.AccountID `json:"accounts,omitempty"`
}

// Reshard moves a set of virtual nodes from one shard to another, live.
//
// Returns when the move has completed or aborted. Writes to the moving arcs are
// refused for the frozen window only; every other key in both shards keeps
// committing throughout.
func (c *Coordinator) Reshard(plan ReshardPlan) (ReshardStatus, error) {
	if plan.From == plan.To {
		return ReshardStatus{}, fmt.Errorf("shard: cannot reshard %s onto itself", plan.From)
	}
	if len(plan.VNodes) == 0 {
		return ReshardStatus{}, fmt.Errorf("shard: no vnodes to move")
	}
	src, ok := c.groups[plan.From]
	if !ok {
		return ReshardStatus{}, fmt.Errorf("shard: no group %s", plan.From)
	}
	dst, ok := c.groups[plan.To]
	if !ok {
		return ReshardStatus{}, fmt.Errorf("shard: no group %s", plan.To)
	}
	if plan.FreezeTimeout <= 0 {
		plan.FreezeTimeout = 10 * time.Second
	}

	m := &Migration{ID: plan.ID, From: plan.From, To: plan.To, VNodes: plan.VNodes}
	if err := c.migrations.begin(m); err != nil {
		return ReshardStatus{}, err
	}
	defer c.migrations.finish(plan.ID)

	status := ReshardStatus{
		ID: plan.ID, From: plan.From, To: plan.To, VNodes: len(plan.VNodes),
	}

	// ---- 1. PREPARE: which accounts are actually moving --------------------
	//
	// Computed from the ring, not from a key range: the moving accounts are
	// exactly those whose hash lands on one of the reassigned virtual nodes.
	moving := c.accountsOnVNodes(src, plan.From, plan.VNodes)
	status.Accounts = moving

	if len(moving) == 0 {
		// Nothing to copy. Still a real migration — ownership must still flip, or
		// accounts opened later on these arcs would go to the wrong shard.
		if err := c.migrations.advance(plan.ID, MigFrozen); err != nil {
			return status, err
		}
		if err := c.migrations.advance(plan.ID, MigCutover); err != nil {
			return status, err
		}
		c.ring.Reassign(plan.From, plan.To, plan.VNodes)
		if err := c.migrations.advance(plan.ID, MigDone); err != nil {
			return status, err
		}
		status.Phase = MigDone.String()
		return status, nil
	}

	// ---- 2. FREEZE ---------------------------------------------------------
	//
	// The copy below happens INSIDE the freeze in this implementation, which is a
	// deliberate simplification and the honest place to say so: a production
	// system copies the bulk under live traffic and freezes only for the final
	// delta. The demo's datasets are small enough that the bulk copy IS the
	// delta, and doing it in two passes here would add machinery that teaches
	// nothing the single pass does not.
	//
	// What is NOT simplified is the ordering and the invariant: writes are
	// refused before any balance is read, so no write can slip in between the
	// read and the cutover.
	frozenAt := time.Now()
	if err := c.migrations.advance(plan.ID, MigFrozen); err != nil {
		return status, err
	}
	status.Phase = MigFrozen.String()

	// ---- 3. DELTA: copy the balances ---------------------------------------
	moved, err := c.copyAccounts(src, dst, moving, plan.FreezeTimeout)
	status.Moved = moved
	if err != nil {
		// Abort rather than flipping ownership onto an incomplete destination.
		// Ownership never left the source, so refusing here loses nothing.
		m.err = err
		_ = c.migrations.advance(plan.ID, MigAborted)
		status.Phase = MigAborted.String()
		status.Frozen = time.Since(frozenAt)
		status.Err = err.Error()
		return status, fmt.Errorf("shard: reshard %s aborted during copy: %w", plan.ID, err)
	}

	// ---- 4. CUTOVER --------------------------------------------------------
	//
	// Ownership flips here, and from this instant ShardFor sends these keys to the
	// destination. The frozen window ends with it: the destination is authoritative
	// and accepting writes.
	if err := c.migrations.advance(plan.ID, MigCutover); err != nil {
		return status, err
	}

	// Make the flip PERMANENT by moving the arcs on the ring itself.
	//
	// The migration table describes a move in flight and is torn down when this
	// function returns. Recording the new ownership only there meant finishing the
	// migration handed the keys straight back to the source — the cutover undid
	// itself the instant it completed. The ring is the durable answer; the table
	// only covers the window while the answer is changing.
	c.ring.Reassign(plan.From, plan.To, plan.VNodes)

	status.Frozen = time.Since(frozenAt)

	// ---- 5. CLEANUP --------------------------------------------------------
	//
	// Only now is it safe to remove the source's copy: the cutover is committed,
	// so the destination is unambiguously authoritative and its copy is the one
	// that counts.
	if err := c.dropAccounts(src, moving); err != nil {
		// The move itself succeeded — ownership flipped and the data is on the
		// destination. A failed cleanup leaves a harmless orphan copy that nothing
		// routes to, so it is reported rather than treated as a failed migration.
		status.Err = fmt.Sprintf("moved successfully but source cleanup failed: %v", err)
	}

	if err := c.migrations.advance(plan.ID, MigDone); err != nil {
		return status, err
	}
	status.Phase = MigDone.String()
	return status, nil
}

// accountsOnVNodes lists the source shard's accounts that hash onto the moving
// virtual nodes.
func (c *Coordinator) accountsOnVNodes(src Group, from ID, vnodes []int) []ledger.AccountID {
	want := make(map[int]bool, len(vnodes))
	for _, v := range vnodes {
		want[v] = true
	}

	var out []ledger.AccountID
	for acct := range src.Machine().State.Balances() {
		sid, vnode := c.ring.LookupVNode(string(acct))
		if sid == from && want[vnode] {
			out = append(out, acct)
		}
	}
	return out
}

// copyAccounts replicates balances onto the destination shard.
//
// Each account is opened on the destination through its OWN Raft log, so the
// destination's copy is a committed, replicated fact rather than a value written
// into memory behind consensus's back. That is the difference between the
// destination genuinely owning the data and merely appearing to.
func (c *Coordinator) copyAccounts(src, dst Group, accts []ledger.AccountID, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	moved := 0

	for _, acct := range accts {
		if time.Now().After(deadline) {
			return moved, fmt.Errorf(
				"freeze window of %v expired after %d/%d accounts; aborting rather than "+
					"extending the write outage", timeout, moved, len(accts))
		}

		bal, ok := src.Machine().State.Balance(acct)
		if !ok {
			continue
		}

		_, isLeader, err := dst.Propose(Command{
			Op: OpSingle,
			Ledger: ledger.Command{
				Op:             ledger.OpOpenAccount,
				IdempotencyKey: fmt.Sprintf("reshard-open-%s", acct),
				To:             acct,
				Amount:         bal,
				Timestamp:      c.clock.Now(),
			},
		}, c.commitTimeout)
		if err != nil {
			return moved, fmt.Errorf("copying %s: %w", acct, err)
		}
		if !isLeader {
			return moved, fmt.Errorf("copying %s: destination is not leader", acct)
		}
		moved++
	}
	return moved, nil
}

// dropAccounts removes the moved accounts from the source shard.
//
// KNOWN COSMETIC REMNANT: the account stays visible on the source with a zero
// balance, because the ledger is double-entry and has no delete — an account is
// drained, not erased. The cluster total stays exactly correct (a zero adds
// nothing) and nothing routes to the stale entry, since ownership now resolves to
// the destination. It is recorded here rather than hidden because a reader
// looking at the source shard WILL see the name and should know why.
//
// Erasing it properly would mean a ledger-level close-account operation, which is
// a real feature with its own rules (what happens to history? can the id be
// reused?) rather than a tidy-up, so it is deliberately not invented here.
//
// Withdraw-to-zero rather than a delete primitive, because the ledger has no
// delete: it is double-entry, and money is conserved by construction. Zeroing
// the source's copy after the destination holds it keeps the CLUSTER-WIDE total
// correct — which is the invariant every chaos test in this project asserts.
func (c *Coordinator) dropAccounts(src Group, accts []ledger.AccountID) error {
	for _, acct := range accts {
		bal, ok := src.Machine().State.Balance(acct)
		if !ok || bal == 0 {
			continue
		}

		_, isLeader, err := src.Propose(Command{
			Op: OpSingle,
			Ledger: ledger.Command{
				Op:             ledger.OpWithdraw,
				IdempotencyKey: fmt.Sprintf("reshard-drain-%s", acct),
				From:           acct,
				Amount:         bal,
				Timestamp:      c.clock.Now(),
			},
		}, c.commitTimeout)
		if err != nil {
			return fmt.Errorf("draining %s: %w", acct, err)
		}
		if !isLeader {
			return fmt.Errorf("draining %s: source is not leader", acct)
		}
	}
	return nil
}

// Migrations reports the in-flight moves, for the UI and for tests.
func (c *Coordinator) Migrations() []ReshardStatus {
	var out []ReshardStatus
	for _, m := range c.migrations.list() {
		st := ReshardStatus{
			ID: m.ID, From: m.From, To: m.To,
			Phase: m.phase.String(), VNodes: len(m.VNodes), Moved: m.moved,
		}
		if m.err != nil {
			st.Err = m.err.Error()
		}
		out = append(out, st)
	}
	return out
}
