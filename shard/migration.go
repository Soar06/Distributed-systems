package shard

import (
	"fmt"
	"sync"
)

// Live resharding: moving a key range from one shard to another while both keep
// serving. Theory and sources: learn/READING_LIST.md §23.
//
// THE HAZARD THIS EXISTS TO PREVENT
//
// During a move, two shards can both believe they own the same key. If ownership
// flips before the destination has the data, reads miss committed writes. If it
// flips after the source stops accepting writes, those writes are lost. If both
// accept writes at once, the two copies diverge and the ledger has two truths.
//
// Raft does not help. It guarantees agreement WITHIN one group; a move is a
// handoff BETWEEN two independent groups, each with its own log and leader.
// Consensus inside each shard is exactly as strong as before and says nothing
// about which of them is authoritative for a key.
//
// THE INVARIANT
//
//	At every instant, for every key, exactly one shard is authoritative — and the
//	transition is atomic with respect to any single operation.
//
// Not "eventually one". Every operation resolves ownership exactly once, at
// ShardFor, and gets a single answer that cannot change underneath it.
//
// WHY THERE IS A FROZEN WINDOW
//
// An AP system could let both sides accept writes and reconcile afterwards,
// because it is willing to hold two truths for a while. This system is CP
// (§3, §22), so the choice is a brief refusal for the moving range or a
// correctness violation. A bank takes the refusal. Making the window short is
// engineering; removing it would mean abandoning the guarantee.
//
// Only the MOVING range freezes. The rest of the source shard's keyspace is
// untouched and keeps committing throughout.

// MigrationPhase is where a move has got to.
//
// The constants are Mig-prefixed rather than Phase-prefixed because shard.Phase
// already exists for 2PC (twopc.go). Two unrelated phase enums sharing names
// would compile only by accident of ordering and would read as if a migration
// and a transaction were the same kind of thing.
//
// The phases are ordered and a migration only ever moves forward. There is no
// "rollback to serving" from Frozen: once writes have been refused, going
// backwards would mean the freeze accomplished nothing while still having cost
// availability. An aborted migration completes as Aborted, which returns
// ownership to the source having moved nothing.
type MigrationPhase int

const (
	// MigPreparing: destination is being filled. Source serves everything,
	// including writes to the moving range. Nothing has changed for clients.
	MigPreparing MigrationPhase = iota

	// MigFrozen: the source refuses NEW writes for the moving range only, and
	// reports them as retryable rather than failed. Reads still work — the source
	// still holds the authoritative copy. This is the only window that costs
	// availability, and it covers one range, not one shard.
	MigFrozen

	// MigCutover: ownership has flipped. The destination is authoritative and
	// the source must no longer answer for these keys.
	MigCutover

	// MigDone: cutover is committed and the source has discarded its copy.
	MigDone

	// MigAborted: the move was abandoned before cutover. Ownership never left
	// the source and no data moved.
	MigAborted
)

func (p MigrationPhase) String() string {
	switch p {
	case MigPreparing:
		return "preparing"
	case MigFrozen:
		return "frozen"
	case MigCutover:
		return "cutover"
	case MigDone:
		return "done"
	case MigAborted:
		return "aborted"
	}
	return "unknown"
}

// Migration is one in-flight move of a set of ring arcs from From to To.
//
// The moving unit is VIRTUAL NODES, not a key range. With consistent hashing a
// shard's territory is ~150 separate arcs, so "move a range" means "reassign some
// virtual nodes", and the keys that move are exactly those hashing into the
// reassigned arcs. That is a real advantage over range partitioning: no key
// comparison, no split point to choose, and the moved fraction is predictable
// from how many vnodes move.
type Migration struct {
	ID   string
	From ID
	To   ID

	// VNodes are the virtual node indices of From being reassigned to To.
	VNodes []int

	phase MigrationPhase

	// moved counts keys copied to the destination, for progress reporting.
	moved int

	// err records why an aborted migration was abandoned.
	err error
}

// Phase returns the migration's current phase.
func (m *Migration) Phase() MigrationPhase { return m.phase }

// Err returns why the migration aborted, if it did.
func (m *Migration) Err() error { return m.err }

// migrationTable tracks in-flight moves and answers the only question that
// matters on the hot path: for this key, right now, who is authoritative?
type migrationTable struct {
	mu sync.RWMutex

	// active is keyed by migration id. Several ranges can be in flight at once as
	// long as they do not overlap; overlapping moves of the same vnode are
	// refused at Begin, because two migrations disagreeing about one key's owner
	// is the exact hazard this file exists to prevent.
	active map[string]*Migration

	// claimed maps a (shard, vnode) being moved to its migration, so ownership
	// resolution is a map lookup rather than a scan over every active move.
	claimed map[vnodeKey]*Migration
}

type vnodeKey struct {
	shard ID
	index int
}

func newMigrationTable() *migrationTable {
	return &migrationTable{
		active:  make(map[string]*Migration),
		claimed: make(map[vnodeKey]*Migration),
	}
}

// begin registers a migration, refusing any move that overlaps one in flight.
func (t *migrationTable) begin(m *Migration) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.active[m.ID]; exists {
		return fmt.Errorf("shard: migration %s is already running", m.ID)
	}

	// Overlap check before claiming anything, so a rejected migration leaves the
	// table exactly as it found it.
	for _, v := range m.VNodes {
		k := vnodeKey{shard: m.From, index: v}
		if other, taken := t.claimed[k]; taken {
			return fmt.Errorf(
				"shard: vnode %d of %s is already moving under migration %s; "+
					"two migrations claiming one key is how a ledger ends up with two truths",
				v, m.From, other.ID)
		}
	}

	for _, v := range m.VNodes {
		t.claimed[vnodeKey{shard: m.From, index: v}] = m
	}
	t.active[m.ID] = m
	return nil
}

// ownerOf resolves who is authoritative for a vnode of a shard right now.
//
// Returns the shard to route to, and whether writes are currently frozen for it.
// This is called on every operation, so it takes a read lock and does one map
// lookup — the migration machinery must not become a cost paid by every
// transaction in the cluster.
func (t *migrationTable) ownerOf(s ID, vnode int) (owner ID, frozen bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, moving := t.claimed[vnodeKey{shard: s, index: vnode}]
	if !moving {
		return s, false
	}

	switch m.phase {
	case MigPreparing:
		// Source still authoritative and still accepting writes. The destination is
		// being filled in the background and is not yet allowed to answer.
		return m.From, false

	case MigFrozen:
		// Still the source's data — reads are served from it — but new writes are
		// refused so the final delta cannot be overtaken by a write that arrives
		// after it was computed.
		return m.From, true

	case MigCutover, MigDone:
		return m.To, false

	case MigAborted:
		return m.From, false
	}
	return s, false
}

// advance moves a migration to the next phase, enforcing the legal order.
//
// Phases only move forward. Allowing Frozen -> Preparing would mean the freeze
// cost availability for nothing, and worse, a write admitted after the final
// delta was computed would be silently left behind on the source.
func (t *migrationTable) advance(id string, to MigrationPhase) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.active[id]
	if !ok {
		return fmt.Errorf("shard: no migration %s", id)
	}

	legal := map[MigrationPhase][]MigrationPhase{
		MigPreparing: {MigFrozen, MigAborted},
		MigFrozen:    {MigCutover, MigAborted},
		MigCutover:   {MigDone},
	}
	allowed, ok := legal[m.phase]
	if !ok {
		return fmt.Errorf("shard: migration %s is %s and cannot advance", id, m.phase)
	}
	for _, a := range allowed {
		if a == to {
			m.phase = to
			return nil
		}
	}
	return fmt.Errorf("shard: migration %s cannot go %s -> %s", id, m.phase, to)
}

// finish removes a completed or aborted migration and releases its claims.
//
// Only legal from a terminal phase. Releasing claims mid-flight would make the
// moving keys resolve to the source while the destination believed it owned
// them.
func (t *migrationTable) finish(id string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.active[id]
	if !ok {
		return fmt.Errorf("shard: no migration %s", id)
	}
	if m.phase != MigDone && m.phase != MigAborted {
		return fmt.Errorf("shard: migration %s is %s, not finished", id, m.phase)
	}

	for _, v := range m.VNodes {
		delete(t.claimed, vnodeKey{shard: m.From, index: v})
	}
	delete(t.active, id)
	return nil
}

// list returns the in-flight migrations.
func (t *migrationTable) list() []*Migration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]*Migration, 0, len(t.active))
	for _, m := range t.active {
		out = append(out, m)
	}
	return out
}
