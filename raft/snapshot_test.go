package raft

import (
	"fmt"
	"testing"
)

// memStorage is an in-memory Storage implementing every optional extension.
//
// raft must not import storage — the dependency is one-directional, raft ->
// storage's interfaces only — so these tests carry their own backend. It is not a
// weaker test for it: what is under test here is Raft's compaction logic, and the
// real file-backed path is exercised end to end in sim/ and rpc/.
type memStorage struct {
	state    []byte
	applied  uint64
	snapIdx  uint64
	snapTerm uint64
	snapData []byte
	hasSnap  bool
}

func (m *memStorage) Save(state []byte) error {
	m.state = append([]byte(nil), state...)
	return nil
}

func (m *memStorage) Load() ([]byte, error) {
	if m.state == nil {
		return nil, nil
	}
	return append([]byte(nil), m.state...), nil
}

func (m *memStorage) SaveApplied(index uint64) error { m.applied = index; return nil }
func (m *memStorage) LoadApplied() (uint64, error)   { return m.applied, nil }

func (m *memStorage) SaveSnapshot(idx, term uint64, data []byte) error {
	m.snapIdx, m.snapTerm = idx, term
	m.snapData = append([]byte(nil), data...)
	m.hasSnap = true
	return nil
}

func (m *memStorage) LoadSnapshot() (uint64, uint64, []byte, bool, error) {
	if !m.hasSnap {
		return 0, 0, nil, false, nil
	}
	return m.snapIdx, m.snapTerm, append([]byte(nil), m.snapData...), true, nil
}

// Raft-level snapshotting and log compaction tests (§7).
//
// Per RULES.md rule 3 these cover: the normal path (compact, then keep working),
// the restart path (a compacted node comes back identical), the failure paths (a
// state machine that cannot snapshot; a snapshot arriving for state we already
// have), and the safety path — Log Matching and State Machine Safety asserted
// ACROSS a compaction boundary, which is where compaction could break them.
//
// The measurement this exists to fix is recorded in DESIGN.md: RaftState.Save
// rewrites the whole state per persist, measured at 481x write amplification at
// 800 entries.

// countingSnapSM is a state machine that can snapshot: it sums the bytes it has
// applied, so a restored copy is trivially comparable to a replayed one.
type countingSnapSM struct {
	total   uint64
	applied []string
}

func (c *countingSnapSM) Apply(cmd []byte) any {
	for _, b := range cmd {
		c.total += uint64(b)
	}
	c.applied = append(c.applied, string(cmd))
	return c.total
}

func (c *countingSnapSM) Snapshot() ([]byte, error) {
	// Deliberately includes the applied list, not just the total: a snapshot that
	// captures only a summary would pass a balance check while losing history.
	out := fmt.Sprintf("%d", c.total)
	for _, a := range c.applied {
		out += "|" + a
	}
	return []byte(out), nil
}

func (c *countingSnapSM) RestoreSnapshot(data []byte) error {
	var total uint64
	var applied []string
	parts := splitAll(string(data), '|')
	if len(parts) == 0 {
		return fmt.Errorf("empty snapshot")
	}
	if _, err := fmt.Sscanf(parts[0], "%d", &total); err != nil {
		return fmt.Errorf("bad snapshot header %q: %w", parts[0], err)
	}
	applied = append(applied, parts[1:]...)
	c.total, c.applied = total, applied
	return nil
}

func splitAll(s string, sep byte) []string {
	var out []string
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(s[i])
	}
	return append(out, cur)
}

// plainSM cannot snapshot, so it must never be compacted.
type plainSM struct{ n int }

func (p *plainSM) Apply([]byte) any { p.n++; return p.n }

// newSnapServer builds a single-node server over the given storage.
func newSnapServer(t *testing.T, store Storage, sm StateMachine) *Server {
	t.Helper()
	s := NewServer("n1", []NodeID{"n1"}, sm)
	s.SetStorage(store)
	return s
}

// commitN appends n entries and marks them applied, without running the role
// loop — this isolates compaction from election timing.
func commitN(t *testing.T, s *Server, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Leader
	s.currentTerm = 1
	for i := range n {
		idx := s.lastIndex() + 1
		s.log = append(s.log, LogEntry{Term: 1, Index: idx, Command: []byte(fmt.Sprintf("cmd-%d", i))})
	}
	s.commitIndex = s.lastIndex()
	s.applyCommitted()
	if err := s.persistLocked(); err != nil {
		t.Fatalf("persist: %v", err)
	}
}

// --- compaction -----------------------------------------------------------

// The basic property: compaction discards the log prefix and the state machine
// is unchanged.
func TestCompactionDiscardsPrefixAndKeepsState(t *testing.T) {
	store := &memStorage{}
	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)

	commitN(t, s, 50)
	before := sm.total
	beforeLen := len(s.LogEntries())

	did, err := s.MaybeCompact(10)
	if err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}
	if !did {
		t.Fatalf("no compaction with %d entries against a threshold of 10", beforeLen-1)
	}

	idx, term, has := s.SnapshotInfo()
	if !has {
		t.Fatal("compaction reported success but no snapshot was recorded")
	}
	if idx != 50 {
		t.Fatalf("snapshot covers index %d, want 50 (lastApplied)", idx)
	}
	if term != 1 {
		t.Fatalf("snapshot term = %d, want 1", term)
	}

	// The log is now the sentinel alone: everything was applied and compacted.
	if got := len(s.LogEntries()); got != 1 {
		t.Fatalf("log holds %d entries after compacting through the end, want 1 (the "+
			"snapshot sentinel)", got)
	}
	if sm.total != before {
		t.Fatalf("state machine changed during compaction: %d -> %d", before, sm.total)
	}

	// And the boundary is still answerable, which is what keeps replication working.
	s.mu.Lock()
	base, baseT := s.baseIndex(), s.baseTerm()
	s.mu.Unlock()
	if base != 50 || baseT != 1 {
		t.Fatalf("sentinel = (index %d, term %d), want (50, 1) — without these the log "+
			"is unmatched at its own boundary and replication stalls forever", base, baseT)
	}
}

// Compaction must never discard entries the state machine has not applied.
//
// A snapshot is a picture of the state machine, so an entry that is committed but
// not yet applied would be lost entirely: gone from the log and absent from the
// snapshot.
func TestCompactionNeverDiscardsUnappliedEntries(t *testing.T) {
	store := &memStorage{}
	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)

	commitN(t, s, 30)

	// Append 10 more that are committed but deliberately NOT applied.
	s.mu.Lock()
	for i := range 10 {
		idx := s.lastIndex() + 1
		s.log = append(s.log, LogEntry{Term: 1, Index: idx, Command: []byte(fmt.Sprintf("late-%d", i))})
	}
	s.commitIndex = s.lastIndex()
	applied := s.lastApplied
	s.mu.Unlock()

	if _, err := s.MaybeCompact(5); err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}

	idx, _, _ := s.SnapshotInfo()
	if idx > applied {
		t.Fatalf("compacted through index %d but only %d was applied — the entries "+
			"between are in neither the log nor the snapshot", idx, applied)
	}

	// The unapplied entries must still be there.
	s.mu.Lock()
	last := s.lastIndex()
	_, stillThere := s.entryAt(applied + 1)
	s.mu.Unlock()
	if last != 40 {
		t.Fatalf("last index = %d, want 40", last)
	}
	if !stillThere {
		t.Fatalf("entry %d was discarded despite not being applied", applied+1)
	}
}

// A state machine that cannot snapshot must never be compacted — the log is the
// only copy of its state.
func TestStateMachineWithoutSnapshotSupportIsNeverCompacted(t *testing.T) {
	store := &memStorage{}
	s := newSnapServer(t, store, &plainSM{})

	commitN(t, s, 40)

	did, err := s.MaybeCompact(5)
	if err != nil {
		t.Fatalf("MaybeCompact returned an error for an unsnapshottable machine: %v", err)
	}
	if did {
		t.Fatal("a state machine that cannot snapshot was compacted; its state exists " +
			"only in the log that was just discarded")
	}
	if got := len(s.LogEntries()); got != 41 {
		t.Fatalf("log length %d, want 41 — nothing should have been discarded", got)
	}
}

// Below the threshold, nothing happens: compaction is not free and should not run
// on a short log.
func TestCompactionRespectsThreshold(t *testing.T) {
	store := &memStorage{}
	s := newSnapServer(t, store, &countingSnapSM{})

	commitN(t, s, 5)
	did, err := s.MaybeCompact(100)
	if err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}
	if did {
		t.Fatal("compacted a 5-entry log against a threshold of 100")
	}
	if _, _, has := s.SnapshotInfo(); has {
		t.Fatal("a snapshot was recorded despite no compaction")
	}
}

// --- restart across a compaction boundary --------------------------------

// The property that matters most: a compacted node restarts with IDENTICAL
// state. If the snapshot is not loaded before replay, the node rebuilds from a
// partial history and comes back wrong while looking healthy.
func TestRestartAfterCompactionRestoresIdenticalState(t *testing.T) {
	store := &memStorage{}

	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)
	commitN(t, s, 60)

	if did, err := s.MaybeCompact(10); err != nil || !did {
		t.Fatalf("MaybeCompact: did=%v err=%v", did, err)
	}
	// A few more entries after the snapshot, so restore must combine BOTH the
	// snapshot and the log tail.
	commitN(t, s, 5)

	wantTotal := sm.total
	wantApplied := len(sm.applied)

	// Restart: a fresh server and a fresh, EMPTY state machine over the same files.
	restored := &countingSnapSM{}
	s2 := newSnapServer(t, store, restored)
	if err := s2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.total != wantTotal {
		t.Fatalf("restored total = %d, want %d — the snapshot and the log tail did not "+
			"combine into the same state", restored.total, wantTotal)
	}
	if len(restored.applied) != wantApplied {
		t.Fatalf("restored %d applied commands, want %d", len(restored.applied), wantApplied)
	}
	if got := s2.LastApplied(); got != 65 {
		t.Fatalf("lastApplied after restore = %d, want 65", got)
	}
}

// Restoring must not re-apply entries the snapshot already covers.
//
// Replay starting at index 1 instead of after the boundary would double-count
// everything in the snapshot — the state machine would look plausible and be
// wrong by exactly the compacted prefix.
func TestRestoreDoesNotReapplySnapshottedEntries(t *testing.T) {
	store := &memStorage{}

	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)
	commitN(t, s, 40)
	if did, _ := s.MaybeCompact(10); !did {
		t.Fatal("setup: no compaction")
	}
	want := sm.total

	restored := &countingSnapSM{}
	s2 := newSnapServer(t, store, restored)
	if err := s2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.total != want {
		t.Fatalf("restored total = %d, want %d (double-counted by %d)",
			restored.total, want, restored.total-want)
	}
	if len(restored.applied) != 40 {
		t.Fatalf("restored applied %d commands, want exactly 40 — entries covered by "+
			"the snapshot were replayed on top of it", len(restored.applied))
	}
}

// A node whose snapshot exists but whose state machine cannot consume it must
// refuse to boot rather than serve reads from a partial state.
func TestRestoreRefusesSnapshotAnUnsnapshottableMachineCannotUse(t *testing.T) {
	store := &memStorage{}

	s := newSnapServer(t, store, &countingSnapSM{})
	commitN(t, s, 30)
	if did, _ := s.MaybeCompact(5); !did {
		t.Fatal("setup: no compaction")
	}

	// Restart with a state machine that cannot restore snapshots.
	s2 := newSnapServer(t, store, &plainSM{})
	if err := s2.Restore(); err == nil {
		t.Fatal("a node booted with a snapshot its state machine cannot restore; it " +
			"would serve reads missing everything before the boundary")
	}
}

// --- InstallSnapshot ------------------------------------------------------

// A follower behind the leader's compacted prefix is caught up by the snapshot.
func TestInstallSnapshotCatchesUpALaggingFollower(t *testing.T) {
	store := &memStorage{}

	// The leader's state, compacted.
	leaderSM := &countingSnapSM{}
	leader := newSnapServer(t, store, leaderSM)
	commitN(t, leader, 40)
	if did, _ := leader.MaybeCompact(5); !did {
		t.Fatal("setup: leader did not compact")
	}
	idx, term, _ := leader.SnapshotInfo()
	data, err := leaderSM.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// A follower that has seen nothing.
	followerSM := &countingSnapSM{}
	follower := newSnapServer(t, &memStorage{}, followerSM)

	reply := follower.InstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "n1",
		LastIncludedIndex: idx, LastIncludedTerm: term,
		Data: data, Done: true,
	})
	if reply.Term != 1 {
		t.Fatalf("reply term = %d, want 1", reply.Term)
	}

	if followerSM.total != leaderSM.total {
		t.Fatalf("follower total = %d, leader = %d — the snapshot did not catch it up",
			followerSM.total, leaderSM.total)
	}
	if got := follower.LastApplied(); got != idx {
		t.Fatalf("follower lastApplied = %d, want %d — leaving it behind would re-apply "+
			"entries the snapshot already contains", got, idx)
	}
	if got := follower.CommitIndex(); got < idx {
		t.Fatalf("follower commitIndex = %d, want at least %d", got, idx)
	}
}

// A stale InstallSnapshot must be rejected: a snapshot older than what we have
// would rewind a state machine that is ahead of it, losing committed entries.
func TestStaleInstallSnapshotIsIgnored(t *testing.T) {
	store := &memStorage{}
	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)
	commitN(t, s, 40)
	before := sm.total

	// A snapshot covering only index 5, far behind where we are.
	old := &countingSnapSM{}
	old.Apply([]byte("stale"))
	staleData, _ := old.Snapshot()

	s.InstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "n2",
		LastIncludedIndex: 5, LastIncludedTerm: 1,
		Data: staleData, Done: true,
	})

	if sm.total != before {
		t.Fatalf("a stale snapshot rewound the state machine: %d -> %d", before, sm.total)
	}
	if got := s.LastApplied(); got != 40 {
		t.Fatalf("lastApplied = %d after a stale snapshot, want 40", got)
	}
}

// An InstallSnapshot at a lower term must be rejected outright (Figure 13 rule 1).
func TestInstallSnapshotFromStaleTermIsRejected(t *testing.T) {
	store := &memStorage{}
	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)

	s.mu.Lock()
	s.currentTerm = 5
	s.mu.Unlock()

	before := sm.total
	reply := s.InstallSnapshot(InstallSnapshotArgs{
		Term: 3, LeaderID: "old-leader",
		LastIncludedIndex: 10, LastIncludedTerm: 3,
		Data: []byte("99"), Done: true,
	})

	if reply.Term != 5 {
		t.Fatalf("reply term = %d, want 5 so the stale leader steps down", reply.Term)
	}
	if sm.total != before {
		t.Fatal("a snapshot from a stale term was applied")
	}
}

// A higher term in InstallSnapshot must make this server step down — Figure 2's
// unconditional rule applies to every RPC, including this one.
func TestInstallSnapshotWithHigherTermStepsDown(t *testing.T) {
	store := &memStorage{}
	s := newSnapServer(t, store, &countingSnapSM{})

	s.mu.Lock()
	s.currentTerm = 2
	s.role = Leader
	s.mu.Unlock()

	sm := &countingSnapSM{}
	sm.Apply([]byte("x"))
	data, _ := sm.Snapshot()

	reply := s.InstallSnapshot(InstallSnapshotArgs{
		Term: 9, LeaderID: "n2",
		LastIncludedIndex: 3, LastIncludedTerm: 9,
		Data: data, Done: true,
	})

	if reply.Term != 9 {
		t.Fatalf("reply term = %d, want 9", reply.Term)
	}
	if got := s.Role(); got != Follower {
		t.Fatalf("role = %v after a higher-term InstallSnapshot, want Follower", got)
	}
	if got := s.CurrentTerm(); got != 9 {
		t.Fatalf("currentTerm = %d, want 9", got)
	}
}

// Figure 13 rule 6: if an existing entry matches the snapshot's last index and
// term, the log AFTER it is retained rather than thrown away.
func TestInstallSnapshotRetainsMatchingLogTail(t *testing.T) {
	store := &memStorage{}
	sm := &countingSnapSM{}
	s := newSnapServer(t, store, sm)

	commitN(t, s, 20)
	s.mu.Lock()
	tailEntry, _ := s.entryAt(15)
	lastBefore := s.lastIndex()
	s.mu.Unlock()

	// A snapshot covering index 10 with a matching term. lastApplied is already
	// 20, so this is stale and must be ignored — asserting the guard, not the
	// retention. Retention is exercised by the leader-side compaction path, which
	// discardThrough covers directly.
	other := &countingSnapSM{}
	other.Apply([]byte("y"))
	data, _ := other.Snapshot()

	s.InstallSnapshot(InstallSnapshotArgs{
		Term: 1, LeaderID: "n2",
		LastIncludedIndex: 10, LastIncludedTerm: tailEntry.Term,
		Data: data, Done: true,
	})

	s.mu.Lock()
	lastAfter := s.lastIndex()
	s.mu.Unlock()
	if lastAfter != lastBefore {
		t.Fatalf("last index %d -> %d: a snapshot behind lastApplied truncated the log",
			lastBefore, lastAfter)
	}
}

// --- safety across a compaction boundary ---------------------------------

// Log Matching (Figure 3) must hold across a compaction boundary.
//
// The property: if two logs contain an entry with the same index and term, the
// logs are identical up through that index. Compaction discards entries, so the
// boundary is exactly where this could break — a compacted server must still
// agree with an uncompacted one about every entry they both hold.
func TestLogMatchingHoldsAcrossCompactionBoundary(t *testing.T) {
	storeA, storeB := &memStorage{}, &memStorage{}

	a := newSnapServer(t, storeA, &countingSnapSM{})
	b := newSnapServer(t, storeB, &countingSnapSM{})

	commitN(t, a, 40)
	commitN(t, b, 40)

	// Compact only A.
	if did, _ := a.MaybeCompact(5); !did {
		t.Fatal("setup: A did not compact")
	}

	entriesA, entriesB := a.LogEntries(), b.LogEntries()

	// Every entry A still holds must match B's at the same index.
	byIndexB := make(map[Index]LogEntry, len(entriesB))
	for _, e := range entriesB {
		byIndexB[e.Index] = e
	}
	checked := 0
	for _, e := range entriesA {
		if e.Index == 0 {
			continue
		}
		other, ok := byIndexB[e.Index]
		if !ok {
			t.Fatalf("compacted server holds entry %d that the uncompacted one does not", e.Index)
		}
		if other.Term != e.Term {
			t.Fatalf("Log Matching violated at index %d: terms %d vs %d",
				e.Index, e.Term, other.Term)
		}
		checked++
	}

	// The sentinel must agree with the real entry it replaced.
	a.mu.Lock()
	base, baseTerm := a.baseIndex(), a.baseTerm()
	a.mu.Unlock()
	if orig, ok := byIndexB[base]; ok && orig.Term != baseTerm {
		t.Fatalf("the snapshot sentinel claims index %d has term %d, but the real entry "+
			"has term %d — the compacted log lies about its own boundary",
			base, baseTerm, orig.Term)
	}
	t.Logf("compacted log holds %d entries (base index %d); all agree with the "+
		"uncompacted log", checked, base)
}

// State Machine Safety (Figure 3): a compacted server and an uncompacted one that
// applied the same log must reach exactly the same state.
func TestStateMachineSafetyHoldsAcrossCompaction(t *testing.T) {
	storeA, storeB := &memStorage{}, &memStorage{}

	smA, smB := &countingSnapSM{}, &countingSnapSM{}
	a := newSnapServer(t, storeA, smA)
	b := newSnapServer(t, storeB, smB)

	commitN(t, a, 50)
	commitN(t, b, 50)

	if did, _ := a.MaybeCompact(10); !did {
		t.Fatal("setup: A did not compact")
	}
	commitN(t, a, 10)
	commitN(t, b, 10)

	if smA.total != smB.total {
		t.Fatalf("compacted and uncompacted state machines diverged: %d vs %d",
			smA.total, smB.total)
	}
	if len(smA.applied) != len(smB.applied) {
		t.Fatalf("applied %d vs %d commands", len(smA.applied), len(smB.applied))
	}
	for i := range smA.applied {
		if smA.applied[i] != smB.applied[i] {
			t.Fatalf("State Machine Safety violated at position %d: %q vs %q",
				i, smA.applied[i], smB.applied[i])
		}
	}
}

// --- the wall this exists to remove --------------------------------------

// Compaction must actually reduce the bytes written per persist.
//
// DESIGN.md measured the problem: RaftState.Save rewrites the whole state on
// every persist, 481x write amplification at 800 entries. This asserts the fix
// lands rather than merely that the code runs.
func TestCompactionReducesPersistedStateSize(t *testing.T) {
	store := &memStorage{}
	s := newSnapServer(t, store, &countingSnapSM{})

	commitN(t, s, 400)

	before, err := s.storage.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if did, err := s.MaybeCompact(10); err != nil || !did {
		t.Fatalf("MaybeCompact: did=%v err=%v", did, err)
	}

	after, err := s.storage.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(after) >= len(before) {
		t.Fatalf("persisted state did not shrink: %d -> %d bytes. Compaction exists to "+
			"stop every subsequent Save rewriting the whole history",
			len(before), len(after))
	}
	t.Logf("persisted state: %d -> %d bytes after compacting 400 entries (%.1fx smaller)",
		len(before), len(after), float64(len(before))/float64(len(after)))
}
