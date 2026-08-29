package storage

// RaftState adapts a WAL to the raft.Storage interface.
//
// Raft's persistent state is saved as a full snapshot record each time rather
// than as incremental edits. That is deliberate for Phase 1: it is obviously
// correct (the last intact record is the state), at the cost of rewriting the log
// on every append. Incremental records plus periodic compaction is the
// optimization, and it belongs with snapshotting in LATER.md — not before the
// simple version is proven.
type RaftState struct {
	wal *WAL

	// applied optionally records how far the state machine has been applied, so a
	// restart can replay the log and come back with the same state rather than an
	// empty one. Nil means the feature is not in use.
	applied *AppliedFile
}

// NewRaftState wraps a WAL.
func NewRaftState(w *WAL) *RaftState {
	return &RaftState{wal: w}
}

// NewRaftStateWithApplied wraps a WAL and an applied-index file, making the
// result satisfy raft.AppliedStorage as well as raft.Storage.
func NewRaftStateWithApplied(w *WAL, a *AppliedFile) *RaftState {
	return &RaftState{wal: w, applied: a}
}

// SaveApplied implements raft.AppliedStorage.
func (r *RaftState) SaveApplied(index uint64) error {
	if r.applied == nil {
		return nil
	}
	return r.applied.Save(index)
}

// LoadApplied implements raft.AppliedStorage.
func (r *RaftState) LoadApplied() (uint64, error) {
	if r.applied == nil {
		return 0, nil
	}
	return r.applied.Load()
}

// Save durably records the state, replacing anything previously stored.
//
// Truncate-then-append is NOT atomic: a crash between the two leaves no state at
// all. That is survivable — a node that loses its state is indistinguishable from
// a brand-new node, and Raft handles new nodes safely. What must never happen is a
// *partial* or *stale-but-plausible* state, and the checksummed record format
// prevents that.
func (r *RaftState) Save(state []byte) error {
	if err := r.wal.Truncate(); err != nil {
		return err
	}
	return r.wal.Append(state)
}

// Load returns the most recent saved state, or nil if none.
func (r *RaftState) Load() ([]byte, error) {
	records, err := r.wal.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[len(records)-1], nil
}

// Close closes the underlying WAL and applied-index file.
func (r *RaftState) Close() error {
	if r.applied != nil {
		_ = r.applied.Close()
	}
	return r.wal.Close()
}
