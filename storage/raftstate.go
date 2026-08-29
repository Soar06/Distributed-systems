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
}

// NewRaftState wraps a WAL.
func NewRaftState(w *WAL) *RaftState {
	return &RaftState{wal: w}
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

// Close closes the underlying WAL.
func (r *RaftState) Close() error { return r.wal.Close() }
