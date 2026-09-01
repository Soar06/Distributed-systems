package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// RaftState adapts a WAL to the raft.Storage interface.
//
// Raft's persistent state is saved as a full snapshot record each time rather
// than as incremental edits. That is deliberate for Phase 1: it is obviously
// correct (the last intact record is the state), at the cost of rewriting the log
// on every append. Incremental records plus periodic compaction is the
// optimization, and it belongs with snapshotting in LATER.md — not before the
// simple version is proven.
type RaftState struct {
	// path is the state file. Save replaces it atomically by rename, so all
	// access goes through the path rather than a cached file handle.
	path string

	// applied optionally records how far the state machine has been applied, so a
	// restart can replay the log and come back with the same state rather than an
	// empty one. Nil means the feature is not in use.
	applied *AppliedFile
}

// NewRaftState wraps a WAL, using its path as the state file.
//
// Deprecated: prefer OpenRaftState, which takes the path directly. Holding the
// file open is not merely unnecessary now that Save replaces it by rename — on
// Windows an open handle makes the rename fail outright.
func NewRaftState(w *WAL) *RaftState {
	return &RaftState{path: w.Path()}
}

// OpenRaftState creates state storage for the file at path, optionally with an
// applied-index file alongside it.
//
// Nothing is held open: Save replaces the file atomically by rename and Load
// reads it by path, so an open handle would only get in the way.
func OpenRaftState(path string, applied *AppliedFile) *RaftState {
	return &RaftState{path: path, applied: applied}
}

// NewRaftStateWithApplied is NewRaftState plus an applied-index file, making the
// result satisfy raft.AppliedStorage as well as raft.Storage.
func NewRaftStateWithApplied(w *WAL, a *AppliedFile) *RaftState {
	return &RaftState{path: w.Path(), applied: a}
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
// This is an ATOMIC whole-file replacement, not truncate-then-append.
//
// The earlier truncate-then-append left a window in which the file was durably
// EMPTY. The comment that used to sit here claimed that was survivable because
// "a node that loses its state is indistinguishable from a brand-new node" —
// that reasoning is wrong and was the most dangerous line in this package. A
// brand-new node has a new identity; a node that keeps its ID but forgets
// votedFor can grant a SECOND vote in a term it already voted in, producing two
// leaders in one term. It also silently discards log entries it has already
// acknowledged, while the leader still counts them as replicated.
//
// Figure 2 requires persistent state to survive a crash unconditionally. A
// reader now sees either the complete old state or the complete new state.
func (r *RaftState) Save(state []byte) error {
	// The record framing (length + CRC32) is kept so a torn or corrupt file is
	// still detectable, on top of the atomicity the rename provides.
	return writeFileAtomic(r.path, frameRecord(state))
}

// Load returns the most recent saved state, or nil if none.
// Load returns the most recent saved state, or nil if none.
//
// Reads the file by path rather than through a cached handle: Save replaces the
// file atomically via rename, so any handle opened earlier refers to the old,
// now-unlinked inode.
func (r *RaftState) Load() ([]byte, error) {
	payload, err := readFramedFile(r.path)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// Close closes the applied-index file. The state file itself is not held open —
// Save and Load each open it for the duration of the operation.
func (r *RaftState) Close() error {
	if r.applied != nil {
		return r.applied.Close()
	}
	return nil
}

// Snapshot persistence (§7).
//
// The snapshot lives in its own file beside the state file, written with the same
// atomic write-temp / fsync / rename / fsync-dir discipline. A separate file
// rather than a section of the state file, for one reason: the state file is
// rewritten on EVERY persist, and folding a large snapshot into it would multiply
// exactly the write amplification snapshotting exists to remove.

// snapshotPath returns the snapshot file's path.
func (r *RaftState) snapshotPath() string { return r.path + ".snapshot" }

// SaveSnapshot implements raft.SnapshotStorage.
//
// Written atomically: a torn snapshot is worse than no snapshot, because the log
// prefix it covers has been discarded and cannot be replayed instead.
func (r *RaftState) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	rec := encodeSnapshotRecord(lastIncludedIndex, lastIncludedTerm, data)
	return writeFileAtomic(r.snapshotPath(), rec)
}

// LoadSnapshot implements raft.SnapshotStorage.
//
// A corrupt or truncated snapshot is reported as "no snapshot" rather than as an
// error, and this is safe ONLY because the log covering it is still on disk: the
// state file is written after the snapshot, never before, so a node that cannot
// read its snapshot can always fall back to replaying the log. Refusing to boot
// would turn a recoverable state into an outage, which is the same reasoning
// AppliedFile.Load already applies.
func (r *RaftState) LoadSnapshot() (uint64, uint64, []byte, bool, error) {
	raw, err := os.ReadFile(r.snapshotPath())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, false, nil
		}
		return 0, 0, nil, false, fmt.Errorf("storage: read snapshot: %w", err)
	}

	idx, term, data, ok := decodeSnapshotRecord(raw)
	if !ok {
		return 0, 0, nil, false, nil
	}
	return idx, term, data, true, nil
}

// encodeSnapshotRecord serializes a snapshot with a CRC32 over the payload.
//
// Layout: [8 index][8 term][8 len][4 crc][data]. The checksum is what makes a
// torn write detectable rather than silently restored as truncated state.
func encodeSnapshotRecord(index, term uint64, data []byte) []byte {
	buf := make([]byte, 28+len(data))
	binary.LittleEndian.PutUint64(buf[0:8], index)
	binary.LittleEndian.PutUint64(buf[8:16], term)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(data)))
	binary.LittleEndian.PutUint32(buf[24:28], crc32.ChecksumIEEE(data))
	copy(buf[28:], data)
	return buf
}

// decodeSnapshotRecord parses and verifies a record. ok=false means the record is
// unusable and the caller must fall back to the log.
func decodeSnapshotRecord(raw []byte) (index, term uint64, data []byte, ok bool) {
	if len(raw) < 28 {
		return 0, 0, nil, false
	}
	index = binary.LittleEndian.Uint64(raw[0:8])
	term = binary.LittleEndian.Uint64(raw[8:16])
	n := binary.LittleEndian.Uint64(raw[16:24])
	sum := binary.LittleEndian.Uint32(raw[24:28])

	// Length validated before it is trusted: a declared size must never drive an
	// allocation or a slice bound on its own.
	if uint64(len(raw)-28) != n {
		return 0, 0, nil, false
	}
	body := raw[28:]
	if crc32.ChecksumIEEE(body) != sum {
		return 0, 0, nil, false
	}

	out := make([]byte, n)
	copy(out, body)
	return index, term, out, true
}
