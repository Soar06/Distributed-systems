package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Durable record of how far the state machine has been applied.
//
// This is deliberately a SEPARATE file from the Raft state WAL. The Raft log must
// be written before an RPC reply (Figure 2); the applied index is written after
// entries are applied. Keeping them apart means neither write can tear the other,
// and the applied marker can be rewritten frequently without touching the log.
//
// The file is tiny and fixed-size, so it is written whole each time: an 8-byte
// index plus its checksum. A torn write is detected and treated as "nothing
// applied", which is safe — replay simply starts from the beginning, and applying
// the same log in the same order is deterministic.

const appliedRecordSize = 12 // 8 bytes index + 4 bytes CRC

// AppliedFile stores the last-applied log index durably.
type AppliedFile struct {
	mu   sync.Mutex
	f    *os.File
	path string
	sync bool
}

// OpenApplied opens or creates the applied-index file at path.
func OpenApplied(path string) (*AppliedFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open applied: %w", err)
	}
	return &AppliedFile{f: f, path: path, sync: true}, nil
}

// Save records the applied index durably.
func (a *AppliedFile) Save(index uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var rec [appliedRecordSize]byte
	binary.LittleEndian.PutUint64(rec[0:8], index)
	binary.LittleEndian.PutUint32(rec[8:12], checksum(rec[0:8]))

	if _, err := a.f.WriteAt(rec[:], 0); err != nil {
		return fmt.Errorf("storage: write applied: %w", err)
	}
	if a.sync {
		if err := a.f.Sync(); err != nil {
			return fmt.Errorf("storage: fsync applied: %w", err)
		}
	}
	return nil
}

// Load returns the recorded applied index, or 0 if none is recorded or the
// record is damaged.
//
// A damaged record deliberately returns 0 rather than an error: replaying the
// whole log from the start is always correct, just slower. Refusing to start
// because a hint file is corrupt would turn a recoverable situation into an
// outage.
func (a *AppliedFile) Load() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var rec [appliedRecordSize]byte
	n, err := a.f.ReadAt(rec[:], 0)
	if err != nil || n < appliedRecordSize {
		return 0, nil // absent or short: treat as nothing applied
	}
	idx := binary.LittleEndian.Uint64(rec[0:8])
	want := binary.LittleEndian.Uint32(rec[8:12])
	if checksum(rec[0:8]) != want {
		return 0, nil // torn or corrupt: replay from the start
	}
	return idx, nil
}

// SetSync controls fsync-on-write. Only for benchmarks.
func (a *AppliedFile) SetSync(on bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sync = on
}

// Close closes the file.
func (a *AppliedFile) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
}
