package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/homura/core-bank/raft"
)

// raft.Snapshotter for CountingSM, so simulator clusters can be compacted.
//
// Without this the simulator could never exercise §7 at all: MaybeCompact
// silently declines for a state machine that cannot serialize itself, so every
// compaction test would have quietly passed by doing nothing. That is exactly how
// the InstallSnapshot send path went missing — the receiver existed, the tests
// looked green, and no leader ever sent one.
//
// CountingSM's convenient []string view is AppliedCopy(); it was renamed from
// Snapshot() so that this method can carry the name raft.Snapshotter requires.
// The collision was silent: with a []string Snapshot(), CountingSM could never
// satisfy the interface, MaybeCompact declined for every simulator cluster, and
// every compaction test passed by doing nothing at all.

// Snapshot implements the serializing half of raft.Snapshotter.
//
// Format: [8 count] then per entry [8 len][bytes]. Fixed-width little-endian,
// matching the discipline in raft/persist.go and ledger/snapshot.go.
func (c *CountingSM) Snapshot() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var b bytes.Buffer
	var scratch [8]byte

	binary.LittleEndian.PutUint64(scratch[:], uint64(len(c.Applied)))
	b.Write(scratch[:])
	for _, a := range c.Applied {
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(a)))
		b.Write(scratch[:])
		b.WriteString(a)
	}
	return b.Bytes(), nil
}

// RestoreSnapshot implements the restoring half of raft.Snapshotter.
//
// Replaces rather than merges: a snapshot is a complete picture, and merging it
// into existing state would produce a machine matching no log position.
func (c *CountingSM) RestoreSnapshot(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("sim: snapshot too short (%d bytes)", len(data))
	}
	n := binary.LittleEndian.Uint64(data[0:8])
	pos := 8

	applied := make([]string, 0, n)
	for range n {
		if pos+8 > len(data) {
			return fmt.Errorf("sim: truncated snapshot at entry header %d", len(applied))
		}
		size := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		// The declared length is checked against what is present before being used
		// as a slice bound.
		if uint64(len(data)-pos) < size {
			return fmt.Errorf("sim: truncated snapshot: entry declares %d bytes, %d remain",
				size, len(data)-pos)
		}
		applied = append(applied, string(data[pos:pos+int(size)]))
		pos += int(size)
	}

	// Decoded fully before anything is assigned: a failed restore must leave the
	// machine exactly as it was.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Applied = applied
	return nil
}

// Compile-time proof that CountingSM really does satisfy raft.Snapshotter.
//
// Asserted rather than assumed: the interface was silently unsatisfied before,
// and nothing failed — MaybeCompact simply declined and every compaction test
// passed by doing nothing.
var _ raft.Snapshotter = (*CountingSM)(nil)
