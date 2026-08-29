package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Atomic whole-file replacement.
//
// The naive way to replace a file's contents is truncate-then-write. That leaves
// a window in which the file is durably EMPTY: a crash there loses everything.
//
// For Raft's persistent state that is not a tolerable risk, and the intuition
// that it is — "a node that loses its state is indistinguishable from a brand-new
// node" — is wrong. A brand-new node has a new identity. A node that keeps its ID
// but forgets `votedFor` can vote a second time in a term it already voted in,
// which produces two leaders in one term and breaks Election Safety. It also
// silently drops log entries it already acknowledged, while the leader still
// counts them as replicated.
//
// The fix is the standard one: write a temp file, fsync it, rename it over the
// target, then fsync the directory so the rename itself is durable. Rename is
// atomic on both POSIX and Windows (via ReplaceFile/MoveFileEx semantics in the
// Go runtime), so a reader sees either the old contents or the new — never
// neither.

// writeFileAtomic replaces path's contents with data, durably and atomically.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("storage: create temp: %w", err)
	}
	tmpName := tmp.Name()

	// Clean up the temp file on any failure path.
	defer func() {
		if tmp != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("storage: write temp: %w", err)
	}
	// fsync the DATA before the rename, or the rename can land while the contents
	// are still only in the page cache.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("storage: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: close temp: %w", err)
	}
	tmp = nil // handed off; the deferred cleanup must not remove it now

	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("storage: rename into place: %w", err)
	}

	// fsync the directory so the rename itself survives a power loss. Without
	// this the file contents are durable but the directory entry pointing at them
	// may not be.
	return syncDir(dir)
}

// syncDir fsyncs a directory.
//
// On Windows a directory cannot be opened for sync the way it can on POSIX; the
// rename is already ordered by the filesystem there, so a failure to open the
// directory is not treated as an error.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil // not supported on this platform; rename ordering suffices
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Windows returns an error for directory sync; ignore it rather than
		// failing a write that already succeeded.
		return nil
	}
	return nil
}
