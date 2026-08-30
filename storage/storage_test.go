package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Tests per RULES.md rule 3: normal, failure (torn write, corruption), retry
// (reopen), and concurrent flows.

func tempWAL(t *testing.T) *WAL {
	t.Helper()
	w, err := Open(filepath.Join(t.TempDir(), "raft.wal"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// --- Normal flow ----------------------------------------------------------

func TestAppendAndReadBack(t *testing.T) {
	w := tempWAL(t)

	want := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for _, r := range want {
		if err := w.Append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEmptyWALReadsEmpty(t *testing.T) {
	w := tempWAL(t)
	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty WAL returned %d records", len(got))
	}
}

func TestEmptyPayloadRoundTrips(t *testing.T) {
	w := tempWAL(t)
	if err := w.Append([]byte{}); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
}

// --- Durability across reopen (the restart flow) -------------------------

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := w.Append([]byte("durable")); err != nil {
		t.Fatalf("append: %v", err)
	}
	w.Close()

	// Reopen as a restarted process would.
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != 1 || string(got[0]) != "durable" {
		t.Fatalf("after reopen got %q, want [durable]", got)
	}
}

func TestAppendAfterReadAllContinues(t *testing.T) {
	w := tempWAL(t)
	w.Append([]byte("a"))
	if _, err := w.ReadAll(); err != nil {
		t.Fatalf("readall: %v", err)
	}
	// ReadAll seeks and truncates; appending afterwards must still work and must
	// not clobber the existing record.
	if err := w.Append([]byte("b")); err != nil {
		t.Fatalf("append after readall: %v", err)
	}

	got, _ := w.ReadAll()
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2: %q", len(got), got)
	}
}

// --- Failure flow: torn writes (the crash-recovery path) -----------------

// A process killed mid-append leaves a partial record. Recovery must drop it,
// not replay it as if it were complete.
func TestTornWriteAtTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, _ := Open(path)
	w.Append([]byte("complete-1"))
	w.Append([]byte("complete-2"))
	w.Close()

	// Simulate a crash mid-write: append a header claiming more bytes than
	// actually follow.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{50, 0, 0, 0, 0, 0, 0, 0}) // length=50, crc=0
	f.Write([]byte("only a few bytes"))      // far fewer than 50
	f.Close()

	w2, _ := Open(path)
	defer w2.Close()
	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 — torn tail must be dropped: %q", len(got), got)
	}
	if string(got[0]) != "complete-1" || string(got[1]) != "complete-2" {
		t.Fatalf("intact records corrupted: %q", got)
	}
}

// After recovery the file must be truncated, so the next append starts from a
// clean boundary rather than after the garbage.
func TestRecoveryTruncatesGarbage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, _ := Open(path)
	w.Append([]byte("good"))
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{99, 0, 0, 0, 0, 0, 0, 0})
	f.Write([]byte("garbage"))
	f.Close()

	w2, _ := Open(path)
	defer w2.Close()
	if _, err := w2.ReadAll(); err != nil {
		t.Fatalf("readall: %v", err)
	}
	if err := w2.Append([]byte("after-recovery")); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "good" || string(got[1]) != "after-recovery" {
		t.Fatalf("got %q, want [good after-recovery]", got)
	}
}

// A flipped bit in a payload must be caught by the checksum, not silently
// returned as valid data.
func TestChecksumCatchesCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, _ := Open(path)
	w.Append([]byte("original-content"))
	w.Append([]byte("second-record"))
	w.Close()

	// Corrupt a byte inside the FIRST record's payload (offset 8 = past header).
	data, _ := os.ReadFile(path)
	data[10] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	w2, _ := Open(path)
	defer w2.Close()
	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	// The corrupt record must not be returned as if valid.
	if len(got) > 0 && string(got[0]) == "original-content" {
		t.Fatal("checksum did not detect a corrupted payload")
	}
	if len(got) != 0 {
		t.Fatalf("got %d records after corruption at record 0, want 0", len(got))
	}
}

func TestRejectsOversizedRecord(t *testing.T) {
	w := tempWAL(t)
	if err := w.Append(make([]byte, maxRecordSize+1)); err == nil {
		t.Fatal("expected oversized record to be rejected")
	}
}

// --- Concurrent flow ------------------------------------------------------

func TestConcurrentAppends(t *testing.T) {
	w := tempWAL(t)

	const writers = 8
	const each = 25
	done := make(chan struct{})

	for g := range writers {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range each {
				if err := w.Append([]byte(fmt.Sprintf("w%d-%d", g, i))); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(g)
	}
	for range writers {
		<-done
	}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != writers*each {
		t.Fatalf("got %d records, want %d — concurrent appends interleaved badly",
			len(got), writers*each)
	}
	// Every record must be intact: no torn or interleaved payloads.
	seen := make(map[string]bool)
	for _, r := range got {
		if seen[string(r)] {
			t.Fatalf("duplicate record %q", r)
		}
		seen[string(r)] = true
	}
}

func TestTruncateClearsEverything(t *testing.T) {
	w := tempWAL(t)
	w.Append([]byte("a"))
	w.Append([]byte("b"))

	if err := w.Truncate(); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	got, _ := w.ReadAll()
	if len(got) != 0 {
		t.Fatalf("got %d records after truncate, want 0", len(got))
	}
}
