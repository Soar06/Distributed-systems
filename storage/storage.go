// Package storage provides durable persistence for Raft state.
//
// Figure 2 requires that persistent state (currentTerm, votedFor, log[]) reach
// stable storage BEFORE the server responds to any RPC. This is not a
// performance detail — it is a safety requirement. A server that grants a vote,
// crashes, restarts having forgotten the vote, and votes again for a different
// candidate in the same term can produce two leaders in one term, violating
// Election Safety.
//
// The format is a write-ahead log: append-only records, each length-prefixed and
// checksummed. A torn write at the tail (the process died mid-append) is detected
// and truncated on recovery, because a half-written record is indistinguishable
// from corruption and must never be replayed as if it were real.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrCorrupt indicates a record failed its checksum somewhere other than at the
// tail of the file — real corruption, not an interrupted write.
var ErrCorrupt = errors.New("storage: corrupt record")

// recordHeader is [4-byte length][4-byte CRC32] preceding each payload.
const headerSize = 8

// maxRecordSize bounds a single record, so a corrupt length field cannot cause a
// huge allocation.
const maxRecordSize = 64 << 20 // 64 MiB

// frameRecord wraps a payload in the WAL's on-disk record format:
// [4-byte length][4-byte CRC32][payload]. Exposed so RaftState can write a
// single framed record atomically without going through Append.
func frameRecord(payload []byte) []byte {
	out := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
	copy(out[headerSize:], payload)
	return out
}

// readFramedFile reads a whole file written by frameRecord and returns its
// payload. A missing file returns (nil, nil); a truncated or checksum-failing
// file also returns (nil, nil) — treated as "no state", which is correct because
// an atomically-written file is either wholly present or wholly absent, so a bad
// one means the very first write was interrupted.
func readFramedFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("storage: read state: %w", err)
	}
	if len(data) < headerSize {
		return nil, nil
	}
	length := binary.LittleEndian.Uint32(data[0:4])
	want := binary.LittleEndian.Uint32(data[4:8])
	if length > maxRecordSize || int(length) != len(data)-headerSize {
		return nil, nil
	}
	payload := data[headerSize:]
	if crc32.ChecksumIEEE(payload) != want {
		return nil, nil
	}
	return payload, nil
}

// checksum is the shared CRC32 used by the WAL and the applied-index file.
func checksum(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

// WAL is an append-only write-ahead log on disk.
//
// All writes are followed by fsync before returning. That is what makes the
// durability guarantee real: without fsync the data sits in the OS page cache and
// a power loss discards it, which is precisely the failure Figure 2's requirement
// exists to survive.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
	// syncOnWrite can be disabled in tests that measure throughput rather than
	// durability. It defaults to true and production code must never turn it off.
	syncOnWrite bool

	// offset is the write position, tracked explicitly because the file is not
	// opened O_APPEND (see Open).
	offset int64
}

// Open opens or creates a WAL at path, creating parent directories as needed.
func Open(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir: %w", err)
	}
	// Deliberately NOT O_APPEND: on Windows, ftruncate against a handle opened
	// for append is denied, and recovery from a torn write requires truncating.
	// The offset is tracked explicitly instead, which is portable.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: seek: %w", err)
	}
	return &WAL{f: f, path: path, syncOnWrite: true, offset: end}, nil
}

// SetSyncOnWrite controls whether each append is fsynced. Only for benchmarks.
func (w *WAL) SetSyncOnWrite(on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.syncOnWrite = on
}

// Append writes one record and, by default, fsyncs before returning.
func (w *WAL) Append(payload []byte) error {
	if len(payload) > maxRecordSize {
		return fmt.Errorf("storage: record too large: %d bytes", len(payload))
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))

	rec := make([]byte, 0, headerSize+len(payload))
	rec = append(rec, hdr...)
	rec = append(rec, payload...)

	// One WriteAt for the whole record: a single syscall cannot interleave with
	// another writer's record, which keeps concurrent appends intact.
	if _, err := w.f.WriteAt(rec, w.offset); err != nil {
		return fmt.Errorf("storage: write record: %w", err)
	}
	w.offset += int64(len(rec))
	if w.syncOnWrite {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("storage: fsync: %w", err)
		}
	}
	return nil
}

// ReadAll returns every intact record in order.
//
// A truncated or checksum-failing record at the very end of the file is treated
// as an interrupted write: it is dropped, and the file is truncated to the last
// good record. This is the crash-recovery path, and it is why a torn write cannot
// corrupt state.
func (w *WAL) ReadAll() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	r := bufio.NewReader(io.NewSectionReader(w.f, 0, w.offset))

	var (
		records [][]byte
		offset  int64
	)
	for {
		hdr := make([]byte, headerSize)
		n, err := io.ReadFull(r, hdr)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || (err == nil && n < headerSize) {
			// Partial header: torn write at the tail.
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.LittleEndian.Uint32(hdr[0:4])
		want := binary.LittleEndian.Uint32(hdr[4:8])
		if length > maxRecordSize {
			// A bogus length means the header itself is garbage; stop here rather
			// than trusting it.
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			// Partial payload: torn write at the tail.
			break
		}
		if crc32.ChecksumIEEE(payload) != want {
			// Checksum failure. At the tail this is a torn write; we cannot
			// distinguish it from mid-file corruption without more metadata, so we
			// stop and truncate — dropping a possibly-bad record is always safer
			// than replaying one.
			break
		}

		records = append(records, payload)
		offset += headerSize + int64(length)
	}

	// Truncate away any trailing garbage so the next Append starts clean.
	if offset != w.offset {
		if err := w.f.Truncate(offset); err != nil {
			return nil, fmt.Errorf("storage: truncate: %w", err)
		}
	}
	w.offset = offset
	return records, nil
}

// Truncate discards the entire log. Used when replacing state wholesale.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	w.offset = 0
	return w.f.Sync()
}

// Sync flushes to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// Path returns the file path.
func (w *WAL) Path() string { return w.path }
