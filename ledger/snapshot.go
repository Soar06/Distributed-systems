package ledger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/homura/core-bank/hlc"
)

// State snapshots, for Raft log compaction (§7).
//
// The rule that makes this safe, and the reason this file is longer than it
// looks like it should be: a snapshot must capture EVERYTHING the state machine
// derived from the log, so that restoring it produces a state indistinguishable
// from replaying the whole log. Anything omitted is silently destroyed the moment
// the log prefix is discarded — and no amount of Raft correctness catches it,
// because the log is still perfect. The bug shows up as money.
//
// Every field of State is therefore included, and each one earns its place:
//
//   - balances     — the obvious one.
//   - applied      — idempotency results. Losing these makes a retried request
//                    execute a SECOND time, which is a double-spend.
//   - fingerprints — the request digest bound to each key. Losing these lets a
//                    key be reused for a different request, which is the bug that
//                    once returned one account's balance for another's withdrawal.
//   - reserves     — funds promised to in-flight cross-shard transactions.
//                    Losing these frees money already committed to a 2PC
//                    transaction, breaking the unretractable promise a YES vote
//                    makes. THIS is the field most likely to be forgotten, and
//                    the one that costs the most.
//   - history      — the append-only event-sourced record. Losing it does not
//                    change any balance, but it destroys the audit trail and
//                    makes VerifyDoubleEntry re-derive from nothing, so a
//                    compacted node could no longer prove its own books.
//   - seq          — the monotonic counter. Restarting it would make two
//                    different transactions share a sequence number.
//
// Determinism: maps are serialized in SORTED key order. Go's map iteration is
// randomized, so an unsorted encoding would produce different bytes on different
// nodes for identical state — which is harmless for correctness here but makes
// snapshots impossible to compare, and comparing them is exactly how a test
// proves compaction did not change anything.

// Snapshot implements raft.Snapshotter for the ledger state machine.
func (m *Machine) Snapshot() ([]byte, error) { return m.State.Snapshot() }

// RestoreSnapshot implements raft.Snapshotter.
func (m *Machine) RestoreSnapshot(data []byte) error { return m.State.RestoreSnapshot(data) }

// Snapshot serializes the entire ledger state.
func (s *State) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var b bytes.Buffer
	putUint64(&b, s.seq)

	// balances
	putUint64(&b, uint64(len(s.balances)))
	for _, id := range sortedAccounts(s.balances) {
		putStr(&b, string(id))
		putUint64(&b, uint64(s.balances[id]))
	}

	// applied: idempotency key -> result
	putUint64(&b, uint64(len(s.applied)))
	for _, k := range sortedKeys(s.applied) {
		r := s.applied[k]
		putStr(&b, k)
		putBool(&b, r.OK)
		putStr(&b, r.Err)
		putUint64(&b, uint64(r.Balance))
	}

	// fingerprints: idempotency key -> request digest
	putUint64(&b, uint64(len(s.fingerprints)))
	for _, k := range sortedKeys(s.fingerprints) {
		putStr(&b, k)
		putUint64(&b, s.fingerprints[k])
	}

	// reserves: txID -> held funds
	putUint64(&b, uint64(len(s.reserves)))
	for _, k := range sortedKeys(s.reserves) {
		r := s.reserves[k]
		putStr(&b, k)
		putStr(&b, r.TxID)
		putStr(&b, string(r.Account))
		putUint64(&b, uint64(r.Amount))
	}

	// history
	putUint64(&b, uint64(len(s.history)))
	for _, t := range s.history {
		putUint64(&b, t.Seq)
		putUint64(&b, uint64(t.Op))
		putStr(&b, t.IdempotencyKey)
		// The HLC timestamp travels in the snapshot too. Omitting it would make
		// compaction silently destroy cross-shard ordering — the exact class of bug
		// G3's design named: anything the snapshot fails to capture is gone the
		// moment the log prefix is discarded, and no Raft correctness catches it.
		putUint64(&b, t.Timestamp.Wall)
		putUint64(&b, uint64(t.Timestamp.Logical))
		putUint64(&b, uint64(len(t.Entries)))
		for _, e := range t.Entries {
			putStr(&b, string(e.Account))
			putUint64(&b, uint64(e.Amount))
		}
	}

	return b.Bytes(), nil
}

// RestoreSnapshot replaces the ledger's contents with a snapshot.
//
// Replaces rather than merges. A snapshot is a complete picture, and merging it
// into existing state would produce a ledger that matches no log position — the
// balances of one history with the reservations of another.
func (s *State) RestoreSnapshot(data []byte) error {
	r := &reader{buf: data}

	seq := r.uint64()

	balances := make(map[AccountID]Money)
	for n := r.uint64(); n > 0; n-- {
		id := AccountID(r.str())
		balances[id] = Money(r.uint64())
	}

	applied := make(map[string]Result)
	for n := r.uint64(); n > 0; n-- {
		k := r.str()
		applied[k] = Result{OK: r.bool(), Err: r.str(), Balance: Money(r.uint64())}
	}

	fingerprints := make(map[string]uint64)
	for n := r.uint64(); n > 0; n-- {
		k := r.str()
		fingerprints[k] = r.uint64()
	}

	reserves := make(map[string]Reserve)
	for n := r.uint64(); n > 0; n-- {
		k := r.str()
		reserves[k] = Reserve{
			TxID:    r.str(),
			Account: AccountID(r.str()),
			Amount:  Money(r.uint64()),
		}
	}

	var history []Transaction
	for n := r.uint64(); n > 0; n-- {
		t := Transaction{Seq: r.uint64(), Op: Op(r.uint64()), IdempotencyKey: r.str()}
		t.Timestamp = hlc.Timestamp{Wall: r.uint64(), Logical: uint32(r.uint64())}
		for m := r.uint64(); m > 0; m-- {
			t.Entries = append(t.Entries, Entry{
				Account: AccountID(r.str()),
				Amount:  Money(r.uint64()),
			})
		}
		history = append(history, t)
	}

	// Every field is decoded BEFORE anything is assigned. A truncated snapshot
	// must leave the ledger untouched rather than half-replaced: a state machine
	// holding one snapshot's balances and another's reserves is worse than one
	// that refused to restore.
	if r.err != nil {
		return fmt.Errorf("ledger: restore snapshot: %w", r.err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq = seq
	s.balances = balances
	s.applied = applied
	s.fingerprints = fingerprints
	s.reserves = reserves
	s.history = history
	return nil
}

// --- encoding helpers -----------------------------------------------------
//
// Fixed-width little-endian throughout, matching raft/persist.go's discipline, so
// decoding never depends on platform specifics. Money and Op are written as
// uint64 and converted back: both are integer types, and a signed Money round
// trips through uint64 unchanged in two's complement.

func putUint64(b *bytes.Buffer, v uint64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], v)
	b.Write(scratch[:])
}

func putStr(b *bytes.Buffer, s string) {
	putUint64(b, uint64(len(s)))
	b.WriteString(s)
}

func putBool(b *bytes.Buffer, v bool) {
	if v {
		b.WriteByte(1)
		return
	}
	b.WriteByte(0)
}

// reader decodes a snapshot, latching the first error.
//
// Latching rather than returning per-field errors keeps the decode readable, and
// — more importantly — makes it impossible to use a value that was never decoded:
// once err is set every subsequent read returns a zero value and the caller
// checks err once, before assigning anything.
type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) uint64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.pos+8 > len(r.buf) {
		r.err = fmt.Errorf("truncated: want 8 bytes at %d, have %d", r.pos, len(r.buf)-r.pos)
		return 0
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8])
	r.pos += 8
	return v
}

func (r *reader) str() string {
	n := r.uint64()
	if r.err != nil {
		return ""
	}
	// The declared length is validated against what is actually present before it
	// is used as a bound. A hostile or torn record must not be able to drive a
	// slice past the buffer.
	if uint64(len(r.buf)-r.pos) < n {
		r.err = fmt.Errorf("truncated string: declares %d bytes, %d remain", n, len(r.buf)-r.pos)
		return ""
	}
	v := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return v
}

func (r *reader) bool() bool {
	if r.err != nil {
		return false
	}
	if r.pos >= len(r.buf) {
		r.err = fmt.Errorf("truncated bool at %d", r.pos)
		return false
	}
	v := r.buf[r.pos] == 1
	r.pos++
	return v
}

func sortedAccounts(m map[AccountID]Money) []AccountID {
	out := make([]AccountID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
