package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/homura/core-bank/ledger"
)

// Shard state-machine snapshots, for Raft log compaction (§7).
//
// This is the most dangerous snapshot in the system, and the reason G3's design
// named it as the first thing to test.
//
// A shard replica's state is its ledger PLUS its 2PC transaction records. The
// records are what make a participant's YES vote survive a crash: PhasePrepared
// means this shard promised, unretractably, that a transfer may commit, and it is
// holding the customer's funds against that promise. That promise is derived from
// the log — which is exactly why compaction can destroy it. Discard the log
// prefix without capturing txs, and a restarted node comes back having forgotten
// a commitment it can never legitimately withdraw, with the reserved money
// spendable again.
//
// So: everything in Machine that is derived from the log goes into the snapshot.
//
//   - the ledger state (balances, idempotency, and reserves — see ledger/snapshot.go)
//   - txs      — every 2PC record, including its phase, decision, participants,
//                and named coordinator. Recovery needs all of it: an in-doubt
//                participant must know WHICH group to ask for the outcome.
//   - results  — single-shard idempotency results, so a retry after compaction
//                still returns the original answer instead of re-applying.
//
// byIndex is deliberately NOT included. It maps a Raft log index to the result
// that entry produced, so a proposer can read its own entry's outcome. Those
// indices are precisely the ones being discarded, and no proposer can still be
// waiting on an entry that was applied long enough ago to be compacted — the
// waiter would have timed out and reported Indeterminate. Carrying them forward
// would grow without bound for no reader.

// Snapshot implements raft.Snapshotter.
func (m *Machine) Snapshot() ([]byte, error) {
	ledgerData, err := m.State.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("shard: snapshot ledger: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var b bytes.Buffer

	// The ledger snapshot goes in length-prefixed, so the two encodings stay
	// independent and neither has to know the other's layout.
	putU64(&b, uint64(len(ledgerData)))
	b.Write(ledgerData)

	// txs, in sorted order so the encoding is deterministic across nodes.
	putU64(&b, uint64(len(m.txs)))
	for _, id := range sortedTxIDs(m.txs) {
		rec := m.txs[id]
		putStr(&b, string(rec.ID))
		putU64(&b, uint64(rec.Phase))
		putBool(&b, rec.Debit)
		putBool(&b, rec.Decided)
		putBool(&b, rec.Commit)
		putStr(&b, string(rec.Coordinator))

		putU64(&b, uint64(len(rec.Participants)))
		for _, p := range rec.Participants {
			putStr(&b, string(p))
		}

		// The underlying ledger command, so a recovered participant can still apply
		// the outcome: committing a credit needs the account and amount, which live
		// nowhere else once the log entry is gone.
		putU64(&b, uint64(rec.Cmd.Op))
		putStr(&b, rec.Cmd.IdempotencyKey)
		putStr(&b, string(rec.Cmd.From))
		putStr(&b, string(rec.Cmd.To))
		putU64(&b, uint64(rec.Cmd.Amount))
	}

	// results: single-shard idempotency
	putU64(&b, uint64(len(m.results)))
	for _, k := range sortedResultKeys(m.results) {
		r := m.results[k]
		putStr(&b, k)
		putBool(&b, r.OK)
		putStr(&b, r.Err)
		putU64(&b, uint64(r.Balance))
	}

	return b.Bytes(), nil
}

// RestoreSnapshot implements raft.Snapshotter.
func (m *Machine) RestoreSnapshot(data []byte) error {
	r := &rd{buf: data}

	ledgerLen := r.u64()
	ledgerData := r.bytes(ledgerLen)

	txs := make(map[TxID]*TxRecord)
	for n := r.u64(); n > 0; n-- {
		rec := &TxRecord{
			ID:          TxID(r.str()),
			Phase:       Phase(r.u64()),
			Debit:       r.b(),
			Decided:     r.b(),
			Commit:      r.b(),
			Coordinator: ID(r.str()),
		}
		for p := r.u64(); p > 0; p-- {
			rec.Participants = append(rec.Participants, ID(r.str()))
		}
		rec.Cmd = ledger.Command{
			Op:             ledger.Op(r.u64()),
			IdempotencyKey: r.str(),
			From:           ledger.AccountID(r.str()),
			To:             ledger.AccountID(r.str()),
			Amount:         ledger.Money(r.u64()),
		}
		if r.err == nil {
			txs[rec.ID] = rec
		}
	}

	results := make(map[string]ledger.Result)
	for n := r.u64(); n > 0; n-- {
		k := r.str()
		results[k] = ledger.Result{OK: r.b(), Err: r.str(), Balance: ledger.Money(r.u64())}
	}

	// Decode fully before touching anything. A half-restored shard machine —
	// holding one snapshot's promises against another's balances — is worse than
	// one that refused the snapshot outright.
	if r.err != nil {
		return fmt.Errorf("shard: restore snapshot: %w", r.err)
	}

	if err := m.State.RestoreSnapshot(ledgerData); err != nil {
		return fmt.Errorf("shard: restore ledger: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs = txs
	m.results = results
	// byIndex is intentionally reset: its keys are log indices from before the
	// snapshot, which no longer exist.
	m.byIndex = make(map[uint64]ledger.Result)
	return nil
}

// --- encoding helpers -----------------------------------------------------

func putU64(b *bytes.Buffer, v uint64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], v)
	b.Write(scratch[:])
}

func putStr(b *bytes.Buffer, s string) {
	putU64(b, uint64(len(s)))
	b.WriteString(s)
}

func putBool(b *bytes.Buffer, v bool) {
	if v {
		b.WriteByte(1)
		return
	}
	b.WriteByte(0)
}

// rd decodes a snapshot, latching the first error so a truncated record can
// never yield a value that was silently never read.
type rd struct {
	buf []byte
	pos int
	err error
}

func (r *rd) u64() uint64 {
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

// bytes reads n bytes, validating the declared length against what is actually
// present before using it as a bound.
func (r *rd) bytes(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if uint64(len(r.buf)-r.pos) < n {
		r.err = fmt.Errorf("truncated blob: declares %d bytes, %d remain", n, len(r.buf)-r.pos)
		return nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out
}

func (r *rd) str() string {
	n := r.u64()
	if r.err != nil {
		return ""
	}
	if uint64(len(r.buf)-r.pos) < n {
		r.err = fmt.Errorf("truncated string: declares %d bytes, %d remain", n, len(r.buf)-r.pos)
		return ""
	}
	v := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return v
}

func (r *rd) b() bool {
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

func sortedTxIDs(m map[TxID]*TxRecord) []TxID {
	out := make([]TxID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedResultKeys(m map[string]ledger.Result) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
