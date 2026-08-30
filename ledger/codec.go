package ledger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/homura/core-bank/hlc"
)

// Encoding of Command to and from the opaque []byte that Raft replicates.
//
// A hand-rolled fixed-width encoding rather than JSON or gob, for one reason:
// determinism. The bytes in the log must decode to exactly the same command on
// every node and after every restart, and an encoding with map iteration or
// version-dependent field ordering is a determinism hazard inside a state
// machine.

var errBadEncoding = errors.New("ledger: malformed command encoding")

// Encode serializes a command.
//
// Layout: [1 op][2 keyLen][key][2 fromLen][from][2 toLen][to][8 amount]
func (c Command) Encode() []byte {
	var b bytes.Buffer
	b.WriteByte(byte(c.Op))
	writeStr(&b, c.IdempotencyKey)
	writeStr(&b, string(c.From))
	writeStr(&b, string(c.To))

	var amt [8]byte
	binary.LittleEndian.PutUint64(amt[:], uint64(c.Amount))
	b.Write(amt[:])

	// The HLC timestamp is APPENDED, after every pre-existing field.
	//
	// Deliberate: a decoder reading a record written before this field existed
	// simply finds no trailing bytes and leaves the zero value. Inserting it in
	// the middle would make every previously-written WAL record undecodable, which
	// for a bank means an unreadable audit trail — the log is the authoritative
	// history, and a format change that orphans it destroys the thing being
	// protected.
	var ts [12]byte
	binary.LittleEndian.PutUint64(ts[0:8], c.Timestamp.Wall)
	binary.LittleEndian.PutUint32(ts[8:12], c.Timestamp.Logical)
	b.Write(ts[:])

	return b.Bytes()
}

// Decode parses a command produced by Encode.
func Decode(data []byte) (Command, error) {
	r := bytes.NewReader(data)

	op, err := r.ReadByte()
	if err != nil {
		return Command{}, errBadEncoding
	}
	key, err := readStr(r)
	if err != nil {
		return Command{}, err
	}
	from, err := readStr(r)
	if err != nil {
		return Command{}, err
	}
	to, err := readStr(r)
	if err != nil {
		return Command{}, err
	}

	var amt [8]byte
	if _, err := r.Read(amt[:]); err != nil {
		return Command{}, errBadEncoding
	}

	cmd := Command{
		Op:             Op(op),
		IdempotencyKey: key,
		From:           AccountID(from),
		To:             AccountID(to),
		Amount:         Money(binary.LittleEndian.Uint64(amt[:])),
	}

	// The timestamp is optional on the wire: a record written before the field
	// existed ends here, and leaving the zero value is exactly right — it means
	// "unstamped", which is different from "stamped at time zero".
	var ts [12]byte
	if n, err := io.ReadFull(r, ts[:]); err == nil {
		cmd.Timestamp = hlc.Timestamp{
			Wall:    binary.LittleEndian.Uint64(ts[0:8]),
			Logical: binary.LittleEndian.Uint32(ts[8:12]),
		}
	} else if n != 0 {
		// Partially present: the record is truncated, not merely old. Refused
		// rather than silently accepted with a half-read timestamp.
		return Command{}, errBadEncoding
	}

	return cmd, nil
}

func writeStr(b *bytes.Buffer, s string) {
	var l [2]byte
	binary.LittleEndian.PutUint16(l[:], uint16(len(s)))
	b.Write(l[:])
	b.WriteString(s)
}

func readStr(r *bytes.Reader) (string, error) {
	var l [2]byte
	if _, err := r.Read(l[:]); err != nil {
		return "", errBadEncoding
	}
	n := binary.LittleEndian.Uint16(l[:])
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := r.Read(buf); err != nil {
		return "", errBadEncoding
	}
	return string(buf), nil
}

// Machine adapts State to raft.StateMachine, decoding each command before
// applying it.
//
// Kept separate from State so the ledger itself has no knowledge of Raft, and can
// be tested with no consensus involved at all.
type Machine struct {
	State *State

	// results records the result of every applied command, keyed by idempotency
	// key, so a client API can find out what its submission actually did once the
	// entry commits.
	mu      sync.Mutex
	results map[string]Result

	// fingerprints binds each idempotency key to the request that first used it,
	// so a replay with a different operation is rejected rather than answered
	// with the original request's result.
	fingerprints map[string]uint64
}

// NewMachine wraps a ledger state.
func NewMachine(s *State) *Machine {
	return &Machine{
		State:        s,
		results:      make(map[string]Result),
		fingerprints: make(map[string]uint64),
	}
}

// Apply implements raft.StateMachine.
//
// A command that fails to decode is skipped rather than panicking: a corrupt
// entry must not take down every node in the cluster simultaneously. It returns a
// failed Result so the behavior is still identical on every node — which is what
// determinism requires.
func (m *Machine) Apply(cmd []byte) any {
	c, err := Decode(cmd)
	if err != nil {
		return Result{Err: errBadEncoding.Error()}
	}
	res := m.State.Apply(c)

	m.mu.Lock()
	m.results[c.IdempotencyKey] = res
	m.fingerprints[c.IdempotencyKey] = c.fingerprint()
	m.mu.Unlock()
	return res
}

// Result returns the recorded result for an idempotency key.
//
// Deprecated: prefer ResultFor, which also verifies the key belongs to the same
// request. Returning a cached result without that check forges a success for a
// different operation.
func (m *Machine) Result(key string) (Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.results[key]
	return r, ok
}

// ResultFor returns the recorded result for a command's idempotency key, but
// only if the key was first used for THIS same request.
//
// Returns (result, true, nil) on a genuine retry, (_, false, nil) when the key
// is unknown, and (_, false, ErrIdempotencyConflict) when the key was used for a
// different request.
func (m *Machine) ResultFor(cmd Command) (Result, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.results[cmd.IdempotencyKey]
	if !ok {
		return Result{}, false, nil
	}
	if m.fingerprints[cmd.IdempotencyKey] != cmd.fingerprint() {
		return Result{}, false, ErrIdempotencyConflict
	}
	return r, true, nil
}
