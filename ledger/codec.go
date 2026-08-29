package ledger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
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

	return Command{
		Op:             Op(op),
		IdempotencyKey: key,
		From:           AccountID(from),
		To:             AccountID(to),
		Amount:         Money(binary.LittleEndian.Uint64(amt[:])),
	}, nil
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
}

// NewMachine wraps a ledger state.
func NewMachine(s *State) *Machine {
	return &Machine{State: s, results: make(map[string]Result)}
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
	m.mu.Unlock()
	return res
}

// Result returns the recorded result for an idempotency key.
func (m *Machine) Result(key string) (Result, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.results[key]
	return r, ok
}
