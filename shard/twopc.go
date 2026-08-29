package shard

import (
	"fmt"

	"github.com/homura/core-bank/ledger"
)

// Two-Phase Commit over Raft.
//
// Implements the shape Spanner uses (OSDI 2012, quoted in learn/READING_LIST.md
// §12), substituting Raft for Paxos:
//
//   - "One of the participant groups is chosen as the coordinator" — there is no
//     external coordinator process. The debit shard's leader takes the role.
//   - Participants "log a prepare record through Paxos" — the vote is a replicated
//     log entry, durable before it is sent.
//   - The coordinator "logs a commit record through Paxos (or an abort...)".
//   - "Each participant leader logs the transaction's outcome through Paxos."
//
// Nothing about the 2PC state lives in memory. An in-memory coordinator could not
// survive the failure the protocol exists to handle, which would make the whole
// exercise a toy.

// TxID identifies a distributed transaction.
type TxID string

// Phase is a participant's or coordinator's state for one transaction.
type Phase uint8

const (
	// PhaseNone means the transaction is unknown here.
	PhaseNone Phase = iota

	// PhasePrepared means this participant has voted YES and reserved the funds.
	// This is an UNRETRACTABLE PROMISE: having voted yes, a participant may not
	// unilaterally abort, even across a crash. That is what makes the vote
	// meaningful, and also what makes 2PC blocking.
	PhasePrepared

	// PhaseCommitted / PhaseAborted are terminal.
	PhaseCommitted
	PhaseAborted
)

func (p Phase) String() string {
	switch p {
	case PhaseNone:
		return "None"
	case PhasePrepared:
		return "Prepared"
	case PhaseCommitted:
		return "Committed"
	case PhaseAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// Op is a 2PC log entry operation. Each of these becomes a Raft log entry in the
// relevant group — that is what makes the protocol crash-safe.
type Op uint8

const (
	// OpPrepare is logged by a participant when it votes YES and reserves funds.
	OpPrepare Op = iota + 1

	// OpDecision is logged by the COORDINATOR: the commit-or-abort decision for
	// the whole transaction. Once this entry commits in the coordinator's Raft
	// group, the transaction's outcome is decided and survives any crash.
	OpDecision

	// OpOutcome is logged by a participant when it applies the decision.
	OpOutcome

	// OpSingle is an ordinary single-shard operation, needing no 2PC at all.
	OpSingle
)

// Command is what gets replicated through Raft for sharded operations. It wraps
// either a plain ledger command or one step of the 2PC protocol.
type Command struct {
	Op Op

	// TxID identifies the distributed transaction (empty for OpSingle).
	TxID TxID

	// Ledger carries the underlying bank operation.
	Ledger ledger.Command

	// Commit is the decision, for OpDecision and OpOutcome.
	Commit bool

	// Role records whether this participant is the debit or credit side, which
	// determines what committing actually does to the local balances.
	Debit bool

	// Participants lists every shard in the transaction. The coordinator needs
	// this in its decision record so a replacement leader knows who to notify.
	Participants []ID

	// Coordinator names the shard acting as coordinator for this transaction.
	//
	// This is explicit rather than inferred from Participants[0]: positional
	// convention is not validated anywhere, and several code paths legitimately
	// build records with an empty Participants slice. Recovery must be able to ask
	// a NAMED group for the decision, never guess from slice order.
	Coordinator ID
}

// TxRecord is the durable state of one distributed transaction at one shard.
//
// This is derived by applying 2PC log entries in order, exactly like balances are
// derived from ledger entries. It is never mutated directly.
type TxRecord struct {
	ID           TxID
	Phase        Phase
	Cmd          ledger.Command
	Debit        bool
	Participants []ID
	Coordinator  ID

	// Decided records the coordinator's decision once known.
	Decided bool
	Commit  bool
}

// Encode serializes a sharded command for the Raft log.
//
// Layout: [1 op][2 txidLen][txid][1 commit][1 debit][2 nParts][parts...][ledger]
func (c Command) Encode() []byte {
	buf := []byte{byte(c.Op)}
	buf = appendStr(buf, string(c.TxID))

	var b byte
	if c.Commit {
		b = 1
	}
	buf = append(buf, b)

	b = 0
	if c.Debit {
		b = 1
	}
	buf = append(buf, b)

	buf = append(buf, byte(len(c.Participants)>>8), byte(len(c.Participants)))
	for _, p := range c.Participants {
		buf = appendStr(buf, string(p))
	}
	buf = appendStr(buf, string(c.Coordinator))

	return append(buf, c.Ledger.Encode()...)
}

// DecodeCommand parses a command produced by Encode.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < 1 {
		return Command{}, fmt.Errorf("shard: empty command")
	}
	c := Command{Op: Op(data[0])}
	pos := 1

	txid, n, err := readStr(data, pos)
	if err != nil {
		return Command{}, err
	}
	c.TxID, pos = TxID(txid), n

	if pos+2 > len(data) {
		return Command{}, fmt.Errorf("shard: truncated command")
	}
	c.Commit = data[pos] == 1
	c.Debit = data[pos+1] == 1
	pos += 2

	if pos+2 > len(data) {
		return Command{}, fmt.Errorf("shard: truncated participants")
	}
	nParts := int(data[pos])<<8 | int(data[pos+1])
	pos += 2

	for range nParts {
		p, n, err := readStr(data, pos)
		if err != nil {
			return Command{}, err
		}
		c.Participants = append(c.Participants, ID(p))
		pos = n
	}

	coord, pos, err := readStr(data, pos)
	if err != nil {
		return Command{}, err
	}
	c.Coordinator = ID(coord)

	lc, err := ledger.Decode(data[pos:])
	if err != nil {
		return Command{}, err
	}
	c.Ledger = lc
	return c, nil
}

func appendStr(buf []byte, s string) []byte {
	buf = append(buf, byte(len(s)>>8), byte(len(s)))
	return append(buf, s...)
}

func readStr(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", 0, fmt.Errorf("shard: truncated string header")
	}
	n := int(data[pos])<<8 | int(data[pos+1])
	pos += 2
	if pos+n > len(data) {
		return "", 0, fmt.Errorf("shard: truncated string body")
	}
	return string(data[pos : pos+n]), pos + n, nil
}
