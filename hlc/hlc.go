// Package hlc implements Hybrid Logical Clocks (Phase 3).
//
// The problem it solves for this project: each shard's ledger assigns
// Transaction.Seq, a per-shard monotonic counter. That orders events perfectly
// WITHIN one shard and says nothing across shards. Two transactions with Seq=7 on
// shard-0 and Seq=7 on shard-1 have no relationship at all, so the system cannot
// answer questions a bank actually asks: show a customer's transactions in order
// when their accounts span shards; what did the books look like at 14:32; did the
// debit leg happen before the credit leg it caused.
//
// That last one is not hypothetical here. The two legs of a cross-shard transfer
// live in DIFFERENT RAFT LOGS by construction, which is why Phase 2 had to resolve
// double-entry as a global invariant rather than a per-shard one.
//
// Why not wall-clock time: clocks on different machines disagree, and a single
// machine's clock can jump backwards (NTP correction, VM migration). A debit
// stamped at one shard could then carry a LATER timestamp than the credit it
// caused, reversing cause and effect in the audit trail. And this project's
// determinism rule forbids reading the clock at apply time at all — two replicas
// applying the same log entry would produce different state.
//
// An HLC timestamp is a pair (l, c): l a physical time in milliseconds, c a
// logical counter breaking ties within the same millisecond. Two properties
// follow, and both are load-bearing:
//
//   - If A causally precedes B then hlc(A) < hlc(B). Lamport's happened-before,
//     preserved exactly.
//   - l stays within clock skew of physical time, so a timestamp is still usable
//     as an approximate real-world reading. A pure logical clock drifts
//     arbitrarily far and cannot answer "as of 14:32".
//
// What this deliberately does NOT provide: external consistency. A transaction
// that finished before another began in real time can still receive a larger
// timestamp, if the two were never causally connected. Spanner solves that with
// TrueTime — GPS and atomic clocks giving bounded uncertainty, plus a commit wait
// that stalls until the uncertainty passes. That needs hardware this project does
// not have, and would add latency to every commit to fix a problem a
// single-machine system cannot exhibit. CockroachDB made the same trade.
//
// Theory in learn/READING_LIST.md §19.
package hlc

import (
	"fmt"
	"sync"
	"time"
)

// Timestamp is a hybrid logical clock reading.
//
// Wall is milliseconds since the Unix epoch; Logical breaks ties within the same
// millisecond. Compared lexicographically.
type Timestamp struct {
	Wall    uint64
	Logical uint32
}

// Less reports whether t happened before other in HLC order.
func (t Timestamp) Less(other Timestamp) bool {
	if t.Wall != other.Wall {
		return t.Wall < other.Wall
	}
	return t.Logical < other.Logical
}

// Equal reports exact equality.
func (t Timestamp) Equal(other Timestamp) bool {
	return t.Wall == other.Wall && t.Logical == other.Logical
}

// After reports whether t happened after other.
func (t Timestamp) After(other Timestamp) bool { return other.Less(t) }

// IsZero reports whether this is the zero timestamp, which no clock ever
// produces: Now always returns Wall > 0 for any sane physical clock.
//
// Distinguishing "unset" from "very early" matters when reading records written
// before HLC existed — they carry the zero value, and treating that as a real
// timestamp would sort them all together at the beginning of time as though they
// were simultaneous.
func (t Timestamp) IsZero() bool { return t.Wall == 0 && t.Logical == 0 }

// Time converts the physical component back to a time.Time.
//
// Approximate by construction: the logical counter is discarded, and the wall
// component may have been nudged ahead of real time by causality. Good enough for
// display and for "as of" queries, never for measuring durations.
func (t Timestamp) Time() time.Time {
	return time.UnixMilli(int64(t.Wall))
}

// String renders a timestamp for logs and diagnostics.
func (t Timestamp) String() string {
	if t.IsZero() {
		return "hlc:unset"
	}
	return fmt.Sprintf("%s+%d", t.Time().UTC().Format("2006-01-02T15:04:05.000Z"), t.Logical)
}

// Clock is a hybrid logical clock.
//
// Safe for concurrent use: a shard's leader stamps commands from several client
// goroutines at once, and two callers must never receive the same timestamp.
type Clock struct {
	mu   sync.Mutex
	last Timestamp

	// physical reads the local wall clock, in milliseconds since the epoch.
	// Injectable so tests can drive a clock backwards, which is the case that
	// matters most and cannot be arranged with a real one.
	physical func() uint64
}

// New returns a clock reading the system wall clock.
func New() *Clock {
	return &Clock{physical: func() uint64 { return uint64(time.Now().UnixMilli()) }}
}

// NewWithPhysical returns a clock reading a supplied physical time source.
func NewWithPhysical(physical func() uint64) *Clock {
	return &Clock{physical: physical}
}

// Now returns the next timestamp for a local event, and is what a leader calls
// before appending a command to the log.
//
// Never returns the same timestamp twice, and never goes backwards — including
// when the physical clock does. A clock that jumped backwards would otherwise
// stamp a later event with an earlier time, which for a ledger means an audit
// trail that contradicts causality.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.physical()

	if physical > c.last.Wall {
		// The physical clock advanced: take it and reset the counter. This is what
		// keeps the timestamp anchored to real time rather than drifting.
		c.last = Timestamp{Wall: physical, Logical: 0}
		return c.last
	}

	// The physical clock did not advance — either two events in the same
	// millisecond, or the clock went BACKWARDS. Both are handled the same way:
	// keep the last wall reading and increment the counter, so time never moves
	// backwards from an observer's point of view.
	c.last.Logical++
	return c.last
}

// Update merges a timestamp received from another node and returns the local
// clock's new reading.
//
// Called when a message carrying an HLC timestamp arrives — a 2PC prepare, a
// decision, an outcome. This is the step that makes causality hold ACROSS shards:
// after receiving a message stamped at time T, this node's clock reads strictly
// greater than T, so anything it does next is correctly ordered after the event
// that caused it.
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.physical()

	// The new wall reading is the largest of the three candidates. Taking the
	// remote value when it leads is precisely what propagates causality; taking
	// the physical clock when IT leads is what stops the timestamp drifting away
	// from real time.
	wall := physical
	if c.last.Wall > wall {
		wall = c.last.Wall
	}
	if remote.Wall > wall {
		wall = remote.Wall
	}

	switch {
	case wall == c.last.Wall && wall == remote.Wall:
		// Tied with both: exceed the larger of the two counters.
		if remote.Logical > c.last.Logical {
			c.last.Logical = remote.Logical
		}
		c.last.Logical++

	case wall == c.last.Wall:
		// Tied with our own last reading only.
		c.last.Logical++

	case wall == remote.Wall:
		// Tied with the remote reading only: exceed its counter.
		c.last = Timestamp{Wall: wall, Logical: remote.Logical + 1}

	default:
		// The physical clock leads both: reset the counter.
		c.last = Timestamp{Wall: wall, Logical: 0}
	}

	return c.last
}

// Last returns the clock's current reading without advancing it.
func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}
