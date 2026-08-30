package hlc

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Hybrid Logical Clock tests (Phase 3).
//
// Per RULES.md rule 3: normal (time advances), failure (the physical clock goes
// BACKWARDS — the case a real clock cannot be made to produce on demand, and the
// one that matters most), concurrent (many goroutines stamping at once), and the
// causality property the whole mechanism exists for.
//
// The property under test throughout: if A causally precedes B, then
// hlc(A) < hlc(B). For a ledger that is not an abstraction — it is whether the
// audit trail can contradict cause and effect.

// fakeClock is a controllable physical time source.
type fakeClock struct{ ms atomic.Uint64 }

func (f *fakeClock) now() uint64  { return f.ms.Load() }
func (f *fakeClock) set(v uint64) { f.ms.Store(v) }

func newTestClock(start uint64) (*Clock, *fakeClock) {
	f := &fakeClock{}
	f.set(start)
	return NewWithPhysical(f.now), f
}

// --- ordering -------------------------------------------------------------

func TestTimestampOrdering(t *testing.T) {
	cases := []struct {
		a, b Timestamp
		less bool
	}{
		{Timestamp{10, 0}, Timestamp{11, 0}, true},
		{Timestamp{11, 0}, Timestamp{10, 0}, false},
		{Timestamp{10, 1}, Timestamp{10, 2}, true},
		{Timestamp{10, 2}, Timestamp{10, 1}, false},
		{Timestamp{10, 5}, Timestamp{10, 5}, false},
		// The logical counter must not outrank the wall component: a large counter
		// at an earlier millisecond is still earlier.
		{Timestamp{10, 999}, Timestamp{11, 0}, true},
	}
	for _, c := range cases {
		if got := c.a.Less(c.b); got != c.less {
			t.Fatalf("%v.Less(%v) = %v, want %v", c.a, c.b, got, c.less)
		}
	}
}

// The zero value must be distinguishable from a real timestamp, or records
// written before HLC existed all sort together at the beginning of time as
// though they were simultaneous.
func TestZeroTimestampIsDistinguishable(t *testing.T) {
	var zero Timestamp
	if !zero.IsZero() {
		t.Fatal("the zero value does not report IsZero")
	}
	if (Timestamp{Wall: 1}).IsZero() {
		t.Fatal("a real timestamp reported IsZero")
	}
	if got := zero.String(); got != "hlc:unset" {
		t.Fatalf("zero renders as %q; an unset timestamp must not look like a real one", got)
	}
}

// --- local events ---------------------------------------------------------

// Successive calls must be strictly increasing, always.
func TestNowIsStrictlyMonotonic(t *testing.T) {
	c, phys := newTestClock(1000)

	var prev Timestamp
	for i := range 100 {
		// Advance the physical clock only occasionally, so most calls land in the
		// same millisecond and must be separated by the counter.
		if i%10 == 0 {
			phys.set(phys.now() + 1)
		}
		ts := c.Now()
		if i > 0 && !prev.Less(ts) {
			t.Fatalf("call %d returned %v, not after %v — two events cannot share a "+
				"timestamp or the ledger cannot order them", i, ts, prev)
		}
		prev = ts
	}
}

// Within one millisecond the counter separates events.
func TestCounterSeparatesEventsInTheSameMillisecond(t *testing.T) {
	c, _ := newTestClock(5000)

	a, b, d := c.Now(), c.Now(), c.Now()
	if a.Wall != 5000 || b.Wall != 5000 || d.Wall != 5000 {
		t.Fatalf("wall components drifted within one millisecond: %v %v %v", a, b, d)
	}
	if !(a.Logical < b.Logical && b.Logical < d.Logical) {
		t.Fatalf("counters did not increase: %v %v %v", a, b, d)
	}
}

// When the physical clock advances, the counter resets — which is what keeps the
// timestamp anchored to real time instead of drifting.
func TestCounterResetsWhenPhysicalClockAdvances(t *testing.T) {
	c, phys := newTestClock(1000)

	c.Now()
	c.Now()
	if got := c.Last().Logical; got == 0 {
		t.Fatal("setup: the counter should have advanced")
	}

	phys.set(1001)
	ts := c.Now()
	if ts.Wall != 1001 {
		t.Fatalf("wall = %d, want 1001", ts.Wall)
	}
	if ts.Logical != 0 {
		t.Fatalf("counter = %d after the physical clock advanced, want 0 — without "+
			"the reset the counter grows without bound and the timestamp drifts away "+
			"from real time", ts.Logical)
	}
}

// THE failure case: the physical clock goes backwards.
//
// NTP correction, VM migration, a manual clock change. A naive implementation
// stamps a later event with an earlier time, and the audit trail then contradicts
// causality — a debit appearing to happen after the credit it caused.
func TestClockGoingBackwardsNeverProducesAnEarlierTimestamp(t *testing.T) {
	c, phys := newTestClock(10_000)

	before := c.Now()

	// The clock jumps back a full second.
	phys.set(9_000)

	for i := range 20 {
		ts := c.Now()
		if !before.Less(ts) {
			t.Fatalf("after the physical clock jumped backwards, call %d returned %v "+
				"which is not after %v. A ledger whose timestamps go backwards has an "+
				"audit trail that contradicts cause and effect", i, ts, before)
		}
		before = ts
	}

	// The wall component must hold at the old reading rather than following the
	// clock down.
	if got := c.Last().Wall; got != 10_000 {
		t.Fatalf("wall = %d after a backwards jump, want it held at 10000", got)
	}
}

// Once the physical clock catches up past the held reading, it takes over again.
func TestClockRecoversAfterBackwardsJump(t *testing.T) {
	c, phys := newTestClock(10_000)
	c.Now()

	phys.set(9_000)
	c.Now()

	// Real time moves past the held value.
	phys.set(10_500)
	ts := c.Now()
	if ts.Wall != 10_500 {
		t.Fatalf("wall = %d once the physical clock overtook the held reading, want "+
			"10500 — the clock must re-anchor to real time rather than staying on the "+
			"counter forever", ts.Wall)
	}
	if ts.Logical != 0 {
		t.Fatalf("counter = %d, want 0 after re-anchoring", ts.Logical)
	}
}

// --- receiving remote timestamps -----------------------------------------

// The property the whole mechanism exists for: after receiving a message stamped
// at T, this node's clock reads strictly greater than T.
func TestUpdateExceedsAReceivedTimestamp(t *testing.T) {
	c, _ := newTestClock(1000)

	// A message from a node whose clock is well ahead.
	remote := Timestamp{Wall: 5000, Logical: 7}
	got := c.Update(remote)

	if !remote.Less(got) {
		t.Fatalf("after receiving %v the clock reads %v, which is not after it. "+
			"Anything this node does next would be ordered BEFORE the event that "+
			"caused it", remote, got)
	}
	if got.Wall != 5000 {
		t.Fatalf("wall = %d, want 5000 — the remote reading must be adopted when it leads",
			got.Wall)
	}
}

// A remote timestamp in the same millisecond must still be exceeded, via the
// counter.
func TestUpdateBreaksTiesInTheSameMillisecond(t *testing.T) {
	c, _ := newTestClock(3000)
	local := c.Now() // {3000, 0}

	remote := Timestamp{Wall: 3000, Logical: 5}
	got := c.Update(remote)

	if !remote.Less(got) {
		t.Fatalf("clock reads %v after receiving %v; a tie must be broken upward",
			got, remote)
	}
	if !local.Less(got) {
		t.Fatalf("clock reads %v, which is not after its own earlier reading %v",
			got, local)
	}
	if got.Logical != 6 {
		t.Fatalf("counter = %d, want 6 (one past the remote's 5)", got.Logical)
	}
}

// A stale remote timestamp must not drag the clock backwards.
func TestUpdateIgnoresAStaleRemoteTimestamp(t *testing.T) {
	c, _ := newTestClock(8000)
	local := c.Now()

	got := c.Update(Timestamp{Wall: 100, Logical: 0})

	if got.Wall != 8000 {
		t.Fatalf("wall = %d after receiving a very old timestamp, want 8000 — a slow "+
			"peer must not be able to rewind this node's clock", got.Wall)
	}
	if !local.Less(got) {
		t.Fatalf("clock reads %v, not after its own earlier %v", got, local)
	}
}

// Causality across a chain of nodes: A -> B -> C must produce increasing
// timestamps even though each node's physical clock differs.
func TestCausalityHoldsAcrossAChainOfNodes(t *testing.T) {
	// Three nodes with deliberately skewed clocks. B is BEHIND A, which is the
	// case that would break under plain wall-clock timestamps.
	a, _ := newTestClock(10_000)
	b, _ := newTestClock(9_000)
	d, _ := newTestClock(9_500)

	// A does something and sends it to B.
	tA := a.Now()
	tB := b.Update(tA)

	// B does something in response and sends it to C.
	tB2 := b.Now()
	tC := d.Update(tB2)

	if !tA.Less(tB) {
		t.Fatalf("B's timestamp %v is not after A's %v, although B's event was "+
			"caused by A's. B's clock is behind A's, which is exactly why wall-clock "+
			"timestamps cannot be used for ordering", tB, tA)
	}
	if !tB.Less(tB2) {
		t.Fatalf("B's second event %v is not after its first %v", tB2, tB)
	}
	if !tB2.Less(tC) {
		t.Fatalf("C's timestamp %v is not after B's %v", tC, tB2)
	}
	// And transitively, which is the property an audit trail depends on.
	if !tA.Less(tC) {
		t.Fatalf("causality is not transitive: A=%v, C=%v", tA, tC)
	}
}

// --- concurrency ----------------------------------------------------------

// A leader stamps commands from several client goroutines at once. No two may
// receive the same timestamp, or the ledger cannot order them.
func TestConcurrentStampsAreAllDistinct(t *testing.T) {
	c, _ := newTestClock(1000)

	const goroutines, each = 8, 200
	var mu sync.Mutex
	seen := make(map[Timestamp]bool, goroutines*each)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				ts := c.Now()
				mu.Lock()
				if seen[ts] {
					mu.Unlock()
					t.Errorf("duplicate timestamp %v handed to two callers", ts)
					return
				}
				seen[ts] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*each {
		t.Fatalf("%d distinct timestamps from %d calls", len(seen), goroutines*each)
	}
}

// Mixed local and remote activity under concurrency must never produce a
// timestamp that goes backwards.
func TestConcurrentNowAndUpdateStayMonotonic(t *testing.T) {
	c, phys := newTestClock(1000)

	var wg sync.WaitGroup
	var highest atomic.Uint64 // packs wall<<20 | logical, for a cheap max

	pack := func(ts Timestamp) uint64 { return ts.Wall<<20 | uint64(ts.Logical) }

	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 100 {
				var ts Timestamp
				if j%3 == 0 {
					ts = c.Update(Timestamp{Wall: 1000 + uint64(j), Logical: uint32(i)})
				} else {
					ts = c.Now()
				}
				for {
					cur := highest.Load()
					if p := pack(ts); p <= cur {
						break
					} else if highest.CompareAndSwap(cur, p) {
						break
					}
				}
			}
			phys.set(phys.now() + 1)
		}(i)
	}
	wg.Wait()

	// Every subsequent reading must exceed everything observed during the run.
	final := c.Now()
	if pack(final) <= highest.Load() {
		t.Fatalf("the clock went backwards under concurrent Now/Update: final %v "+
			"does not exceed the highest observed reading", final)
	}
}
