package shard

import (
	"fmt"
	"testing"
)

// Ring tests per RULES.md rule 3. The properties that matter are the ones §11
// names: determinism, minimal remapping when the shard set changes, and even
// distribution via virtual nodes.

func keys(n int) []string {
	out := make([]string, n)
	for i := range n {
		out[i] = fmt.Sprintf("account-%d", i)
	}
	return out
}

func shards(n int) []ID {
	out := make([]ID, n)
	for i := range n {
		out[i] = ID(fmt.Sprintf("shard-%d", i))
	}
	return out
}

// --- Determinism ----------------------------------------------------------

// Placement MUST be a pure function of (key, ring config). Two nodes that
// disagreed about who owns an account would be a correctness bug: both could
// believe they are authoritative for the same money.
func TestLookupIsDeterministic(t *testing.T) {
	r1 := NewRing(shards(4), DefaultVNodes)
	r2 := NewRing(shards(4), DefaultVNodes)

	for _, k := range keys(500) {
		if a, b := r1.Lookup(k), r2.Lookup(k); a != b {
			t.Fatalf("two identical rings disagree on %q: %s vs %s", k, a, b)
		}
	}
}

// Ring construction must not depend on the order shards were listed — nodes may
// read their config in any order.
func TestRingIndependentOfShardOrder(t *testing.T) {
	forward := NewRing([]ID{"a", "b", "c", "d"}, DefaultVNodes)
	backward := NewRing([]ID{"d", "c", "b", "a"}, DefaultVNodes)

	for _, k := range keys(300) {
		if x, y := forward.Lookup(k), backward.Lookup(k); x != y {
			t.Fatalf("shard listing order changed placement of %q: %s vs %s", k, x, y)
		}
	}
}

func TestRepeatedLookupsAreStable(t *testing.T) {
	r := NewRing(shards(3), DefaultVNodes)
	first := r.Lookup("alice")
	for range 100 {
		if got := r.Lookup("alice"); got != first {
			t.Fatalf("lookup unstable: %s then %s", first, got)
		}
	}
}

// --- Minimal remapping ----------------------------------------------------

// The whole point of consistent hashing (§11): adding the nth shard should move
// only about 1/n of the keys, NOT nearly all of them as hash%N would.
func TestAddingShardMovesFewKeys(t *testing.T) {
	before := NewRing(shards(4), DefaultVNodes)
	after := NewRing(shards(5), DefaultVNodes)

	ks := keys(5000)
	moved := 0
	for _, k := range ks {
		if before.Lookup(k) != after.Lookup(k) {
			moved++
		}
	}

	frac := float64(moved) / float64(len(ks))
	t.Logf("adding a 5th shard moved %d/%d keys (%.1f%%)", moved, len(ks), frac*100)

	// Theory says ~1/5 = 20%. Allow generous slack for hash variance, but this
	// must be nowhere near the ~80% that modulo hashing would produce.
	if frac > 0.35 {
		t.Fatalf("moved %.1f%% of keys, want ~20%% — consistent hashing is not working", frac*100)
	}
	if frac < 0.05 {
		t.Fatalf("moved only %.1f%% of keys — suspiciously low, is the new shard being used?", frac*100)
	}
}

// The contrast that justifies the whole technique. Modulo hashing is measured
// here purely to show what we are avoiding.
func TestConsistentHashingBeatsModulo(t *testing.T) {
	ks := keys(5000)

	before := NewRing(shards(4), DefaultVNodes)
	after := NewRing(shards(5), DefaultVNodes)
	ringMoved := 0
	for _, k := range ks {
		if before.Lookup(k) != after.Lookup(k) {
			ringMoved++
		}
	}

	moduloMoved := 0
	for _, k := range ks {
		if hashKey(k)%4 != hashKey(k)%5 {
			moduloMoved++
		}
	}

	t.Logf("4→5 shards: consistent hashing moved %d keys, modulo would move %d",
		ringMoved, moduloMoved)

	if ringMoved >= moduloMoved {
		t.Fatalf("consistent hashing moved %d keys vs modulo's %d — no better than modulo",
			ringMoved, moduloMoved)
	}
}

// --- Virtual nodes / distribution ----------------------------------------

// Virtual nodes exist to fix skew. Without enough of them a few shards own wildly
// unequal slices of the ring.
func TestVirtualNodesBalanceDistribution(t *testing.T) {
	r := NewRing(shards(4), DefaultVNodes)
	dist := r.Distribution(keys(10000))

	ideal := 10000 / 4
	for sid, count := range dist {
		ratio := float64(count) / float64(ideal)
		t.Logf("%s: %d keys (%.2fx ideal)", sid, count, ratio)
		if ratio < 0.6 || ratio > 1.4 {
			t.Fatalf("%s got %d keys, %.2fx the ideal %d — distribution too skewed",
				sid, count, ratio, ideal)
		}
	}
}

// Demonstrates WHY virtual nodes are needed: with one point per shard the ring
// divides very unevenly.
func TestFewVirtualNodesAreSkewed(t *testing.T) {
	few := NewRing(shards(4), 1)
	many := NewRing(shards(4), DefaultVNodes)
	ks := keys(10000)

	spread := func(d map[ID]int) float64 {
		lo, hi := 1<<30, 0
		for _, c := range d {
			lo = min(lo, c)
			hi = max(hi, c)
		}
		if lo == 0 {
			return 1e9
		}
		return float64(hi) / float64(lo)
	}

	fewSpread := spread(few.Distribution(ks))
	manySpread := spread(many.Distribution(ks))
	t.Logf("max/min ratio — 1 vnode: %.2f, %d vnodes: %.2f",
		fewSpread, DefaultVNodes, manySpread)

	if manySpread >= fewSpread {
		t.Fatalf("virtual nodes did not improve balance: %.2f with many vs %.2f with one",
			manySpread, fewSpread)
	}
}

func TestEveryKeyLandsSomewhere(t *testing.T) {
	r := NewRing(shards(3), DefaultVNodes)
	for _, k := range keys(1000) {
		if r.Lookup(k) == "" {
			t.Fatalf("key %q mapped to no shard", k)
		}
	}
}

func TestEmptyRingReturnsNothing(t *testing.T) {
	r := NewRing(nil, DefaultVNodes)
	if got := r.Lookup("anything"); got != "" {
		t.Fatalf("empty ring returned %q", got)
	}
}

// --- Codec ----------------------------------------------------------------

func TestShardCommandRoundTrips(t *testing.T) {
	cases := []Command{
		{Op: OpSingle},
		{Op: OpPrepare, TxID: "tx1", Debit: true, Participants: []ID{"s0", "s1"}},
		{Op: OpDecision, TxID: "tx2", Commit: true, Participants: []ID{"s0", "s1"}},
		{Op: OpOutcome, TxID: "tx3", Commit: false, Debit: false},
	}
	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got.Op != want.Op || got.TxID != want.TxID ||
			got.Commit != want.Commit || got.Debit != want.Debit {
			t.Fatalf("round trip: got %+v, want %+v", got, want)
		}
		if len(got.Participants) != len(want.Participants) {
			t.Fatalf("participants: got %v, want %v", got.Participants, want.Participants)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, bad := range [][]byte{{}, {1}, {1, 255, 255}} {
		if _, err := DecodeCommand(bad); err == nil {
			t.Fatalf("decoded garbage %v without error", bad)
		}
	}
}
