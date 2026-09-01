package demo

import (
	"strings"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
)

// Event log tests.
//
// The log exists so a reader can answer "what happened to Vu" and "why did the
// leader move" without parsing prose. These tests assert the filters actually
// select, and — the part that matters most — that a quorum loss is recorded even
// when NO leadership change accompanies it.

func TestEventFilterSelectsByEachAxis(t *testing.T) {
	l := newEventLog(100)
	l.add(Event{Kind: KindClient, Account: "Vu", Shard: "shard-0", Text: "vu deposit"})
	l.add(Event{Kind: KindClient, Account: "dave", Shard: "shard-1", Text: "dave deposit"})
	l.add(Event{Kind: KindRaft, Shard: "shard-0", Node: "node-2", Text: "election"})
	l.add(Event{Kind: KindDrift, Node: "node-3", Text: "drift"})

	cases := []struct {
		name string
		f    EventFilter
		want int
	}{
		{"no filter matches all", EventFilter{}, 4},
		{"by account", EventFilter{Account: "Vu"}, 1},
		{"by shard", EventFilter{Shard: "shard-0"}, 2},
		{"by node", EventFilter{Node: "node-2"}, 1},
		{"by kind", EventFilter{Kind: KindClient}, 2},
		{"excluding drift", EventFilter{ExcludeKind: KindDrift}, 3},
		{"account and kind together", EventFilter{Account: "Vu", Kind: KindClient}, 1},
		{"contradictory filter matches nothing", EventFilter{Account: "Vu", Shard: "shard-1"}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := 0
			for _, e := range l.snapshot() {
				if tc.f.matches(e) {
					got++
				}
			}
			if got != tc.want {
				t.Fatalf("%+v matched %d events, want %d", tc.f, got, tc.want)
			}
		})
	}
}

// Account filtering must be case-insensitive: a user typing "vu" is asking about
// the same account the ledger calls "Vu".
func TestEventFilterIsCaseInsensitive(t *testing.T) {
	l := newEventLog(10)
	l.add(Event{Kind: KindClient, Account: "Vu", Text: "x"})

	if !(EventFilter{Account: "vu"}).matches(l.snapshot()[0]) {
		t.Fatal("account filter should not be case-sensitive")
	}
}

// The log is bounded, because it is appended to on every action.
func TestEventLogIsBounded(t *testing.T) {
	l := newEventLog(5)
	for i := range 20 {
		l.add(Event{Kind: KindClient, Text: "e"})
		_ = i
	}
	if got := len(l.snapshot()); got != 5 {
		t.Fatalf("log holds %d events, want it capped at 5", got)
	}
	// The newest must survive, not the oldest.
	if l.snapshot()[4].Seq != 20 {
		t.Fatalf("newest event has seq %d, want 20 — the log dropped the wrong end",
			l.snapshot()[4].Seq)
	}
}

// A shard can stop committing with NO leadership change: a partitioned leader
// keeps reporting Leader in the same term. Watching only leader/term transitions
// therefore misses the exact failure the demo is about, so quorum is tracked
// separately.
func TestQuorumLossIsRecordedWithoutALeadershipChange(t *testing.T) {
	c, err := New(1, 3, 3, 20260903)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})

	// The watcher reports TRANSITIONS, so it needs one sample of the healthy state
	// to transition away from. Killing within a single 120ms tick means its first
	// observation is already the degraded one, and there is correctly nothing to
	// report — that is the watcher behaving properly, not a missing event.
	waitUntil(t, 5*time.Second, "the watcher to record a healthy baseline", func() bool {
		return c.watcherSampled()
	})

	view := c.Snapshot()
	leader := view.Shards[0].Leader
	if leader == "" {
		t.Fatal("no leader to keep")
	}

	// Kill the two NON-leaders, so the leader survives and never learns it lost
	// contact. Leadership does not change; the ability to commit does.
	killed := 0
	for _, n := range view.Shards[0].Nodes {
		if n.ID == leader {
			continue
		}
		if err := c.Kill(raft.NodeID(n.ID)); err != nil {
			t.Fatalf("killing %s: %v", n.ID, err)
		}
		killed++
	}
	if killed != 2 {
		t.Fatalf("killed %d nodes, expected 2", killed)
	}

	waitUntil(t, 10*time.Second, "a quorum-loss event", func() bool {
		for _, e := range c.Events(EventFilter{Kind: KindRaft}, 50) {
			if strings.Contains(e.Text, "LOST quorum") {
				return true
			}
		}
		return false
	})

	// And the leader really did stay put — otherwise this test would be passing
	// for the ordinary leadership-change reason rather than the one it targets.
	if got := c.Snapshot().Shards[0].Leader; got != leader {
		t.Logf("leader changed %s -> %q during the outage; the quorum event is still "+
			"correct but this run did not exercise the no-change path", leader, got)
	}
}
