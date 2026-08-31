package demo

import (
	"errors"
	"testing"
	"time"

	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// A write during a majority outage is INDETERMINATE, not refused.
//
// This is the case a user found in the UI before it was ever found in a test:
// two of three replicas were down, a withdraw returned an error, and the money
// left the account anyway once the machines came back. Both halves of that were
// correct behaviour — the entry really was appended and really did commit — but
// the API called it a plain failure, which is a lie about a write that is still
// in flight.
//
// Why it matters beyond wording: a caller told "refused" may reasonably reissue
// with a NEW idempotency key. Both entries then commit and the withdrawal is
// applied twice. Reporting indeterminate is what makes "retry with the same key"
// the obvious response.

// waitUntil polls until cond holds or the deadline passes.
func waitUntil(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", within, what)
}

func TestWriteDuringMajorityOutageIsIndeterminateNotRefused(t *testing.T) {
	c, err := New(1, 3, 3, 20260901)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})

	if _, err := c.Open("dave", 10_000); err != nil {
		t.Fatalf("opening dave: %v", err)
	}

	// Kill two of three: the shard keeps a leader for a while (it does not yet
	// know it lost contact) but can no longer commit.
	view := c.Snapshot()
	holders := view.Shards[0].Nodes
	if len(holders) < 3 {
		t.Fatalf("expected 3 replicas, got %d", len(holders))
	}
	for _, h := range holders[:2] {
		if err := c.Kill(raft.NodeID(h.ID)); err != nil {
			t.Fatalf("killing %s: %v", h.ID, err)
		}
	}

	waitUntil(t, 10*time.Second, "the shard to lose write capability", func() bool {
		return !c.Snapshot().Shards[0].CanWrite
	})

	_, err = c.Transact("withdraw", "outage-1", "dave", "", 500)
	if err == nil {
		t.Fatal("a write with no majority must not report success")
	}

	// THE ASSERTION THAT MATTERS. Not merely "it failed" — it must be reported as
	// indeterminate, because the entry is in the leader's log and may still
	// commit.
	if !errors.Is(err, shard.ErrCommitUnknown) && !errors.Is(err, shard.ErrInDoubt) {
		t.Fatalf("a write that was appended but not committed must be INDETERMINATE, "+
			"got a plain failure: %v\n"+
			"Reporting this as refused invites a retry under a new key, which "+
			"double-applies once both entries commit.", err)
	}
}

// The other half of the same story: whatever the outcome is called, the money
// must be right. A retry under the SAME key must not double-apply, even though
// the first attempt reported an error and later committed.
func TestIndeterminateWriteRetriedWithSameKeyAppliesOnce(t *testing.T) {
	c, err := New(1, 3, 3, 20260902)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	waitUntil(t, 10*time.Second, "a leader", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})

	if _, err := c.Open("dave", 10_000); err != nil {
		t.Fatalf("opening dave: %v", err)
	}

	view := c.Snapshot()
	holders := view.Shards[0].Nodes
	killed := make([]raft.NodeID, 0, 2)
	for _, h := range holders[:2] {
		id := raft.NodeID(h.ID)
		if err := c.Kill(id); err != nil {
			t.Fatalf("killing %s: %v", id, err)
		}
		killed = append(killed, id)
	}

	waitUntil(t, 10*time.Second, "loss of write capability", func() bool {
		return !c.Snapshot().Shards[0].CanWrite
	})

	// Attempt during the outage: reports indeterminate, entry lands in the log.
	if _, err := c.Transact("withdraw", "same-key", "dave", "", 500); err == nil {
		t.Fatal("expected an error during the outage")
	}

	for _, id := range killed {
		if err := c.Revive(id); err != nil {
			t.Fatalf("reviving %s: %v", id, err)
		}
	}
	waitUntil(t, 15*time.Second, "quorum to return", func() bool {
		return c.Snapshot().Shards[0].CanWrite
	})

	// The retry the API now tells the caller to make. Same key, so if the first
	// attempt committed this must be a no-op rather than a second withdrawal.
	waitUntil(t, 10*time.Second, "the retry to be answerable", func() bool {
		_, err := c.Transact("withdraw", "same-key", "dave", "", 500)
		return err == nil
	})

	// 10000 - 500, applied EXACTLY once across the outage, the replay, and the
	// retry. 9000 would mean the same key withdrew twice.
	bal, ok := c.Snapshot().Shards[0].Accounts["dave"]
	if !ok {
		t.Fatal("dave has no balance after recovery")
	}
	if bal != 9_500 {
		t.Fatalf("dave=%d, want 9500: the withdrawal was applied more than once "+
			"(9000) or lost entirely (10000)", bal)
	}
}
