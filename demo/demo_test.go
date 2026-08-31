package demo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// Tests for the demo control plane and live stream (Phase 4).
//
// The UI's whole job is to tell the truth about the cluster, so what is asserted
// here is that the view MATCHES REALITY — a dashboard that shows a killed node as
// leader, or a stale balance as current, teaches the opposite of what the project
// spent every earlier phase establishing.
//
// Per RULES.md rule 3: normal (money moves, state reflects it), failure (a killed
// node is reported down and its shard fails over), concurrent (two clients on one
// account), and retry (the same idempotency key twice moves money once).

// newTestCluster builds a cluster where every machine holds every shard, which
// is the RF == machine-count case most of these tests assume.
func newTestCluster(t *testing.T, shards, replicas int) *Cluster {
	t.Helper()
	c, err := New(shards, replicas, replicas, 91)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Stop)
	return c
}

// --- the view matches reality --------------------------------------------

func TestSnapshotReportsRealClusterState(t *testing.T) {
	c := newTestCluster(t, 2, 3)

	if _, err := c.Open("alice", 10000); err != nil {
		t.Fatalf("open: %v", err)
	}

	v := c.Snapshot()
	if len(v.Shards) != 2 {
		t.Fatalf("%d shards in view, want 2", len(v.Shards))
	}
	if v.Total != 10000 {
		t.Fatalf("total money = %d, want 10000", v.Total)
	}

	// Every shard must report exactly its replicas, with one leader.
	for _, s := range v.Shards {
		if len(s.Nodes) != 3 {
			t.Fatalf("shard %s shows %d nodes, want 3", s.ID, len(s.Nodes))
		}
		leaders := 0
		for _, n := range s.Nodes {
			if n.Role == "Leader" {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("shard %s shows %d leaders, want exactly 1 — Election Safety "+
				"says at most one per term, and a view showing two is lying", s.ID, leaders)
		}
	}

	// The account must appear on the shard the ring actually assigns it to.
	owner := string(c.sc.Coordinator.ShardFor("alice"))
	found := ""
	for _, s := range v.Shards {
		if _, ok := s.Accounts["alice"]; ok {
			found = s.ID
		}
	}
	if found != owner {
		t.Fatalf("the view places alice on %q but the ring assigns %q; a placement "+
			"view that disagrees with the ring is worse than none", found, owner)
	}
}

// The ring view must use the REAL hash, or it teaches a placement that does not
// exist.
func TestRingViewMatchesActualPlacement(t *testing.T) {
	c := newTestCluster(t, 3, 3)

	for _, name := range []string{"a1", "a2", "a3", "a4", "a5"} {
		if _, err := c.Open(ledger.AccountID(name), 1000); err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
	}

	v := c.Snapshot()
	if len(v.Ring) != 5 {
		t.Fatalf("%d ring points, want 5", len(v.Ring))
	}
	for _, p := range v.Ring {
		want := string(c.sc.Coordinator.ShardFor(ledger.AccountID(p.Account)))
		if p.Shard != want {
			t.Fatalf("ring shows %s on %s, ring says %s", p.Account, p.Shard, want)
		}
		if p.Angle < 0 || p.Angle >= 360 {
			t.Fatalf("%s has angle %.1f, outside the circle", p.Account, p.Angle)
		}
	}

	// Sorted by angle, so the drawn order matches the ring's order.
	for i := 1; i < len(v.Ring); i++ {
		if v.Ring[i].Angle < v.Ring[i-1].Angle {
			t.Fatal("ring points are not in angular order")
		}
	}
}

// --- fault injection ------------------------------------------------------

// THE feature: killing a node must be reported, and the shard must fail over.
func TestKilledNodeIsReportedDownAndShardFailsOver(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if _, err := c.Open("alice", 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	before := c.Snapshot()
	sid := before.Shards[0].ID
	victim := before.Shards[0].Leader
	if victim == "" {
		t.Fatalf("no leader to kill")
	}

	if err := c.Kill(raft.NodeID(victim)); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	// The view must show it down — and NOT as leader. Reporting a killed node as
	// leader is exactly the degraded-quorum blindness G5 removed.
	deadline := time.Now().Add(5 * time.Second)
	var v ClusterView
	for time.Now().Before(deadline) {
		v = c.Snapshot()
		if v.Shards[0].Leader != "" && v.Shards[0].Leader != victim {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, n := range v.Shards[0].Nodes {
		if n.ID == victim {
			if !n.Crashed {
				t.Fatalf("killed node %s is not reported crashed", victim)
			}
			if n.Role == "Leader" {
				t.Fatalf("killed node %s is still shown as Leader", victim)
			}
		}
	}
	if v.Shards[0].Leader == victim || v.Shards[0].Leader == "" {
		t.Fatalf("shard %s did not fail over: leader is %q after killing %s",
			sid, v.Shards[0].Leader, victim)
	}

	// And the surviving majority must still commit.
	if _, err := c.Transact("deposit", "after-kill", "", "alice", 100); err != nil {
		t.Fatalf("the cluster stopped committing after losing one of three: %v", err)
	}
	t.Logf("killed %s, %s took over", victim, v.Shards[0].Leader)
}

// Losing a majority must be reported as NOT READY rather than silently failing.
func TestLosingQuorumIsReportedNotReady(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if _, err := c.Open("alice", 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Kill two of three: no majority remains.
	nodes := c.Snapshot().Shards[0].Nodes
	killed := 0
	for _, n := range nodes {
		if killed == 2 {
			break
		}
		if err := c.Kill(raft.NodeID(n.ID)); err == nil {
			killed++
		}
	}
	if killed != 2 {
		t.Fatalf("killed %d nodes, wanted 2", killed)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := c.Snapshot()
		if !v.Shards[0].Ready {
			if v.Shards[0].Reason == "" {
				t.Fatal("a not-ready shard gives no reason; an operator would have to guess")
			}
			if v.Healthy {
				t.Fatal("the cluster reports healthy while a shard cannot commit")
			}
			t.Logf("quorum lost, correctly reported: %s", v.Shards[0].Reason)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a shard that lost its majority still reports ready — this is exactly " +
		"the degraded state that looks healthy from outside")
}

// A revived node must rejoin and catch up, not stay down forever.
func TestRevivedNodeRejoinsAndCatchesUp(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if _, err := c.Open("alice", 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	victim := ""
	for _, n := range c.Snapshot().Shards[0].Nodes {
		if n.Role != "Leader" {
			victim = n.ID
			break
		}
	}
	if err := c.Kill(raft.NodeID(victim)); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	// Commit while it is away, so it has something to catch up on.
	for i := range 5 {
		c.Transact("deposit", fmt.Sprintf("while-down-%d", i), "", "alice", 100)
	}

	if err := c.Revive(raft.NodeID(victim)); err != nil {
		t.Fatalf("Revive: %v", err)
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		v := c.Snapshot()
		for _, n := range v.Shards[0].Nodes {
			if n.ID == victim && !n.Crashed && !n.Lagging && n.Applied > 0 {
				t.Logf("%s rejoined and caught up to applied=%d", victim, n.Applied)
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never caught up after being revived", victim)
}

// Killing an unknown node, or double-killing, must be refused rather than
// silently accepted — a control plane that pretends to act is worse than one that
// refuses.
func TestInvalidFaultInjectionIsRefused(t *testing.T) {
	c := newTestCluster(t, 1, 3)

	if err := c.Kill("no-such-node"); err == nil {
		t.Fatal("killing an unknown node was accepted")
	}
	if err := c.Revive("no-such-node"); err == nil {
		t.Fatal("reviving an unknown node was accepted")
	}

	victim := c.Snapshot().Shards[0].Nodes[0].ID
	if err := c.Kill(raft.NodeID(victim)); err != nil {
		t.Fatalf("first kill: %v", err)
	}
	if err := c.Kill(raft.NodeID(victim)); err == nil {
		t.Fatal("killing an already-dead node was accepted")
	}
	if err := c.Revive(raft.NodeID(victim)); err != nil {
		t.Fatalf("revive: %v", err)
	}
	if err := c.Revive(raft.NodeID(victim)); err == nil {
		t.Fatal("reviving a live node was accepted")
	}
}

// --- the client contract --------------------------------------------------

// The same idempotency key twice must move money once.
func TestRetryWithSameKeyMovesMoneyOnce(t *testing.T) {
	c := newTestCluster(t, 2, 3)

	if _, err := c.Open("alice", 10000); err != nil {
		t.Fatalf("open: %v", err)
	}

	c.Transact("deposit", "dup-key", "", "alice", 2500)
	c.Transact("deposit", "dup-key", "", "alice", 2500)

	v := c.Snapshot()
	if v.Total != 12500 {
		t.Fatalf("total money = %d after the same key twice, want 12500 — a retried "+
			"request moved money a second time", v.Total)
	}
}

// Two clients withdrawing from one account concurrently: the ledger serializes
// them, so the balance never goes negative and money is conserved.
//
// This is the question NOW.md deliberately left open until the consensus core was
// understood. The backend's answer is what the UI must display.
func TestConcurrentWithdrawalsNeverOverdraw(t *testing.T) {
	c := newTestCluster(t, 2, 3)

	if _, err := c.Open("alice", 3000); err != nil {
		t.Fatalf("open: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := 0

	for i := range 6 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Transact("withdraw", fmt.Sprintf("race-%d", i), "alice", "", 1000)
			if err == nil && res.OK {
				mu.Lock()
				committed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	v := c.Snapshot()
	if v.Total < 0 {
		t.Fatalf("total money went negative: %d", v.Total)
	}
	if committed > 3 {
		t.Fatalf("%d withdrawals of 10.00 committed against a 30.00 balance — the "+
			"same money was spent more than once", committed)
	}
	want := int64(3000 - committed*1000)
	if v.Total != want {
		t.Fatalf("total money = %d after %d committed withdrawals, want %d",
			v.Total, committed, want)
	}
	t.Logf("%d of 6 concurrent withdrawals committed; balance %d, never negative",
		committed, v.Total)
}

// --- HTTP surface ---------------------------------------------------------

func startTestServer(t *testing.T, c *Cluster) *Server {
	t.Helper()
	s, err := Listen("127.0.0.1:0", "../fe", c)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestControlEndpointsWork(t *testing.T) {
	c := newTestCluster(t, 1, 3)
	s := startTestServer(t, c)

	post := func(path string) map[string]any {
		t.Helper()
		resp, err := http.Post("http://"+s.Addr()+path, "", nil)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		json.NewDecoder(resp.Body).Decode(&out)
		return out
	}

	if r := post("/api/open?account=zoe&amount=4000"); r["ok"] != true {
		t.Fatalf("open failed: %+v", r)
	}
	if r := post("/api/tx?op=deposit&key=http-1&to=zoe&amount=600"); r["ok"] != true {
		t.Fatalf("deposit failed: %+v", r)
	}

	v := c.Snapshot()
	if v.Total != 4600 {
		t.Fatalf("total = %d after open+deposit over HTTP, want 4600", v.Total)
	}

	// A write with no idempotency key must be refused, not silently accepted.
	if r := post("/api/tx?op=deposit&to=zoe&amount=100"); r["ok"] == true {
		t.Fatal("a write with no idempotency key was accepted over HTTP")
	}
}

// The stream must actually push frames, and each frame must be a complete,
// parseable view.
func TestStreamPushesCompleteFrames(t *testing.T) {
	c := newTestCluster(t, 1, 3)
	if _, err := c.Open("alice", 1000); err != nil {
		t.Fatalf("open: %v", err)
	}
	s := startTestServer(t, c)

	req, _ := http.NewRequest("GET", "http://"+s.Addr()+"/api/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Read a few frames. Each must be a COMPLETE view — that is what makes a
	// dropped frame harmless and lets the server skip rather than buffer.
	reader := bufio.NewReader(resp.Body)
	frames := 0
	deadline := time.Now().Add(3 * time.Second)

	for frames < 3 && time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil || !strings.HasPrefix(line, "data: ") {
			continue
		}
		var v ClusterView
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &v); err != nil {
			t.Fatalf("frame %d is not valid JSON: %v", frames, err)
		}
		if len(v.Shards) == 0 {
			t.Fatalf("frame %d carries no shards; a partial frame is not a complete "+
				"snapshot and a client that receives only it would be wrong", frames)
		}
		if v.Total != 1000 {
			t.Fatalf("frame %d reports total %d, want 1000", frames, v.Total)
		}
		frames++
	}

	if frames < 3 {
		t.Fatalf("received %d frames in 3s, want at least 3 — the stream must push "+
			"often enough to show transient states like a candidate mid-election", frames)
	}
}
