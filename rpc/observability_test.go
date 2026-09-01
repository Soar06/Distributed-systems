package rpc

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/obs"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// End-to-end observability against a real running cluster (G5).
//
// obs/obs_test.go proves the endpoints render correctly from a dictated health
// snapshot. This file proves the snapshot itself is TRUE — that a real cluster
// which has genuinely lost quorum reports it, rather than a hand-written struct
// saying so.
//
// That gap is the whole point of the feature. The degraded state is dangerous
// precisely because every superficial signal stays green, so a test that fakes
// the health input tests the rendering and not the detection.

// mustReplica fetches a hosted replica or fails the test.
func mustReplica(t *testing.T, c *shardTestCluster, node raft.NodeID, sid shard.ID) *Replica {
	t.Helper()
	rep, ok := c.nodes[node].host.Replica(sid)
	if !ok {
		t.Fatalf("node %s hosts no replica of %s", node, sid)
	}
	return rep
}

func obsGet(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// startObs attaches observability to one node of a running shard cluster.
func startObs(t *testing.T, c *shardTestCluster, id raft.NodeID) *obs.Server {
	t.Helper()
	node := c.nodes[id]
	// Two heartbeats (40ms each here), matching what the binaries use.
	src := NewHostSource(string(id), node.host, 80*time.Millisecond)
	s, err := obs.Listen("127.0.0.1:0", src)
	if err != nil {
		t.Fatalf("obs.Listen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// A healthy running cluster reports ready, with real numbers.
func TestLiveClusterReportsReady(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, _ := c.crossShardPair()
	if err := c.open(a, 5000); err != nil {
		t.Fatalf("open: %v", err)
	}

	// Observe the node leading the shard that owns the account.
	owner := c.ring.Lookup(string(a))
	o := startObs(t, c, c.leaderFor(owner))

	// Readiness needs a heartbeat round to have happened, since a freshly elected
	// leader starts with no contact recorded.
	deadline := time.Now().Add(3 * time.Second)
	var code int
	var body string
	for time.Now().Before(deadline) {
		code, body = obsGet(t, o.Addr(), "/readyz")
		if code == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d on a healthy cluster, want 200\n%s%s", code, body, c.view())
	}

	if code, _ := obsGet(t, o.Addr(), "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}

	// The metrics must reflect real cluster state, not zeros.
	_, metrics := obsGet(t, o.Addr(), "/metrics")
	if !strings.Contains(metrics, "corebank_ledger_accounts") {
		t.Fatalf("account metric missing:\n%s", metrics)
	}
	if !strings.Contains(metrics, `corebank_raft_ready{`) {
		t.Fatalf("readiness metric missing:\n%s", metrics)
	}
	t.Logf("live cluster reports ready; %d bytes of metrics", len(metrics))
}

// THE end-to-end case: a leader that genuinely loses its peers must report NOT
// ready, while still reporting alive.
//
// Every peer replica of the shard is stopped, so the leader is alone. It keeps
// its role and its term — Raft does not take leadership away from a node that
// simply stops hearing replies — and it cannot commit anything. That is the state
// that looks healthy from outside, and the state /readyz exists to expose.
func TestLeaderThatLosesQuorumReportsNotReady(t *testing.T) {
	c := startShardCluster(t, 3, 1)

	sid := c.shards[0]
	leader := c.leaderFor(sid)
	if leader == "" {
		t.Fatalf("no leader%s", c.view())
	}
	o := startObs(t, c, leader)

	// Wait until it is ready, so the transition is unambiguous.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if code, _ := obsGet(t, o.Addr(), "/readyz"); code == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, body := obsGet(t, o.Addr(), "/readyz"); code != http.StatusOK {
		t.Fatalf("cluster never became ready: %d\n%s%s", code, body, c.view())
	}

	// Now stop every OTHER replica of this shard. The leader is isolated.
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if rep, ok := c.nodes[id].host.Replica(sid); ok {
			rep.Raft.Stop()
		}
	}

	// Within a couple of readiness windows the leader must notice.
	deadline = time.Now().Add(3 * time.Second)
	var code int
	var body string
	for time.Now().Before(deadline) {
		code, body = obsGet(t, o.Addr(), "/readyz")
		if code == http.StatusServiceUnavailable {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d after losing every peer, want 503. The leader still "+
			"holds its role and cannot commit a thing — this is exactly the degraded "+
			"state that looks healthy from outside\n%s%s", code, body, c.view())
	}
	if !strings.Contains(body, "quorum") {
		t.Fatalf("/readyz gives no quorum reason:\n%s", body)
	}

	// Still ALIVE: the process is fine, and restarting it here would destroy the
	// cluster's remaining quorum rather than help.
	if hc, _ := obsGet(t, o.Addr(), "/healthz"); hc != http.StatusOK {
		t.Fatalf("/healthz = %d on a live-but-isolated node, want 200", hc)
	}

	// And the metric agrees with the status code, so this is alertable.
	_, metrics := obsGet(t, o.Addr(), "/metrics")
	if !strings.Contains(metrics, `corebank_raft_ready{node="`+string(leader)+`",shard="`+string(sid)+`"} 0`) {
		t.Fatalf("the readiness METRIC does not report 0 while /readyz reports 503; "+
			"an operator watching dashboards would see nothing wrong\n%s", metrics)
	}
	t.Logf("isolated leader correctly reports 503 while alive: %s", strings.TrimSpace(body))
}

// A follower reports ready while it hears from its leader — it can serve
// stale-tolerant reads, which is the basis of follower reads in LATER.md.
func TestFollowerReportsReadyWhileHearingFromLeader(t *testing.T) {
	c := startShardCluster(t, 3, 1)

	sid := c.shards[0]
	follower := c.followerFor(sid)
	if follower == "" {
		t.Fatalf("no follower%s", c.view())
	}
	o := startObs(t, c, follower)

	deadline := time.Now().Add(3 * time.Second)
	var code int
	var body string
	for time.Now().Before(deadline) {
		code, body = obsGet(t, o.Addr(), "/readyz")
		if code == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d on a follower in contact with its leader, want 200\n%s%s",
			code, body, c.view())
	}
}

// In-doubt 2PC transactions must be visible as a metric.
//
// Nothing at the Raft level reveals them: the log is healthy, the cluster is
// committing, and yet customer funds are reserved against a transaction that
// cannot resolve. Without this number that state is invisible until someone
// notices the money.
func TestInDoubtTransactionsAreVisibleInMetrics(t *testing.T) {
	c := startShardCluster(t, 3, 2)

	a, b := c.crossShardPair()
	c.mustOpen(t, a, 10000)
	c.mustOpen(t, b, 1000)

	debitShard := c.ring.Lookup(string(a))
	creditShard := c.ring.Lookup(string(b))
	leader := c.leaderFor(debitShard)
	o := startObs(t, c, leader)

	// Baseline: nothing in doubt.
	_, before := obsGet(t, o.Addr(), "/metrics")
	if !strings.Contains(before, `corebank_txn_in_doubt{node="`+string(leader)+`",shard="`+string(debitShard)+`"} 0`) {
		t.Fatalf("expected zero in-doubt before preparing:\n%s", before)
	}

	// Prepare without deciding: the debit side is now blocked holding funds.
	//
	// Driven straight through the group rather than via Coordinator.Transfer,
	// because the point is to stop the protocol precisely between prepare and
	// decision — the window where a participant is in doubt.
	cmd := ledger.Command{
		Op: ledger.OpTransfer, IdempotencyKey: "obs-doubt",
		From: a, To: b, Amount: 3000,
	}
	group := NewNetworkGroup(debitShard, mustReplica(t, c, leader, debitShard))
	if res, _, err := group.Propose(shard.Command{
		Op: shard.OpPrepare, TxID: "obs-doubt", Ledger: cmd, Debit: true,
		Participants: []shard.ID{debitShard, creditShard}, Coordinator: debitShard,
	}, 3*time.Second); err != nil || !res.OK {
		t.Fatalf("prepare: err=%v res=%+v", err, res)
	}

	deadline := time.Now().Add(2 * time.Second)
	var after string
	for time.Now().Before(deadline) {
		_, after = obsGet(t, o.Addr(), "/metrics")
		if strings.Contains(after, `corebank_txn_in_doubt{node="`+string(leader)+`",shard="`+string(debitShard)+`"} 1`) {
			t.Logf("in-doubt transaction visible in metrics")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("an in-doubt transaction is not visible in metrics; customer funds are "+
		"reserved against a transaction that cannot resolve and nothing shows it\n%s", after)
}
