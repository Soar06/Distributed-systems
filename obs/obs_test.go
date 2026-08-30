package obs

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/homura/core-bank/raft"
)

// Tests for the observability endpoints (G5).
//
// The property that matters: /readyz must return 503 when this node cannot
// commit, even though the process is perfectly alive and /healthz returns 200.
// Collapsing those two answers into one is the degraded-quorum blind spot
// (learn/READING_LIST.md §16), and it produces opposite failures depending on
// which way it is collapsed — a readiness check wired to liveness restarts nodes
// that were merely waiting out a partition; a liveness check wired to readiness
// keeps routing writes to a node that cannot commit.
//
// Per RULES.md rule 3: normal (healthy node serves), failure (degraded quorum,
// no groups at all), and the parsing path (the emitted metrics are actually
// well-formed Prometheus text, not merely non-empty).

// fakeSource lets a test dictate exactly what the endpoints see.
type fakeSource struct {
	id     string
	shards []ShardHealth
}

func (f *fakeSource) NodeID() string          { return f.id }
func (f *fakeSource) Snapshot() []ShardHealth { return f.shards }

func healthyShard(id string, role raft.Role) ShardHealth {
	return ShardHealth{
		ShardID: id,
		Raft: raft.Health{
			ID: "n1", Role: role, Term: 4,
			CommitIndex: 100, LastApplied: 100, LogLength: 100,
			LeaderID: "n1", Ready: true,
			QuorumContact: 3, QuorumNeeded: 2,
		},
		Accounts: 12,
	}
}

func degradedShard(id, reason string) ShardHealth {
	return ShardHealth{
		ShardID: id,
		Raft: raft.Health{
			ID: "n1", Role: raft.Leader, Term: 4,
			CommitIndex: 100, LastApplied: 90, LogLength: 100,
			LeaderID: "n1", Ready: false, NotReadyReason: reason,
			QuorumContact: 1, QuorumNeeded: 2,
		},
		InDoubt:  2,
		Accounts: 12,
	}
}

func startTestServer(t *testing.T, src Source) *Server {
	t.Helper()
	s, err := Listen("127.0.0.1:0", src)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func get(t *testing.T, s *Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + s.Addr() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// A healthy node: both endpoints agree it is fine.
func TestHealthyNodeIsAliveAndReady(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id:     "n1",
		shards: []ShardHealth{healthyShard("shard-0", raft.Leader)},
	})

	if code, _ := get(t, s, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}
	code, body := get(t, s, "/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 for a healthy node\n%s", code, body)
	}
	if !strings.Contains(body, "ready") {
		t.Fatalf("/readyz body does not say ready:\n%s", body)
	}
}

// THE case: a node that cannot commit is ALIVE but NOT READY.
//
// Conflating these is the whole gap. /healthz must still say 200 — the process is
// fine and restarting it would destroy the cluster's remaining quorum — while
// /readyz says 503, so traffic stops arriving at a node that cannot serve it.
func TestDegradedQuorumIsAliveButNotReady(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			degradedShard("shard-0", "leader has heard from 1 of 2 needed for quorum; cannot commit"),
		},
	})

	code, body := get(t, s, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 — the PROCESS is alive; restarting a node "+
			"that was merely waiting out a partition turns a recoverable degradation "+
			"into a restart storm\n%s", code, body)
	}

	code, body = get(t, s, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 — a node that cannot commit must not be sent "+
			"traffic, and this is exactly the state that otherwise looks healthy from "+
			"outside\n%s", code, body)
	}
	if !strings.Contains(body, "NOT READY") {
		t.Fatalf("/readyz does not say NOT READY:\n%s", body)
	}
	// The reason must be carried through: a bare 503 forces whoever is paged to guess.
	if !strings.Contains(body, "quorum") {
		t.Fatalf("/readyz gives no reason for the failure:\n%s", body)
	}
}

// A process hosting several shards is ready only if EVERY one is.
//
// Any-of would report ready while some accounts cannot be served at all, which is
// worse than saying nothing: traffic would arrive and be rejected.
func TestReadinessRequiresEveryHostedShard(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			healthyShard("shard-0", raft.Leader),
			degradedShard("shard-1", "no known leader"),
		},
	})

	code, body := get(t, s, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d with one of two shards degraded, want 503 — this node "+
			"cannot serve every account it is responsible for\n%s", code, body)
	}
	if !strings.Contains(body, "shard-1") {
		t.Fatalf("the failing shard is not named:\n%s", body)
	}
	if !strings.Contains(body, "shard-0: ready") {
		t.Fatalf("the healthy shard is not reported:\n%s", body)
	}
}

// A node hosting no Raft groups is not ready. It cannot serve anything, and
// reporting ready would put an empty node into rotation.
func TestNodeWithNoGroupsIsNotReady(t *testing.T) {
	s := startTestServer(t, &fakeSource{id: "n1"})

	if code, body := get(t, s, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d for a node hosting no groups, want 503\n%s", code, body)
	}
	// Still alive, though: the process is running.
	if code, _ := get(t, s, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}
}

// --- metrics --------------------------------------------------------------

// The emitted text must be well-formed Prometheus exposition, not merely
// non-empty: every sample line needs a preceding HELP and TYPE for its metric.
func TestMetricsAreWellFormedPrometheusText(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			healthyShard("shard-0", raft.Leader),
			degradedShard("shard-1", "no known leader"),
		},
	})

	code, body := get(t, s, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", code)
	}

	declared := make(map[string]bool) // metric name -> saw HELP
	typed := make(map[string]bool)

	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			parts := strings.SplitN(strings.TrimPrefix(line, "# HELP "), " ", 2)
			if len(parts) != 2 || parts[1] == "" {
				t.Fatalf("HELP line has no description: %q", line)
			}
			declared[parts[0]] = true

		case strings.HasPrefix(line, "# TYPE "):
			parts := strings.Fields(strings.TrimPrefix(line, "# TYPE "))
			if len(parts) != 2 {
				t.Fatalf("malformed TYPE line: %q", line)
			}
			switch parts[1] {
			case "gauge", "counter", "histogram", "summary", "untyped":
			default:
				t.Fatalf("unknown metric type %q in %q", parts[1], line)
			}
			typed[parts[0]] = true

		case line == "" || strings.HasPrefix(line, "#"):
			// blank or other comment

		default:
			// A sample line: NAME{labels} VALUE
			fields := strings.Fields(line)
			if len(fields) != 2 {
				t.Fatalf("sample line is not 'name value': %q", line)
			}
			name := fields[0]
			if i := strings.IndexByte(name, '{'); i >= 0 {
				if !strings.HasSuffix(fields[0], "}") {
					t.Fatalf("unterminated label set: %q", line)
				}
				name = name[:i]
			}
			if !declared[name] {
				t.Fatalf("metric %q emitted with no HELP line: %q", name, line)
			}
			if !typed[name] {
				t.Fatalf("metric %q emitted with no TYPE line: %q", name, line)
			}
		}
	}

	if len(declared) == 0 {
		t.Fatal("no metrics emitted at all")
	}
	t.Logf("%d metrics emitted, all with HELP and TYPE", len(declared))
}

// The readiness signal must be present as a metric, not only as an HTTP status:
// that is what makes the degraded state alertable rather than merely visible.
func TestMetricsExposeReadinessAndInDoubt(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			healthyShard("shard-0", raft.Leader),
			degradedShard("shard-1", "cannot commit"),
		},
	})

	_, body := get(t, s, "/metrics")

	want := []string{
		`corebank_raft_ready{node="n1",shard="shard-0"} 1`,
		`corebank_raft_ready{node="n1",shard="shard-1"} 0`,
		// 2PC blocking, made visible. Nothing else reveals that customer funds are
		// held by a transaction that cannot resolve.
		`corebank_txn_in_doubt{node="n1",shard="shard-1"} 2`,
		// The apply gap: 100 committed, 90 applied on the degraded shard.
		`corebank_raft_apply_lag{node="n1",shard="shard-1"} 10`,
		`corebank_raft_quorum_contact{node="n1",shard="shard-1"} 1`,
		`corebank_raft_quorum_needed{node="n1",shard="shard-1"} 2`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Fatalf("metrics missing %q\n---\n%s", w, body)
		}
	}
}

// Role must be exposed numerically so leadership can be graphed and term churn
// spotted — §5.2's liveness cost is invisible without it.
func TestMetricsExposeRoleAndTerm(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id:     "n2",
		shards: []ShardHealth{healthyShard("shard-0", raft.Follower)},
	})

	_, body := get(t, s, "/metrics")
	if !strings.Contains(body, `corebank_raft_role{node="n2",shard="shard-0"} 0`) {
		t.Fatalf("follower role not exposed as 0:\n%s", body)
	}
	if !strings.Contains(body, `corebank_raft_term{node="n2",shard="shard-0"} 4`) {
		t.Fatalf("term not exposed:\n%s", body)
	}
}

// --- status ---------------------------------------------------------------

// /status must be valid JSON carrying the same facts, since that is what the
// Phase 4 dashboard consumes.
func TestStatusIsValidJSON(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			healthyShard("shard-0", raft.Leader),
			degradedShard("shard-1", "no known leader"),
		},
	})

	code, body := get(t, s, "/status")
	if code != http.StatusOK {
		t.Fatalf("/status = %d, want 200", code)
	}

	var out struct {
		NodeID string `json:"node_id"`
		Shards []struct {
			ShardID string `json:"shard_id"`
			Role    string `json:"role"`
			Ready   bool   `json:"ready"`
			Reason  string `json:"not_ready_reason"`
			InDoubt int    `json:"in_doubt"`
		} `json:"shards"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("/status is not valid JSON: %v\n%s", err, body)
	}
	if out.NodeID != "n1" {
		t.Fatalf("node_id = %q, want n1", out.NodeID)
	}
	if len(out.Shards) != 2 {
		t.Fatalf("%d shards in status, want 2", len(out.Shards))
	}
	for _, sh := range out.Shards {
		if sh.ShardID == "shard-1" {
			if sh.Ready {
				t.Fatal("the degraded shard reports ready in /status")
			}
			if sh.Reason == "" {
				t.Fatal("the degraded shard carries no reason in /status")
			}
			if sh.InDoubt != 2 {
				t.Fatalf("in_doubt = %d, want 2", sh.InDoubt)
			}
		}
	}
}

// Sorting keeps scrapes comparable between polls; unsorted map iteration would
// reorder the output on every request for no reason.
func TestMetricsShardOrderIsStable(t *testing.T) {
	src := &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			healthyShard("shard-2", raft.Leader),
			healthyShard("shard-0", raft.Follower),
			healthyShard("shard-1", raft.Follower),
		},
	}
	s := startTestServer(t, src)

	_, first := get(t, s, "/metrics")
	for range 5 {
		_, again := get(t, s, "/metrics")
		if again != first {
			t.Fatal("two scrapes of unchanged state produced different output")
		}
	}

	i0 := strings.Index(first, `shard="shard-0"`)
	i1 := strings.Index(first, `shard="shard-1"`)
	i2 := strings.Index(first, `shard="shard-2"`)
	if !(i0 < i1 && i1 < i2) {
		t.Fatalf("shards are not in sorted order (positions %d, %d, %d)", i0, i1, i2)
	}
}

// A nil source must be refused rather than panicking on the first scrape.
func TestListenRejectsNilSource(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", nil); err == nil {
		t.Fatal("Listen accepted a nil source")
	}
}
