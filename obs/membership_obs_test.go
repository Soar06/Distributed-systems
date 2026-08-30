package obs

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/homura/core-bank/raft"
)

// Membership must be visible from outside (G6 follow-up to G5).
//
// A node left behind on an old configuration is dangerous in a specific way: it
// counts quorum against a membership that no longer exists. Nothing in the
// role/term/commit signals reveals it, so the configuration itself has to be
// exposed — otherwise a reconfiguration that only reached two of three nodes
// looks exactly like one that reached all three.

func memberShard(id string, servers []raft.NodeID, configFailures int) ShardHealth {
	return ShardHealth{
		ShardID: id,
		Raft: raft.Health{
			ID: "n1", Role: raft.Leader, Term: 3,
			CommitIndex: 10, LastApplied: 10, LogLength: 10,
			LeaderID: "n1", Ready: true,
			QuorumContact: len(servers), QuorumNeeded: len(servers)/2 + 1,
			ConfigServers: servers, ConfigFailures: configFailures,
		},
	}
}

// The cluster size must be a metric, so a node left behind is alertable rather
// than merely discoverable by reading JSON.
func TestClusterSizeAndConfigFailuresAreExposedAsMetrics(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id: "n1",
		shards: []ShardHealth{
			memberShard("shard-0", []raft.NodeID{"n1", "n2", "n3"}, 0),
			// A node still on the old 4-server configuration, and one that saw a
			// malformed entry.
			memberShard("shard-1", []raft.NodeID{"n1", "n2", "n3", "n4"}, 2),
		},
	})

	_, body := get(t, s, "/metrics")

	for _, want := range []string{
		`corebank_raft_cluster_size{node="n1",shard="shard-0"} 3`,
		`corebank_raft_cluster_size{node="n1",shard="shard-1"} 4`,
		`corebank_raft_config_failures_total{node="n1",shard="shard-0"} 0`,
		`corebank_raft_config_failures_total{node="n1",shard="shard-1"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n---\n%s", want, body)
		}
	}
}

// /status must name the members, not merely count them: an operator diagnosing a
// stuck reconfiguration needs to know WHICH server is in or out.
func TestStatusNamesTheClusterMembers(t *testing.T) {
	s := startTestServer(t, &fakeSource{
		id:     "n1",
		shards: []ShardHealth{memberShard("shard-0", []raft.NodeID{"n1", "n2", "n3"}, 0)},
	})

	code, body := get(t, s, "/status")
	if code != http.StatusOK {
		t.Fatalf("/status = %d", code)
	}

	var out struct {
		Shards []struct {
			Members []string `json:"members"`
		} `json:"shards"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("/status is not valid JSON: %v\n%s", err, body)
	}
	if len(out.Shards) != 1 || len(out.Shards[0].Members) != 3 {
		t.Fatalf("members not reported: %s", body)
	}
	for _, want := range []string{"n1", "n2", "n3"} {
		found := false
		for _, m := range out.Shards[0].Members {
			if m == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("member %q missing from /status: %v", want, out.Shards[0].Members)
		}
	}
}
