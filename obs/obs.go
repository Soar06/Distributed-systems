// Package obs exposes a node's internal state: metrics, health, and readiness (G5).
//
// The gap it closes, stated as the operator sees it: a cluster committing against
// a degraded quorum looks identical from outside to a healthy one. Every
// superficial signal — the port answers, the role reports, a leader exists — stays
// green while nothing can actually commit. For a bank that means writes which
// appear accepted and are not.
//
// Theory in learn/READING_LIST.md §16.
//
// [project decision] The Prometheus text exposition format is emitted by hand
// rather than through the client library. The format is a few lines of text and
// the metric set here is small and fixed, so the project's zero-dependency rule is
// worth more than the library's convenience — the same reasoning that chose
// net/rpc over gRPC in Phase 1.
//
// [project decision] This listens on its own port, separate from the RPC port. An
// endpoint sharing the consensus port cannot be scraped while the consensus path
// is saturated, which is exactly when the numbers are needed. It also keeps the
// auth story clean: the RPC port requires mutual TLS, while metrics are read by a
// scraper holding no cluster credentials.
package obs

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/homura/core-bank/raft"
)

// ShardHealth is one Raft group's health, plus the domain state an operator needs
// alongside it.
type ShardHealth struct {
	ShardID string
	Raft    raft.Health

	// InDoubt is the count of 2PC transactions this shard is blocked on. Exposed
	// because it is the one number that distinguishes "slow" from "stuck holding
	// customer funds", and it is invisible from any Raft-level signal.
	InDoubt int

	// Accounts is how many accounts this shard's ledger holds.
	Accounts int
}

// Admission is the node-level backpressure state (G7).
//
// Node-level rather than per-shard because admission control is applied at the
// client API, in front of the routing decision — a request is shed before anyone
// knows which shard it belongs to.
type Admission struct {
	InFlight  int64
	Admitted  int64
	ShedBusy  int64
	ShedRate  int64
	Draining  bool
	Available bool
}

// Source supplies the current health of every Raft group in this process.
//
// An interface rather than a concrete type so obs depends on neither rpc nor
// shard: the dependency runs one way, and a single-group node and a sharded node
// can both be described the same way.
type Source interface {
	// NodeID identifies this process.
	NodeID() string

	// Snapshot returns the current health of every group hosted here.
	Snapshot() []ShardHealth
}

// AdmissionSource is an optional Source extension reporting backpressure state.
//
// Optional so a Source predating G7 still satisfies the base interface.
type AdmissionSource interface {
	Admission() Admission
}

// Server hosts the observability endpoints.
type Server struct {
	src      Source
	http     *http.Server
	listener net.Listener
	started  time.Time
}

// Listen starts the observability HTTP server on addr.
func Listen(addr string, src Source) (*Server, error) {
	if src == nil {
		return nil, fmt.Errorf("obs: nil source")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("obs: listen %s: %w", addr, err)
	}

	s := &Server{src: src, listener: l, started: time.Now()}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)

	s.http = &http.Server{
		Handler: mux,
		// A scrape must never be able to park a connection indefinitely: the
		// observability port has to stay answerable precisely when the node is
		// unhealthy.
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go s.http.Serve(l)
	return s, nil
}

// Addr returns the bound address, useful when the port was 0.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close shuts the server down.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.http.Shutdown(ctx)
}

// handleHealthz answers LIVENESS: is this process running?
//
// Deliberately says nothing about whether the node can serve. A liveness probe
// wired to readiness restarts nodes that were merely waiting out a partition,
// turning a recoverable degradation into a restart storm that destroys the
// cluster's remaining quorum.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok\nnode=%s\nuptime=%s\n", s.src.NodeID(), time.Since(s.started).Round(time.Second))
}

// handleReadyz answers READINESS: can this node serve?
//
// 200 only when EVERY group hosted here is in a quorum that can make progress.
// 503 otherwise, with the reason per shard — a boolean with no reason forces
// whoever is paged to guess.
//
// All groups rather than any: a process hosting shard-0 and shard-1 where shard-1
// has lost quorum cannot serve every account it is responsible for, and reporting
// ready would send it traffic it must reject.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	shards := s.src.Snapshot()

	ready := len(shards) > 0
	var b strings.Builder
	for _, sh := range shards {
		if !sh.Raft.Ready {
			ready = false
			fmt.Fprintf(&b, "%s: NOT READY (%s)\n", sh.ShardID, sh.Raft.NotReadyReason)
			continue
		}
		fmt.Fprintf(&b, "%s: ready (%v, term %d)\n", sh.ShardID, sh.Raft.Role, sh.Raft.Term)
	}
	if len(shards) == 0 {
		b.WriteString("no Raft groups hosted on this node\n")
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	fmt.Fprintf(w, "node=%s\n%s", s.src.NodeID(), b.String())
}

// handleStatus returns the whole picture as JSON, for the Phase 4 dashboard.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	type shardOut struct {
		ShardID       string   `json:"shard_id"`
		Role          string   `json:"role"`
		Term          uint64   `json:"term"`
		CommitIndex   uint64   `json:"commit_index"`
		LastApplied   uint64   `json:"last_applied"`
		LogLength     int      `json:"log_length"`
		LeaderID      string   `json:"leader_id"`
		Ready         bool     `json:"ready"`
		Reason        string   `json:"not_ready_reason,omitempty"`
		QuorumContact int      `json:"quorum_contact"`
		QuorumNeeded  int      `json:"quorum_needed"`
		SnapshotIndex uint64   `json:"snapshot_index"`
		HasSnapshot   bool     `json:"has_snapshot"`
		Members       []string `json:"members"`
		InDoubt       int      `json:"in_doubt"`
		Accounts      int      `json:"accounts"`
	}
	out := struct {
		NodeID string     `json:"node_id"`
		Uptime string     `json:"uptime"`
		Shards []shardOut `json:"shards"`
	}{NodeID: s.src.NodeID(), Uptime: time.Since(s.started).Round(time.Second).String()}

	for _, sh := range s.src.Snapshot() {
		out.Shards = append(out.Shards, shardOut{
			ShardID:       sh.ShardID,
			Role:          sh.Raft.Role.String(),
			Term:          uint64(sh.Raft.Term),
			CommitIndex:   uint64(sh.Raft.CommitIndex),
			LastApplied:   uint64(sh.Raft.LastApplied),
			LogLength:     sh.Raft.LogLength,
			LeaderID:      string(sh.Raft.LeaderID),
			Ready:         sh.Raft.Ready,
			Reason:        sh.Raft.NotReadyReason,
			QuorumContact: sh.Raft.QuorumContact,
			QuorumNeeded:  sh.Raft.QuorumNeeded,
			SnapshotIndex: uint64(sh.Raft.SnapshotIndex),
			// Distinguishes "never compacted" from "compacted at index 0", which the
			// index alone cannot express.
			HasSnapshot: sh.Raft.HasSnapshot,
			Members:     members(sh.Raft.ConfigServers),
			InDoubt:     sh.InDoubt,
			Accounts:    sh.Accounts,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

// members renders a configuration for JSON output.
func members(ids []raft.NodeID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, string(id))
	}
	return out
}

// handleMetrics emits the Prometheus text exposition format.
//
// The metric set is chosen for what reveals DISAGREEMENT, which is what matters in
// a consensus system (§16): term churn is leadership instability, a growing gap
// between commit and applied means the state machine is falling behind, and the
// in-doubt count is 2PC's blocking made visible rather than inferred.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	shards := s.src.Snapshot()
	sort.Slice(shards, func(i, j int) bool { return shards[i].ShardID < shards[j].ShardID })

	node := s.src.NodeID()
	var b strings.Builder

	metric := func(name, help, typ string, emit func()) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
		emit()
	}

	lbl := func(sh ShardHealth) string {
		return fmt.Sprintf(`{node="%s",shard="%s"}`, node, sh.ShardID)
	}

	metric("corebank_raft_role",
		"Raft role of this node for a shard (0=follower, 1=candidate, 2=leader).", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_role%s %d\n", lbl(sh), int(sh.Raft.Role))
			}
		})

	metric("corebank_raft_term",
		"Current Raft term. Rapid growth means leadership churn, which costs liveness.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_term%s %d\n", lbl(sh), sh.Raft.Term)
			}
		})

	metric("corebank_raft_commit_index", "Highest log index known committed.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_commit_index%s %d\n", lbl(sh), sh.Raft.CommitIndex)
			}
		})

	metric("corebank_raft_last_applied", "Highest log index applied to the state machine.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_last_applied%s %d\n", lbl(sh), sh.Raft.LastApplied)
			}
		})

	metric("corebank_raft_apply_lag",
		"commitIndex minus lastApplied. A growing value means the state machine is behind.", "gauge",
		func() {
			for _, sh := range shards {
				lag := int64(sh.Raft.CommitIndex) - int64(sh.Raft.LastApplied)
				fmt.Fprintf(&b, "corebank_raft_apply_lag%s %d\n", lbl(sh), lag)
			}
		})

	metric("corebank_raft_log_entries",
		"Log length. The input to the compaction decision (section 7).", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_log_entries%s %d\n", lbl(sh), sh.Raft.LogLength)
			}
		})

	metric("corebank_raft_snapshot_index",
		"Log index covered by the newest snapshot; 0 if never compacted.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_snapshot_index%s %d\n", lbl(sh), sh.Raft.SnapshotIndex)
			}
		})

	metric("corebank_raft_ready",
		"1 if this node is in a quorum that can commit, 0 otherwise. THE signal that "+
			"distinguishes a degraded cluster from a healthy one.", "gauge",
		func() {
			for _, sh := range shards {
				v := 0
				if sh.Raft.Ready {
					v = 1
				}
				fmt.Fprintf(&b, "corebank_raft_ready%s %d\n", lbl(sh), v)
			}
		})

	metric("corebank_raft_quorum_contact",
		"Servers this leader has heard from recently, including itself. 0 on a follower.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_quorum_contact%s %d\n", lbl(sh), sh.Raft.QuorumContact)
			}
		})

	metric("corebank_raft_quorum_needed", "Majority size for the full cluster.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_quorum_needed%s %d\n", lbl(sh), sh.Raft.QuorumNeeded)
			}
		})

	metric("corebank_txn_in_doubt",
		"2PC transactions blocked awaiting a decision. These hold reserved customer funds.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_txn_in_doubt%s %d\n", lbl(sh), sh.InDoubt)
			}
		})

	metric("corebank_ledger_accounts", "Accounts held by this shard's ledger.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_ledger_accounts%s %d\n", lbl(sh), sh.Accounts)
			}
		})

	metric("corebank_storage_persist_failures_total",
		"Failures to persist the applied-index marker. Non-zero means storage is degrading.",
		"counter",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_storage_persist_failures_total%s %d\n",
					lbl(sh), sh.Raft.AppliedPersistFailures)
			}
		})

	metric("corebank_raft_cluster_size",
		"Servers in the configuration this node is operating under. A node disagreeing "+
			"with its peers here has been left behind by a reconfiguration.", "gauge",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_cluster_size%s %d\n",
					lbl(sh), len(sh.Raft.ConfigServers))
			}
		})

	metric("corebank_raft_config_failures_total",
		"Malformed configuration entries seen. Non-zero means this node may be "+
			"operating under the wrong membership, which decides who counts toward quorum.",
		"counter",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_raft_config_failures_total%s %d\n",
					lbl(sh), sh.Raft.ConfigFailures)
			}
		})

	metric("corebank_snapshot_failures_total",
		"Failed snapshot operations. A node that cannot compact will hit the write-amplification wall.",
		"counter",
		func() {
			for _, sh := range shards {
				fmt.Fprintf(&b, "corebank_snapshot_failures_total%s %d\n",
					lbl(sh), sh.Raft.SnapshotFailures)
			}
		})

	if as, ok := s.src.(AdmissionSource); ok {
		adm := as.Admission()
		if adm.Available {
			nl := fmt.Sprintf("{node=%q}", node)

			metric("corebank_admission_in_flight",
				"Requests currently admitted and not yet finished.", "gauge", func() {
					fmt.Fprintf(&b, "corebank_admission_in_flight%s %d\n", nl, adm.InFlight)
				})
			metric("corebank_admission_admitted_total",
				"Requests admitted since start.", "counter", func() {
					fmt.Fprintf(&b, "corebank_admission_admitted_total%s %d\n", nl, adm.Admitted)
				})
			metric("corebank_admission_shed_busy_total",
				"Requests shed because the node was at its in-flight limit. Shedding must "+
					"be visible: a system that sheds silently looks identical to one that is "+
					"merely slow.", "counter", func() {
					fmt.Fprintf(&b, "corebank_admission_shed_busy_total%s %d\n", nl, adm.ShedBusy)
				})
			metric("corebank_admission_shed_rate_total",
				"Requests shed because a client exceeded its rate budget.", "counter", func() {
					fmt.Fprintf(&b, "corebank_admission_shed_rate_total%s %d\n", nl, adm.ShedRate)
				})
			metric("corebank_admission_draining",
				"1 while this node is draining for shutdown and refusing new work.", "gauge",
				func() {
					v := 0
					if adm.Draining {
						v = 1
					}
					fmt.Fprintf(&b, "corebank_admission_draining%s %d\n", nl, v)
				})
		}
	}

	metric("corebank_uptime_seconds", "Seconds since this process started serving metrics.", "gauge",
		func() {
			fmt.Fprintf(&b, "corebank_uptime_seconds{node=\"%s\"} %d\n",
				node, int(time.Since(s.started).Seconds()))
		})

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(b.String()))
}
