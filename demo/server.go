package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// The demo's HTTP surface: an SSE stream plus one-shot control commands.
//
// SAFETY BOUNDARY, restated because it is the important part of this file: these
// endpoints kill nodes and move money with no authentication. This is a separate
// binary that must be started deliberately, and nothing here is reachable from a
// production node.

// Server serves the UI and the control plane.
type Server struct {
	cluster  *Cluster
	http     *http.Server
	listener net.Listener
}

// Listen starts the demo server on addr, serving static files from dir.
func Listen(addr, dir string, c *Cluster) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("demo: listen %s: %w", addr, err)
	}

	s := &Server{cluster: c, listener: l}

	mux := http.NewServeMux()
	mux.Handle("/", noCache(http.FileServer(http.Dir(dir))))
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/open", s.handleOpen)
	mux.HandleFunc("/api/tx", s.handleTx)
	mux.HandleFunc("/api/kill", s.handleKill)
	mux.HandleFunc("/api/revive", s.handleRevive)
	mux.HandleFunc("/api/recover", s.handleRecover)
	mux.HandleFunc("/api/resize", s.handleResize)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/heal", s.handleHeal)
	mux.HandleFunc("/api/underreplicated", s.handleUnderReplicated)
	mux.HandleFunc("/api/events", s.handleEvents)

	s.http = &http.Server{
		Handler: mux,
		// No WriteTimeout: an SSE response is deliberately never finished, and a
		// write deadline would sever the stream on a fixed interval. The read
		// header timeout still bounds a client that connects and says nothing.
		ReadHeaderTimeout: 5 * time.Second,
	}
	go s.http.Serve(l)
	return s, nil
}

// Addr returns the bound address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close stops the server.
func (s *Server) Close() error { return s.http.Close() }

// handleStream pushes the cluster view as Server-Sent Events.
//
// A complete snapshot per frame rather than a delta. That is what makes it safe
// to skip a frame for a slow consumer: a client that misses three and receives
// the fourth is fully current, so the server never has to buffer without bound
// for a backgrounded tab (§20, and the same argument as §18's bounded queue).
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// 100ms is chosen against what it needs to show, not by taste: a candidate
	// state lasts one election timeout (60-120ms in the simulator), so a slower
	// tick would render elections as instantaneous leader swaps and hide the
	// mechanism the view exists to reveal.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			// The browser navigated away or closed the tab. Ending here is what
			// stops an abandoned stream from being served forever.
			return
		case <-ticker.C:
			data, err := json.Marshal(s.cluster.Snapshot())
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// handleState returns one snapshot, for a client that cannot stream.
func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.cluster.Snapshot())
}

// handleOpen creates an account.
func (s *Server) handleOpen(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	amount, _ := strconv.ParseInt(r.URL.Query().Get("amount"), 10, 64)

	if account == "" {
		writeJSON(w, map[string]any{"ok": false, "err": "account is required"})
		return
	}
	res, err := s.cluster.Open(ledger.AccountID(account), ledger.Money(amount))
	writeJSON(w, replyOf(res, err))
}

// handleTx performs a deposit, withdrawal, or transfer.
func (s *Server) handleTx(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	amount, _ := strconv.ParseInt(q.Get("amount"), 10, 64)

	res, err := s.cluster.Transact(
		q.Get("op"), q.Get("key"),
		ledger.AccountID(q.Get("from")), ledger.AccountID(q.Get("to")),
		ledger.Money(amount))
	writeJSON(w, replyOf(res, err))
}

// handleKill takes a node off the network.
//
// The fault the operator chose, which is Chaos Engineering's principle with a
// human in the loop: the value is in watching the steady-state hypothesis hold.
func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	node := r.URL.Query().Get("node")
	if err := s.cluster.Kill(raft.NodeID(node)); err != nil {
		writeJSON(w, map[string]any{"ok": false, "err": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// handleRevive brings a killed node back.
func (s *Server) handleRevive(w http.ResponseWriter, r *http.Request) {
	node := r.URL.Query().Get("node")
	if err := s.cluster.Revive(raft.NodeID(node)); err != nil {
		writeJSON(w, map[string]any{"ok": false, "err": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// handleRecover resolves in-doubt 2PC transactions.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	n, err := s.cluster.RecoverInDoubt()
	out := map[string]any{"ok": err == nil, "resolved": n}
	if err != nil {
		out["err"] = err.Error()
	}
	writeJSON(w, out)
}

// handleResize rebuilds the cluster with a new shape.
//
// A teaching control, not a production one — see demo/resize.go for why adding a
// machine to a LIVE sharded cluster is live resharding rather than wiring.
func (s *Server) handleResize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	shards, _ := strconv.Atoi(q.Get("shards"))
	nodes, _ := strconv.Atoi(q.Get("nodes"))
	rf, _ := strconv.Atoi(q.Get("rf"))

	cur := func() (int, int, int) { return s.cluster.Topology() }
	cs, cn, crf := cur()
	if shards == 0 {
		shards = cs
	}
	if nodes == 0 {
		nodes = cn
	}
	if rf == 0 {
		rf = crf
	}

	if err := s.cluster.Resize(shards, nodes, rf); err != nil {
		writeJSON(w, map[string]any{"ok": false, "err": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "shards": shards, "nodes": nodes, "rf": rf})
}

// handleHealth sets or unlocks a machine's simulated health.
//
// Health biases how eagerly a machine campaigns; it never changes who is allowed
// to win. See demo/health.go and raft/health_priority.go.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	node := raft.NodeID(q.Get("node"))

	if q.Get("unlock") == "1" {
		if err := s.cluster.UnlockHealth(node); err != nil {
			writeJSON(w, map[string]any{"ok": false, "err": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"ok": true, "locked": false})
		return
	}

	var level raft.NodeHealth
	switch q.Get("level") {
	case "low":
		level = raft.HealthLow
	case "high":
		level = raft.HealthHigh
	case "normal":
		level = raft.HealthNormal
	default:
		writeJSON(w, map[string]any{"ok": false, "err": "level must be low, normal or high"})
		return
	}

	// Setting a level pins it by default: an operator who picks a value expects it
	// to stay, and having it drift away seconds later would look like the control
	// did not work.
	lock := q.Get("lock") != "0"
	if err := s.cluster.SetHealth(node, level, lock); err != nil {
		writeJSON(w, map[string]any{"ok": false, "err": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "level": level.String(), "locked": lock})
}

// replyOf maps a ledger result and error onto the client contract the UI shows.
//
// The four outcomes are preserved exactly, because collapsing them is the failure
// this project has guarded against throughout: Indeterminate means the entry may
// yet commit and must be retried with the SAME key, while an ordinary failure
// means it did not happen. A UI that shows both as "failed" teaches the wrong
// thing about the system it exists to explain.
func replyOf(res ledger.Result, err error) map[string]any {
	out := map[string]any{
		"ok":      err == nil && res.OK,
		"balance": int64(res.Balance),
	}
	switch {
	case err == nil:
		if !res.OK {
			out["err"] = res.Err
		}
	// errors.Is, not ==, because ErrCommitUnknown is wrapped with the underlying
	// timeout. A bare == silently falls through to default and reports the very
	// "plain failure" this case exists to prevent.
	case errors.Is(err, shard.ErrInDoubt), errors.Is(err, shard.ErrCommitUnknown):
		out["indeterminate"] = true
		out["err"] = err.Error()
	case err == shard.ErrTxAborted:
		out["aborted"] = true
		if res.Err != "" {
			out["err"] = res.Err
		} else {
			out["err"] = err.Error()
		}
	default:
		out["err"] = err.Error()
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// handleHeal re-replicates every shard that is under-replicated but still has a
// majority. Shards below majority are reported as skipped, with the reason —
// see demo/heal.go for why they must not be "fixed".
func (s *Server) handleHeal(w http.ResponseWriter, r *http.Request) {
	results := s.cluster.Heal()

	healed := 0
	for _, res := range results {
		if res.Added != "" {
			healed++
		}
	}
	writeJSON(w, map[string]any{
		"ok":      true,
		"healed":  healed,
		"results": results,
	})
}

// handleUnderReplicated reports which shards are missing replicas, without
// changing anything.
func (s *Server) handleUnderReplicated(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"ok":     true,
		"shards": s.cluster.UnderReplicatedShards(),
	})
}

// handleEvents returns the structured event log, optionally filtered.
//
// Filters are AND-ed: ?account=Vu&kind=client narrows to that account's own
// operations. An absent filter matches everything, so the default is the full
// stream.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := 200
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}

	events := s.cluster.Events(EventFilter{
		Account:     q.Get("account"),
		Shard:       q.Get("shard"),
		Node:        q.Get("node"),
		Kind:        EventKind(q.Get("kind")),
		ExcludeKind: EventKind(q.Get("exclude_kind")),
	}, limit)

	writeJSON(w, map[string]any{
		"ok":     true,
		"events": events,
		"axes":   s.cluster.EventAxes(),
	})
}

// noCache stops the browser serving a stale copy of the dashboard.
//
// The file server sends only Last-Modified, which lets a browser reuse a cached
// page without revalidating. During development that means edits to the UI appear
// not to take effect: the server is correct, the file on disk is correct, and the
// browser is running yesterday's JavaScript. It cost a full debugging pass here —
// the filter code was proven correct in a headless DOM while the browser kept
// showing the old behaviour.
//
// This is a teaching demo whose files change constantly, so correctness of what
// is displayed matters far more than caching a few kilobytes.
func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		h.ServeHTTP(w, r)
	})
}
