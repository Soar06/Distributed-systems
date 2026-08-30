// Command node runs one core-bank cluster node.
//
// One process = one node. Nodes share nothing but the network: separate memory,
// separate WAL files, communicating only over RPC. On one dev machine that
// discipline is self-imposed; on real hardware it is enforced by physics. The
// binary does not change between the two — only the peer list does (LATER.md).
//
// Usage:
//
//	node -id n1 -listen 127.0.0.1:9001 \
//	     -peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 \
//	     -data ./data
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/obs"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/rpc"
	"github.com/homura/core-bank/storage"
)

func main() {
	var (
		id       = flag.String("id", "", "this node's id (must appear in -peers)")
		listen   = flag.String("listen", "", "address to listen on (host:port)")
		peerList = flag.String("peers", "", "comma-separated id=host:port list, including this node")
		dataDir  = flag.String("data", "./data", "directory for the write-ahead log")
		seed     = flag.Int64("seed", 0, "random seed for election timers (0 = derive from id)")

		// Timing must be tunable: a WAN deployment cannot use LAN defaults, and
		// the RPC timeout must stay well under the election timeout (§5.2's
		// broadcastTime << electionTimeout).
		rpcTimeout  = flag.Duration("rpc-timeout", 100*time.Millisecond, "per-RPC timeout; must be < election-min")
		electionMin = flag.Duration("election-min", 150*time.Millisecond, "minimum election timeout")
		electionMax = flag.Duration("election-max", 300*time.Millisecond, "maximum election timeout")
		heartbeat   = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		allowSolo   = flag.Bool("allow-single-node", false, "permit a single-node cluster (no fault tolerance)")

		// Log compaction (§7). Without it the state file is rewritten in full on
		// every persist, so write cost grows with log length — measured at 481x
		// write amplification by 800 entries.
		snapshotEvery = flag.Int("snapshot-threshold", 1000,
			"compact the log once it exceeds this many entries (0 disables)")
		snapshotCheck = flag.Duration("snapshot-interval", 30*time.Second,
			"how often to consider compacting")

		// Observability (G5), on its own port so it stays scrapable when the
		// consensus path is saturated.
		obsAddr   = flag.String("obs-listen", "", "address for /metrics, /healthz, /readyz, /status (empty disables)")
		logFormat = flag.String("log-format", "text", "log encoding: text or json")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")

		// Admission control (G7): see cmd/shardnode for the reasoning.
		maxInFlight    = flag.Int("max-in-flight", 0, "bound on concurrent proposals (0 disables)")
		perClientRate  = flag.Float64("client-rate", 0, "sustained requests/sec per client (0 disables)")
		perClientBurst = flag.Int("client-burst", 0, "burst size per client (0 disables)")
		drainTimeout   = flag.Duration("drain-timeout", 5*time.Second,
			"how long to let in-flight requests finish on shutdown")
	)
	flag.Parse()

	if *id == "" || *listen == "" || *peerList == "" {
		flag.Usage()
		os.Exit(2)
	}

	logger := obs.NewLogger(*id, obs.ParseLogFormat(*logFormat), obs.ParseLogLevel(*logLevel))

	addrs, ids, err := rpc.ParsePeers(*peerList)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	if _, ok := addrs[raft.NodeID(*id)]; !ok {
		log.Fatalf("node: -id %q is not in -peers", *id)
	}
	if len(ids) < 3 && !*allowSolo {
		log.Fatalf("node: %d peer(s) configured; a cluster needs at least 3 to tolerate "+
			"a single failure. Pass -allow-single-node to override for local testing.", len(ids))
	}

	// §5.2 requires broadcastTime << electionTimeout. If an RPC can outlive the
	// election timeout, followers start elections while the leader is still
	// waiting on a single call — leadership churn under mild degradation.
	if *rpcTimeout >= *electionMin {
		log.Fatalf("node: -rpc-timeout (%v) must be well below -election-min (%v); "+
			"otherwise a single slow RPC triggers spurious elections", *rpcTimeout, *electionMin)
	}
	if *electionMin >= *electionMax {
		log.Fatalf("node: -election-min (%v) must be below -election-max (%v)", *electionMin, *electionMax)
	}
	if *heartbeat >= *electionMin {
		log.Fatalf("node: -heartbeat (%v) must be well below -election-min (%v)", *heartbeat, *electionMin)
	}

	// Durable state. Each node gets its own file: nodes share no storage, even on
	// one machine.
	walPath := filepath.Join(*dataDir, *id+".wal")

	// Records how far the state machine has been applied, so a restart replays the
	// log and comes back with the same ledger rather than an empty one. Without it
	// a restarted node serves reads from nothing until the leader catches it up.
	appliedPath := filepath.Join(*dataDir, *id+".applied")
	appliedFile, err := storage.OpenApplied(appliedPath)
	if err != nil {
		log.Fatalf("node: open applied index: %v", err)
	}
	defer appliedFile.Close()

	// The ledger is the replicated state machine.
	state := ledger.New()
	machine := ledger.NewMachine(state)

	s := *seed
	if s == 0 {
		// Derive a distinct seed per node so election timeouts do not align.
		for _, ch := range *id {
			s = s*31 + int64(ch)
		}
	}

	transport := rpc.NewTransport(addrs, *rpcTimeout)
	defer transport.Close()

	cfg := raft.Config{
		ElectionTimeoutMin: electionMin.Milliseconds(),
		ElectionTimeoutMax: electionMax.Milliseconds(),
		HeartbeatInterval:  heartbeat.Milliseconds(),
	}
	srv := raft.NewServerWith(raft.NodeID(*id), ids, machine, transport, cfg, s)
	srv.SetStorage(storage.OpenRaftState(walPath, appliedFile))

	// Recover anything this node durably knew before it last stopped.
	if err := srv.Restore(); err != nil {
		log.Fatalf("node: restore: %v", err)
	}

	clientAPI := rpc.NewClientService(srv, machine, addrs)
	limits := rpc.Limits{
		MaxInFlight:    *maxInFlight,
		PerClientRate:  *perClientRate,
		PerClientBurst: *perClientBurst,
	}
	clientAPI.SetLimits(limits)
	if !limits.Enabled() {
		logger.Warn("admission control is disabled: an overloaded leader will queue " +
			"without bound, turning rejections into timeouts on writes that may still commit")
	}
	rpcServer, err := rpc.Listen(*listen, srv, clientAPI)
	if err != nil {
		log.Fatalf("node: listen: %v", err)
	}
	defer rpcServer.Close()

	srv.Start()
	defer srv.Stop()

	// readinessWindow is two heartbeats: long enough that a healthy leader does not
	// flap between ready and not, short enough to stay well inside the election
	// timeout so readiness cannot outlive the leadership it describes.
	var obsServer *obs.Server
	if *obsAddr != "" {
		src := newSingleGroupSource(*id, srv, state, 2*(*heartbeat))
		obsServer, err = obs.Listen(*obsAddr, src)
		if err != nil {
			log.Fatalf("node: observability listen: %v", err)
		}
		defer obsServer.Close()
		logger.Info("observability endpoints listening",
			"addr", obsServer.Addr(), "paths", "/metrics /healthz /readyz /status")
	} else {
		logger.Warn("observability is disabled: a cluster committing against a " +
			"degraded quorum will look identical to a healthy one from outside")
	}

	logger.Info("node listening",
		"addr", rpcServer.Addr(), "peers", len(ids), "wal", walPath)

	// Report role changes, so watching a terminal shows elections happening.
	roleDone := make(chan struct{})
	go reportRole(logger, srv, roleDone)

	compactDone := make(chan struct{})
	if *snapshotEvery > 0 {
		go compactPeriodically(logger, srv, *snapshotEvery, *snapshotCheck, compactDone)
	} else {
		logger.Warn("log compaction is disabled: the state file is rewritten in full " +
			"on every persist, so write cost grows with log length without bound")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	logger.Info("shutting down")
	close(roleDone)
	close(compactDone)

	// Ordered shutdown: drain in-flight work, THEN give up leadership, THEN close.
	// See cmd/shardnode for why the order is the whole content.
	if remaining := clientAPI.Admitter().Drain(*drainTimeout); remaining > 0 {
		logger.Warn("shut down with requests still in flight", "in_flight", remaining)
	} else {
		logger.Info("drained cleanly")
	}

	// Ordered shutdown. The deferred closes run afterwards in LIFO order, but
	// leadership must be given up first so clients are redirected rather than
	// left waiting on a node that is going away.
	srv.Stop()
	rpcServer.Close()
}

// compactPeriodically compacts the log once it grows past the threshold.
//
// Runs on a timer but decides on SIZE: the cost compaction bounds is a function
// of log length, not elapsed time. The timer only sets how often the question is
// asked.
func compactPeriodically(logger *slog.Logger, s *raft.Server, threshold int, every time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			did, err := s.MaybeCompact(threshold)
			if err != nil {
				// Not fatal — the log is still authoritative — but not silent either:
				// a node that cannot compact will eventually hit the wall.
				logger.Error("compaction failed", "err", err)
				continue
			}
			if did {
				idx, term, _ := s.SnapshotInfo()
				logger.Info("compacted log", "through_index", uint64(idx), "term", uint64(term))
			}
		}
	}
}

// reportRole logs role transitions until done is closed.
//
// It takes the server mutex on every poll, so it must not outlive the process's
// useful life: it was previously an infinite loop with no stop channel, taxing
// the same lock the role loop and every AppendEntries handler contend for.
func reportRole(logger *slog.Logger, s *raft.Server, done <-chan struct{}) {
	last := raft.Role(-1)
	lastTerm := raft.Term(0)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			role, term := s.Role(), s.CurrentTerm()
			if role != last || term != lastTerm {
				logger.Info("role changed",
					"role", role.String(), "term", uint64(term), "commit", uint64(s.CommitIndex()))
				last, lastTerm = role, term
			}
		}
	}
}
