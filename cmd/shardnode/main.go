// Command shardnode runs one node of a SHARDED core-bank cluster.
//
// node/ runs a single Raft group. This binary hosts one or more shard replicas in
// one process, multiplexed over a single listener and a single connection per
// peer (learn/READING_LIST.md §14). It is what makes Phase 2's sharding and 2PC
// real rather than in-process only: until now shard.Machine was correct and
// durable but no production binary ever instantiated one.
//
// Every hosted shard gets its own Raft server, its own state machine, and its own
// WAL and applied-index files. Replicas share nothing but the transport — the
// same discipline node/ follows, extended to several groups per process.
//
// Usage:
//
//	shardnode -id n1 -listen 127.0.0.1:9001 \
//	          -peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 \
//	          -shards shard-0,shard-1 -hosts shard-0,shard-1 \
//	          -data ./data
//
// With mutual TLS and client authentication:
//
//	shardnode ... -tls-cert node.crt -tls-key node.key -tls-ca ca.crt \
//	          -client-token "$CORE_BANK_TOKEN"
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
	"github.com/homura/core-bank/shard"
	"github.com/homura/core-bank/storage"
)

func main() {
	var (
		id        = flag.String("id", "", "this node's id (must appear in -peers)")
		listen    = flag.String("listen", "", "address to listen on (host:port)")
		peerList  = flag.String("peers", "", "comma-separated id=host:port list, including this node")
		shardList = flag.String("shards", "", "every shard in the cluster, comma-separated")
		hostList  = flag.String("hosts", "", "shards this node holds a replica of (default: all)")
		dataDir   = flag.String("data", "./data", "directory for per-shard WALs")
		seed      = flag.Int64("seed", 0, "random seed for election timers (0 = derive from id)")

		rpcTimeout  = flag.Duration("rpc-timeout", 100*time.Millisecond, "per-RPC timeout; must be < election-min")
		electionMin = flag.Duration("election-min", 150*time.Millisecond, "minimum election timeout")
		electionMax = flag.Duration("election-max", 300*time.Millisecond, "maximum election timeout")
		heartbeat   = flag.Duration("heartbeat", 50*time.Millisecond, "leader heartbeat interval")
		allowSolo   = flag.Bool("allow-single-node", false, "permit a single-node cluster (no fault tolerance)")

		// Log compaction (§7). RaftState.Save rewrites the whole state on every
		// persist, so an uncompacted log costs O(n) bytes per append — measured at
		// 481x write amplification by 800 entries. 0 disables compaction, which is
		// correct but unbounded.
		snapshotEvery = flag.Int("snapshot-threshold", 1000,
			"compact the log once it exceeds this many entries (0 disables)")
		snapshotCheck = flag.Duration("snapshot-interval", 30*time.Second,
			"how often to consider compacting")

		tlsCert     = flag.String("tls-cert", "", "this node's TLS certificate (PEM)")
		tlsKey      = flag.String("tls-key", "", "this node's TLS private key (PEM)")
		tlsCA       = flag.String("tls-ca", "", "CA certificate that signs every node's certificate (PEM)")
		clientToken = flag.String("client-token", "", "bearer token required from clients (empty disables client auth)")

		// Observability (G5). A separate port from the RPC one, deliberately: an
		// endpoint sharing the consensus port cannot be scraped while the consensus
		// path is saturated, which is exactly when the numbers are needed.
		obsAddr   = flag.String("obs-listen", "", "address for /metrics, /healthz, /readyz, /status (empty disables)")
		logFormat = flag.String("log-format", "text", "log encoding: text or json")
		logLevel  = flag.String("log-level", "info", "log level: debug, info, warn, error")

		// Admission control (G7). A bounded queue that REJECTS is more available
		// than an unbounded one that accepts everything: the unbounded version does
		// not remove the capacity limit, it converts a visible rejection into
		// invisible latency and eventually into timeouts on writes that may still
		// commit.
		maxInFlight    = flag.Int("max-in-flight", 0, "bound on concurrent proposals (0 disables)")
		perClientRate  = flag.Float64("client-rate", 0, "sustained requests/sec per client (0 disables)")
		perClientBurst = flag.Int("client-burst", 0, "burst size per client (0 disables)")
		drainTimeout   = flag.Duration("drain-timeout", 5*time.Second,
			"how long to let in-flight requests finish on shutdown")
	)
	flag.Parse()

	if *id == "" || *listen == "" || *peerList == "" || *shardList == "" {
		flag.Usage()
		os.Exit(2)
	}

	logger := obs.NewLogger(*id, obs.ParseLogFormat(*logFormat), obs.ParseLogLevel(*logLevel))

	addrs, ids, err := rpc.ParsePeers(*peerList)
	if err != nil {
		log.Fatalf("shardnode: %v", err)
	}
	if _, ok := addrs[raft.NodeID(*id)]; !ok {
		log.Fatalf("shardnode: -id %q is not in -peers", *id)
	}
	if len(ids) < 3 && !*allowSolo {
		log.Fatalf("shardnode: %d peer(s) configured; a cluster needs at least 3 to tolerate "+
			"a single failure. Pass -allow-single-node to override for local testing.", len(ids))
	}

	// §5.2: broadcastTime << electionTimeout << MTBF. If an RPC can outlive the
	// election timeout, followers start elections while the leader is still
	// waiting on one call, and leadership churns under mild degradation.
	if *rpcTimeout >= *electionMin {
		log.Fatalf("shardnode: -rpc-timeout (%v) must be well below -election-min (%v)", *rpcTimeout, *electionMin)
	}
	if *electionMin >= *electionMax {
		log.Fatalf("shardnode: -election-min (%v) must be below -election-max (%v)", *electionMin, *electionMax)
	}
	if *heartbeat >= *electionMin {
		log.Fatalf("shardnode: -heartbeat (%v) must be well below -election-min (%v)", *heartbeat, *electionMin)
	}

	allShards, err := rpc.ParseShardAssignment(*shardList)
	if err != nil {
		log.Fatalf("shardnode: -shards: %v", err)
	}
	hosted := allShards
	if *hostList != "" {
		hosted, err = rpc.ParseShardAssignment(*hostList)
		if err != nil {
			log.Fatalf("shardnode: -hosts: %v", err)
		}
	}
	// A node that believes it hosts a shard the cluster does not have is the same
	// class of error as a duplicate node id in -peers: the ring would route
	// accounts to a group that exists nowhere, so it is rejected at startup.
	known := make(map[shard.ID]struct{}, len(allShards))
	for _, s := range allShards {
		known[s] = struct{}{}
	}
	for _, s := range hosted {
		if _, ok := known[s]; !ok {
			log.Fatalf("shardnode: -hosts names %q, which is not in -shards", s)
		}
	}

	tlsCfg := rpc.TLSConfig{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: *tlsCA}
	if err := tlsCfg.Validate(); err != nil {
		log.Fatalf("shardnode: %v", err)
	}
	if !tlsCfg.Enabled() {
		// Loud, because a bank running plaintext must never be able to claim it did
		// not know. Raft assumes non-Byzantine participants (§2); an open port makes
		// that assumption false, and a forged AppendEntries at a high term takes the
		// cluster over by the protocol's own rules.
		logger.Warn("TLS is disabled: both ports are unauthenticated plaintext, so "+
			"anyone who can reach them can read every balance and inject AppendEntries",
			"fix", "-tls-cert/-tls-key/-tls-ca")
	}
	if *clientToken == "" {
		logger.Warn("client authentication is disabled", "fix", "-client-token")
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("shardnode: create data dir: %v", err)
	}

	s := *seed
	if s == 0 {
		for _, ch := range *id {
			s = s*31 + int64(ch)
		}
	}

	// One transport for the whole process. Every shard dials through the SAME
	// connection pool, so N hosted shards cost one connection per peer, not N.
	transport := rpc.NewTransportSecure(addrs, *rpcTimeout, tlsCfg)
	defer transport.Close()

	cfg := raft.Config{
		ElectionTimeoutMin: electionMin.Milliseconds(),
		ElectionTimeoutMax: electionMax.Milliseconds(),
		HeartbeatInterval:  heartbeat.Milliseconds(),
	}

	ring := shard.NewRing(allShards, shard.DefaultVNodes)

	var replicas []*rpc.Replica
	var appliedFiles []*storage.AppliedFile

	for i, sid := range hosted {
		// Per-shard storage. Two shards in one process share no files, exactly as
		// two nodes share none: the WAL is what makes a shard's promises durable,
		// and mixing them would make one shard's compaction another shard's problem.
		base := filepath.Join(*dataDir, *id+"-"+string(sid))
		appliedFile, err := storage.OpenApplied(base + ".applied")
		if err != nil {
			log.Fatalf("shardnode: open applied index for %s: %v", sid, err)
		}
		appliedFiles = append(appliedFiles, appliedFile)

		machine := shard.NewMachine(sid, ledger.New())
		srv := raft.NewServerWith(raft.NodeID(*id), ids, machine,
			transport.ForShard(string(sid)), cfg, s+int64(i)*7919)
		srv.SetStorage(storage.OpenRaftState(base+".wal", appliedFile))

		// Replay this shard's log, so a restarted node comes back with its balances
		// AND its 2PC promises intact rather than empty. A participant that voted
		// YES made an unretractable promise; forgetting it across a restart would
		// free funds it has already committed to another transaction.
		if err := srv.Restore(); err != nil {
			log.Fatalf("shardnode: restore %s: %v", sid, err)
		}

		replicas = append(replicas, &rpc.Replica{ShardID: sid, Raft: srv, Machine: machine})
	}
	defer func() {
		for _, f := range appliedFiles {
			f.Close()
		}
	}()

	groups := make(map[shard.ID]shard.Group, len(replicas))
	for _, rep := range replicas {
		groups[rep.ShardID] = rpc.NewNetworkGroup(rep.ShardID, rep)
	}
	coord := shard.NewCoordinator(ring, groups)

	clientAPI := rpc.NewShardClientService(*id, ring, coord, *clientToken, addrs)
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
	host, err := rpc.RegisterShards(*listen, replicas, transport, clientAPI, tlsCfg)
	if err != nil {
		log.Fatalf("shardnode: listen: %v", err)
	}
	clientAPI.Attach(host, tlsCfg.Enabled())
	defer host.Stop()

	host.Start()

	// Observability endpoints. readinessWindow is two heartbeats: long enough that
	// a healthy leader does not flap between ready and not, short enough to stay
	// well inside the election timeout so readiness cannot outlive the leadership
	// it describes.
	var obsServer *obs.Server
	if *obsAddr != "" {
		src := rpc.NewHostSource(*id, host, 2*(*heartbeat))
		src.SetAdmitter(clientAPI.Admitter())
		obsServer, err = obs.Listen(*obsAddr, src)
		if err != nil {
			log.Fatalf("shardnode: observability listen: %v", err)
		}
		defer obsServer.Close()
		logger.Info("observability endpoints listening",
			"addr", obsServer.Addr(),
			"paths", "/metrics /healthz /readyz /status")
	} else {
		logger.Warn("observability is disabled: a cluster committing against a " +
			"degraded quorum will look identical to a healthy one from outside")
	}

	logger.Info("shardnode listening",
		"addr", host.Addr(),
		"peers", len(ids),
		"shards", host.ShardIDs(),
		"tls", tlsCfg.Enabled(),
		"client_auth", *clientToken != "")

	// Resolve in-doubt transactions on becoming a leader.
	//
	// Without this, a real in-doubt transaction blocks until a human runs recovery
	// by hand — the Phase 2 tests called RecoverInDoubt() explicitly, which is fine
	// for a test and useless in production. A new leader of any group is exactly
	// the process that holds the durable decision, so it is the one that must act.
	recoverDone := make(chan struct{})
	go recoverOnLeadership(logger, coord, replicas, recoverDone)

	compactDone := make(chan struct{})
	if *snapshotEvery > 0 {
		go compactPeriodically(logger, replicas, *snapshotEvery, *snapshotCheck, compactDone)
	} else {
		logger.Warn("log compaction is disabled: the state file is rewritten in full " +
			"on every persist, so write cost grows with log length without bound")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	logger.Info("shutting down")
	close(recoverDone)
	close(compactDone)

	// Ordered shutdown (G7). Each step exists for a specific failure:
	//
	//  1. Stop admitting, and let in-flight requests finish — a request that
	//     completes here gets a real answer instead of a dropped connection.
	//  2. Give up leadership, so clients are redirected to a node that can serve
	//     them rather than waiting on one that is going away.
	//  3. Close the listener and connections.
	//
	// Doing 2 before 1 would strand proposals this node is still the only one able
	// to commit; doing 3 before 1 drops answers on the floor, which is exactly what
	// draining exists to avoid.
	if remaining := clientAPI.Admitter().Drain(*drainTimeout); remaining > 0 {
		logger.Warn("shut down with requests still in flight; those clients will see "+
			"a dropped connection and cannot tell whether their write committed",
			"in_flight", remaining)
	} else {
		logger.Info("drained cleanly")
	}

	// Leadership is given up before the listener closes, so clients are redirected
	// rather than left waiting on a node that is going away.
	host.Stop()
}

// compactPeriodically compacts each hosted shard's log once it grows past the
// threshold.
//
// Runs on a timer but decides on SIZE: the cost compaction exists to bound is a
// function of log length, not of elapsed time. The timer only controls how often
// the question is asked, so a busy shard is checked at the same cadence as an
// idle one and neither is checked in a hot loop.
func compactPeriodically(logger *slog.Logger, replicas []*rpc.Replica, threshold int, every time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			for _, rep := range replicas {
				did, err := rep.Raft.MaybeCompact(threshold)
				if err != nil {
					// Not fatal: the log is still authoritative and the next attempt can
					// succeed. But it must be visible — a node that cannot compact will
					// eventually hit the wall compaction exists to prevent.
					obs.ForShard(logger, string(rep.ShardID)).Error("compaction failed", "err", err)
					continue
				}
				if did {
					idx, term, _ := rep.Raft.SnapshotInfo()
					obs.ForShard(logger, string(rep.ShardID)).Info("compacted log",
						"through_index", uint64(idx), "term", uint64(term))
				}
			}
		}
	}
}

// recoverOnLeadership resolves in-doubt transactions whenever this node becomes
// leader of a shard it hosts.
//
// It runs on leadership CHANGE rather than on a timer: recovery is only
// meaningful for a leader, and re-running it on every tick would put steady load
// on the same mutex the consensus loop uses.
func recoverOnLeadership(logger *slog.Logger, coord *shard.Coordinator, replicas []*rpc.Replica, done <-chan struct{}) {
	wasLeader := make(map[shard.ID]bool, len(replicas))
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			for _, rep := range replicas {
				isLeader := rep.Raft.Role() == raft.Leader
				if isLeader && !wasLeader[rep.ShardID] {
					sl := obs.ForShard(logger, string(rep.ShardID))
					sl.Info("became leader; running in-doubt recovery",
						"term", uint64(rep.Raft.CurrentTerm()))
					if n, err := coord.RecoverInDoubt(); err != nil {
						sl.Warn("in-doubt recovery incomplete", "err", err)
					} else if n > 0 {
						sl.Info("in-doubt recovery resolved transactions", "count", n)
					}
				}
				wasLeader[rep.ShardID] = isLeader
			}
		}
	}
}
