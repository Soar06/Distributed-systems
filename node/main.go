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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/homura/core-bank/ledger"
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
	)
	flag.Parse()

	if *id == "" || *listen == "" || *peerList == "" {
		flag.Usage()
		os.Exit(2)
	}

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
	rpcServer, err := rpc.Listen(*listen, srv, clientAPI)
	if err != nil {
		log.Fatalf("node: listen: %v", err)
	}
	defer rpcServer.Close()

	srv.Start()
	defer srv.Stop()

	log.Printf("node %s listening on %s (peers: %d, wal: %s)",
		*id, rpcServer.Addr(), len(ids), walPath)

	// Report role changes, so watching a terminal shows elections happening.
	roleDone := make(chan struct{})
	go reportRole(srv, roleDone)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("node %s shutting down", *id)
	close(roleDone)

	// Ordered shutdown. The deferred closes run afterwards in LIFO order, but
	// leadership must be given up first so clients are redirected rather than
	// left waiting on a node that is going away.
	srv.Stop()
	rpcServer.Close()
}

// reportRole logs role transitions until done is closed.
//
// It takes the server mutex on every poll, so it must not outlive the process's
// useful life: it was previously an infinite loop with no stop channel, taxing
// the same lock the role loop and every AppendEntries handler contend for.
func reportRole(s *raft.Server, done <-chan struct{}) {
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
				log.Printf("  %s: %v (term %d, commit %d)", s.ID(), role, term, s.CommitIndex())
				last, lastTerm = role, term
			}
		}
	}
}
