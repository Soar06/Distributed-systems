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
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	)
	flag.Parse()

	if *id == "" || *listen == "" || *peerList == "" {
		flag.Usage()
		os.Exit(2)
	}

	addrs, ids, err := parsePeers(*peerList)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	if _, ok := addrs[raft.NodeID(*id)]; !ok {
		log.Fatalf("node: -id %q is not in -peers", *id)
	}

	// Durable state. Each node gets its own file: nodes share no storage, even on
	// one machine.
	walPath := filepath.Join(*dataDir, *id+".wal")
	wal, err := storage.Open(walPath)
	if err != nil {
		log.Fatalf("node: open wal: %v", err)
	}
	defer wal.Close()

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

	transport := rpc.NewTransport(addrs, 500*time.Millisecond)
	defer transport.Close()

	srv := raft.NewServerWith(raft.NodeID(*id), ids, machine, transport, raft.DefaultConfig(), s)
	srv.SetStorage(storage.NewRaftStateWithApplied(wal, appliedFile))

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
	go reportRole(srv)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("node %s shutting down", *id)
}

// parsePeers turns "n1=host:port,n2=host:port" into a map and an id list.
func parsePeers(s string) (map[raft.NodeID]string, []raft.NodeID, error) {
	addrs := make(map[raft.NodeID]string)
	var ids []raft.NodeID

	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, nil, fmt.Errorf("bad peer %q, want id=host:port", part)
		}
		addrs[raft.NodeID(id)] = addr
		ids = append(ids, raft.NodeID(id))
	}
	if len(ids) == 0 {
		return nil, nil, fmt.Errorf("no peers given")
	}
	return addrs, ids, nil
}

// reportRole logs role transitions.
func reportRole(s *raft.Server) {
	last := raft.Role(-1)
	lastTerm := raft.Term(0)
	for {
		role, term := s.Role(), s.CurrentTerm()
		if role != last || term != lastTerm {
			log.Printf("  %s: %v (term %d, commit %d)", s.ID(), role, term, s.CommitIndex())
			last, lastTerm = role, term
		}
		time.Sleep(50 * time.Millisecond)
	}
}
