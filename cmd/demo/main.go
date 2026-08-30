// Command demo runs a live cluster behind a web UI (Phase 4).
//
// One process, an in-memory sharded cluster, and a browser UI that can move money
// and kill nodes while watching the cluster react. Everything this project claims
// has so far been verified only by test assertions; this makes it observable.
//
// SAFETY BOUNDARY: this binary exposes unauthenticated endpoints that kill nodes
// and move money. It exists to be run deliberately on a dev machine and is not
// something a production node serves. That is why it is a separate binary rather
// than a flag on node/ or shardnode/.
//
// Usage:
//
//	go run ./cmd/demo
//	open http://127.0.0.1:8080
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/homura/core-bank/demo"
)

func main() {
	var (
		listen   = flag.String("listen", "127.0.0.1:8080", "address for the demo UI")
		dir      = flag.String("ui", "fe", "directory holding the UI files")
		shards   = flag.Int("shards", 3, "number of shards")
		replicas = flag.Int("replicas", 3, "replicas per shard")
		seed     = flag.Int64("seed", 42, "seed for the simulated network")
		seedDemo = flag.Bool("seed-accounts", true, "open a few accounts at startup")
	)
	flag.Parse()

	if *replicas < 3 {
		log.Printf("WARNING: %d replicas per shard tolerates no failures; killing one "+
			"node will stall that shard rather than demonstrating a failover", *replicas)
	}

	cluster, err := demo.New(*shards, *replicas, *seed)
	if err != nil {
		log.Fatalf("demo: %v", err)
	}
	defer cluster.Stop()

	if *seedDemo {
		// A handful of accounts spread across shards, so the ring view has
		// something to show and a cross-shard transfer is possible immediately.
		if err := cluster.SeedAccounts(); err != nil {
			log.Printf("demo: seeding accounts: %v", err)
		}
	}

	srv, err := demo.Listen(*listen, *dir, cluster)
	if err != nil {
		log.Fatalf("demo: %v", err)
	}
	defer srv.Close()

	log.Printf("demo cluster: %d shards x %d replicas", *shards, *replicas)
	log.Printf("bank app:  http://%s/bank-app/", srv.Addr())
	log.Printf("dashboard: http://%s/cluster-dashboard/", srv.Addr())
	log.Printf("WARNING: the control endpoints are unauthenticated. Dev use only.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Printf("demo: shutting down")
}
