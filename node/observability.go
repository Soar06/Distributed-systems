package main

import (
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/obs"
	"github.com/homura/core-bank/raft"
)

// singleGroupSource adapts a single-Raft-group node to obs.Source (G5).
//
// The single-group node reports one "shard" named default, so the same metric
// names, the same /readyz semantics, and the same dashboard work for both
// binaries. A separate metric namespace for the single-group case would mean
// every query and every alert had to be written twice.
type singleGroupSource struct {
	nodeID          string
	srv             *raft.Server
	state           *ledger.State
	readinessWindow time.Duration
}

func newSingleGroupSource(nodeID string, srv *raft.Server, st *ledger.State,
	readinessWindow time.Duration) *singleGroupSource {
	return &singleGroupSource{nodeID: nodeID, srv: srv, state: st, readinessWindow: readinessWindow}
}

func (s *singleGroupSource) NodeID() string { return s.nodeID }

func (s *singleGroupSource) Snapshot() []obs.ShardHealth {
	return []obs.ShardHealth{{
		ShardID: "default",
		Raft:    s.srv.Health(s.readinessWindow),
		// A single Raft group runs no 2PC, so there is nothing that can be in
		// doubt. Reported as 0 rather than omitted, so a dashboard does not have to
		// special-case which binary it is looking at.
		InDoubt:  0,
		Accounts: len(s.state.Balances()),
	}}
}

var _ obs.Source = (*singleGroupSource)(nil)
