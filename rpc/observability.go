package rpc

import (
	"time"

	"github.com/homura/core-bank/obs"
)

// Bridging a hosted set of shard replicas to the observability endpoints (G5).
//
// obs depends on neither rpc nor shard — the dependency runs one way — so the
// adapter lives here, where both are already in scope.

// HostSource adapts a ShardHost to obs.Source.
type HostSource struct {
	nodeID string
	host   *ShardHost

	// readinessWindow bounds how stale a leader's peer contact may be before it
	// stops counting toward quorum. At least a heartbeat interval (or a healthy
	// leader flaps between ready and not) and well under the election timeout (or
	// readiness outlives the leadership it describes).
	readinessWindow time.Duration

	// admit reports backpressure state, when one is attached.
	admit *Admitter
}

// NewHostSource builds an obs.Source over a ShardHost.
func NewHostSource(nodeID string, host *ShardHost, readinessWindow time.Duration) *HostSource {
	return &HostSource{nodeID: nodeID, host: host, readinessWindow: readinessWindow}
}

// NodeID implements obs.Source.
func (h *HostSource) NodeID() string { return h.nodeID }

// Snapshot implements obs.Source.
func (h *HostSource) Snapshot() []obs.ShardHealth {
	ids := h.host.ShardIDs()
	out := make([]obs.ShardHealth, 0, len(ids))

	for _, sid := range ids {
		rep, ok := h.host.Replica(sid)
		if !ok {
			continue
		}
		out = append(out, obs.ShardHealth{
			ShardID:  string(sid),
			Raft:     rep.Raft.Health(h.readinessWindow),
			InDoubt:  len(rep.Machine.InDoubt()),
			Accounts: len(rep.Machine.State.Balances()),
		})
	}
	return out
}

// Compile-time check that a ShardHost really can be observed.
var _ obs.Source = (*HostSource)(nil)
