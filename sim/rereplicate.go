package sim

import (
	"fmt"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
	"github.com/homura/core-bank/shard"
)

// configChangeTimeout bounds how long a heal waits for its configuration entry
// to commit. Generous relative to an election, so a heal that races a leader
// change still succeeds, but finite so a below-majority shard fails rather than
// blocking forever.
const configChangeTimeout = 3 * time.Second

// Adding a replica to a live shard group.
//
// This is the MECHANISM half of re-replication (§21 in the reading list). The
// POLICY half — deciding when a shard needs healing and which machine should
// receive the new replica — lives in demo/heal.go.
//
// The split matters: this file must not decide anything. It builds a replica and
// hands it to Raft's own membership change (§6). Everything that makes healing
// safe is enforced by the caller and by Raft itself, not here.

// AddReplica creates a replica of this shard on machine nid and adds it to the
// group's Raft configuration.
//
// The new replica starts EMPTY. It is populated entirely by the ordinary
// catch-up path — AppendEntries from the leader, or InstallSnapshot when the
// leader has already compacted past what the follower needs (§7). No state is
// copied out of band, which is what keeps the new replica's log identical to
// every other replica's rather than merely similar.
//
// Requires a leader, and therefore a surviving majority. That is not a check
// this function performs as a courtesy — it is structural. AddServer is
// leader-only (raft/membership.go), and a leader cannot exist without a
// majority, so a shard that has lost quorum physically cannot be healed. See
// §21: healing a below-majority shard would mean inventing empty state and
// silently zeroing balances.
func (sc *ShardCluster) AddReplica(sid shard.ID, nid raft.NodeID) error {
	g, ok := sc.Groups[sid]
	if !ok {
		return fmt.Errorf("sim: unknown shard %s", sid)
	}
	g.mu.RLock()
	_, exists := g.Nodes[nid]
	g.mu.RUnlock()
	if exists {
		return fmt.Errorf("sim: %s already holds a replica of %s", nid, sid)
	}

	leaderID := g.leader()
	if leaderID == "" {
		// No leader means no majority, means nothing to copy from.
		return fmt.Errorf(
			"sim: shard %s has no leader; a shard below quorum cannot be re-replicated "+
				"because there is no majority to copy committed entries from", sid)
	}

	ids, nodes, _ := g.snapshot()
	leader := nodes[leaderID]
	if leader == nil {
		return fmt.Errorf("sim: shard %s leader %s vanished mid-heal", sid, leaderID)
	}

	// Build the replica before touching the configuration, so a construction
	// failure cannot leave a config entry pointing at a server that does not
	// exist. Its peer set is the CURRENT holders plus itself.
	peers := append(append([]raft.NodeID(nil), ids...), nid)

	machine := shard.NewMachine(sid, ledger.New())
	srv := raft.NewServerWith(nid, peers, machine,
		g.Net.ForGroup(sid), simConfig(), sc.replicaSeed(sid, nid))

	g.Net.RegisterGroup(sid, nid, srv)
	srv.Start()

	// Now the §6 configuration change. It is an ordinary log entry replicated
	// through the leader, which is why this needed a leader in the first place.
	idx, err := leader.AddServer(nid)
	if err != nil {
		// Roll back the half-built replica: it is not in any configuration, so
		// nothing references it, but leaving it running would have it campaigning
		// against a cluster that does not know it.
		srv.Stop()
		return fmt.Errorf("sim: adding %s to %s: %w", nid, sid, err)
	}

	// WAIT FOR THE CHANGE TO COMMIT. This is the load-bearing step, and omitting
	// it was a real bug caught by TestBelowMajorityShardIsRefusedNotFabricated.
	//
	// AddServer returns once the entry is APPENDED to the leader's log, which a
	// partitioned leader will happily do: Role() is its own local view, and an
	// isolated node goes on believing it leads until it hears a higher term. So
	// "there is a leader" is a liveness hint, never a safety guarantee — checking
	// it alone let a shard with two of three replicas dead accept a new replica,
	// which is precisely the invented-state case §21 forbids.
	//
	// Commitment is the real test. A config entry commits only when a MAJORITY of
	// the shard has it, so waiting here converts a hopeful append into proof that
	// a genuine quorum accepted the change. Below majority it simply never
	// arrives, and the heal fails honestly.
	select {
	case <-leader.WaitApplied(idx):
	case <-time.After(configChangeTimeout):
		srv.Stop()
		return fmt.Errorf(
			"sim: adding %s to %s: configuration change did not commit within %v; "+
				"the shard has no majority to accept it", nid, sid, configChangeTimeout)
	}

	g.mu.Lock()
	g.Nodes[nid] = srv
	g.SMs[nid] = machine
	g.IDs = peers
	g.mu.Unlock()

	// Deliberately NOT poking the other replicas here. A follower learns about a
	// membership change the same way it learns about everything else: the config
	// entry arrives in the log and adoptConfigFromLogLocked picks it up
	// (raft/membership.go). Telling each peer directly would be a side channel
	// around the exact mechanism §6 specifies, and it would diverge the moment a
	// follower was partitioned during the change.
	return nil
}

// RemoveReplica drops a replica from the group's Raft configuration.
//
// Used to retire a permanently dead machine AFTER its replacement has been added
// and caught up. The order is deliberate — see demo/heal.go.
func (sc *ShardCluster) RemoveReplica(sid shard.ID, nid raft.NodeID) error {
	g, ok := sc.Groups[sid]
	if !ok {
		return fmt.Errorf("sim: unknown shard %s", sid)
	}
	g.mu.RLock()
	_, exists := g.Nodes[nid]
	g.mu.RUnlock()
	if !exists {
		return fmt.Errorf("sim: %s holds no replica of %s", nid, sid)
	}

	leaderID := g.leader()
	if leaderID == "" {
		return fmt.Errorf("sim: shard %s has no leader; cannot change configuration", sid)
	}
	_, nodes, _ := g.snapshot()
	leader := nodes[leaderID]
	if leader == nil {
		return fmt.Errorf("sim: shard %s leader %s vanished mid-change", sid, leaderID)
	}

	idx, err := leader.RemoveServer(nid)
	if err != nil {
		return fmt.Errorf("sim: removing %s from %s: %w", nid, sid, err)
	}

	// Same reasoning as AddReplica: append is not commitment. Retiring a replica
	// on the word of a phantom leader would shrink a configuration that the real
	// majority never agreed to shrink.
	select {
	case <-leader.WaitApplied(idx):
	case <-time.After(configChangeTimeout):
		return fmt.Errorf(
			"sim: removing %s from %s: configuration change did not commit within %v",
			nid, sid, configChangeTimeout)
	}

	// Stop the removed replica only after the change is proposed. §6 notes that a
	// removed server may keep receiving RPCs briefly; stopping it early would make
	// it look like a network failure to a leader that is still finishing the
	// change.
	g.mu.RLock()
	victim := g.Nodes[nid]
	g.mu.RUnlock()
	if victim != nil {
		victim.Stop()
	}
	g.mu.Lock()
	delete(g.Nodes, nid)
	delete(g.SMs, nid)
	g.IDs = withoutNode(g.IDs, nid)
	g.mu.Unlock()

	// As in AddReplica: the surviving replicas adopt the shrunk configuration from
	// the log, not from a direct call.
	return nil
}

// Holders returns the machines currently holding a replica of this shard.
func (sc *ShardCluster) Holders(sid shard.ID) []raft.NodeID {
	g, ok := sc.Groups[sid]
	if !ok {
		return nil
	}
	ids, _, _ := g.snapshot()
	return ids
}

// replicaSeed derives a deterministic seed for a newly added replica.
//
// Distinct from every seed used at construction, so a healed replica does not
// share an election-timer sequence with an existing one — two replicas drawing
// identical timeouts would defeat the randomization §5.2 relies on to break
// split votes.
func (sc *ShardCluster) replicaSeed(sid shard.ID, nid raft.NodeID) int64 {
	var h int64 = 1469598103934665603
	for _, b := range []byte(string(sid) + "|" + string(nid)) {
		h = (h ^ int64(b)) * 1099511628211
	}
	if h < 0 {
		h = -h
	}
	return h
}

func withoutNode(ids []raft.NodeID, drop raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(ids))
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}
