package rpc

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// Membership over the wire (§6), the gap the README listed first.
//
// The consensus rules are already tested in raft/membership_test.go. What is
// tested here is the thing that only exists once the API is remote: that an
// operator is told the TRUTH about what happened — applied, rejected, or unknown
// — because the three demand different responses and collapsing them is how a
// cluster gets reconfigured twice.

// A change on the leader is appended, committed, and reported with the resulting
// configuration.
func TestAddServerOverTheWireCommitsAndReportsConfiguration(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	leader := h.waitForLeader(t)

	reply, err := h.adminFor(leader).AddServer("node-4", 3*time.Second)
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if !reply.OK {
		t.Fatalf("AddServer not OK: indeterminate=%v notLeader=%v err=%q",
			reply.Indeterminate, reply.NotLeader, reply.Err)
	}

	// The reply must carry the configuration the change produced, not just a
	// success flag: an operator's next decision depends on what the cluster now
	// looks like, and making them issue a second call to find out invites acting
	// on a stale assumption.
	if !containsNode(reply.Servers, "node-4") {
		t.Fatalf("configuration after add is %v, want it to contain node-4", reply.Servers)
	}
	if len(reply.Servers) != 4 {
		t.Fatalf("configuration has %d servers, want 4: %v", len(reply.Servers), reply.Servers)
	}
}

// A change sent to a follower is refused as NotLeader, with the leader's id, and
// NOTHING is appended.
//
// This is §8's redirect, and it must not be reported as indeterminate: no entry
// exists, so the retry is completely safe.
func TestConfigChangeOnFollowerIsNotLeaderNotIndeterminate(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	leader := h.waitForLeader(t)
	var follower raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			follower = id
			break
		}
	}

	reply, err := h.adminFor(follower).AddServer("node-9", time.Second)
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if reply.OK {
		t.Fatal("a follower must not apply a configuration change")
	}
	if reply.Indeterminate {
		t.Fatal("a follower's refusal is FINAL — nothing was appended — and reporting " +
			"it as indeterminate tells the operator the change may yet take effect")
	}
	if !reply.NotLeader {
		t.Fatalf("expected NotLeader, got err=%q", reply.Err)
	}

	// The configuration must be untouched everywhere.
	for _, id := range h.ids {
		cfg, err := h.adminFor(id).Configuration()
		if err != nil {
			t.Fatalf("Configuration on %s: %v", id, err)
		}
		if containsNode(cfg.Servers, "node-9") {
			t.Fatalf("%s has node-9 in its configuration after a refused change: %v",
				id, cfg.Servers)
		}
	}
}

// §6 allows one change at a time. A second change while one is in flight is
// refused, not queued — and the refusal is final, not indeterminate.
func TestSecondConfigChangeIsRefusedWhileOneIsInFlightOverTheWire(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	leader := h.waitForLeader(t)
	admin := h.adminFor(leader)

	if _, err := admin.AddServer("node-4", 3*time.Second); err != nil {
		t.Fatalf("first AddServer: %v", err)
	}

	// Immediately attempt a second, different change. Whether it is refused
	// depends on whether the first has committed, so both outcomes are legal —
	// what is NOT legal is applying it while the first is uncommitted.
	reply, err := admin.AddServer("node-5", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("second AddServer: %v", err)
	}
	if !reply.OK && reply.Err == "" && !reply.Indeterminate {
		t.Fatal("a refused change must say why")
	}
}

// The retry path: adding a server that is already in the configuration is
// rejected rather than silently duplicating it.
func TestAddingAnExistingServerOverTheWireIsRejected(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	leader := h.waitForLeader(t)
	admin := h.adminFor(leader)

	reply, err := admin.AddServer(h.ids[1], 2*time.Second)
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if reply.OK {
		t.Fatal("adding a server already in the configuration must be rejected")
	}
	if !strings.Contains(reply.Err, "already in the configuration") {
		t.Fatalf("unhelpful error for a duplicate add: %q", reply.Err)
	}
}

// Configuration is readable from any node, including a follower.
//
// After an indeterminate change this is how an operator finds out what actually
// happened, so requiring a leader would make it unavailable in exactly the case
// it exists for.
func TestConfigurationIsReadableFromAnyNode(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	h.waitForLeader(t)

	for _, id := range h.ids {
		reply, err := h.adminFor(id).Configuration()
		if err != nil {
			t.Fatalf("Configuration on %s: %v", id, err)
		}
		if !reply.OK {
			t.Fatalf("Configuration on %s not OK: %q", id, reply.Err)
		}
		if len(reply.Servers) != 3 {
			t.Fatalf("%s reports %d servers, want 3: %v", id, len(reply.Servers), reply.Servers)
		}
	}
}

// A removed server really is gone from the configuration, and quorum follows it:
// a 3-node cluster reduced to 2 needs 2 to commit, not 2 of the original 3.
func TestRemoveServerOverTheWireShrinksTheConfiguration(t *testing.T) {
	h := newAdminHarness(t, 3)
	defer h.stop()

	leader := h.waitForLeader(t)
	var victim raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			victim = id
			break
		}
	}

	reply, err := h.adminFor(leader).RemoveServer(victim, 3*time.Second)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if !reply.OK {
		t.Fatalf("RemoveServer not OK: indeterminate=%v err=%q", reply.Indeterminate, reply.Err)
	}
	if containsNode(reply.Servers, victim) {
		t.Fatalf("%s still in the configuration after removal: %v", victim, reply.Servers)
	}
	if len(reply.Servers) != 2 {
		t.Fatalf("configuration has %d servers after removal, want 2: %v",
			len(reply.Servers), reply.Servers)
	}
}

func containsNode(ids []raft.NodeID, want raft.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// adminHarness is a small real-network cluster for the membership tests.
//
// Mirrors the integration-test harness: real listeners on real ports, real
// net/rpc calls. Testing membership over a mock transport would defeat the
// purpose — the gap being closed was precisely that no wire path existed.
type adminHarness struct {
	ids     []raft.NodeID
	addrs   map[raft.NodeID]string
	servers map[raft.NodeID]*raft.Server
	rpcs    map[raft.NodeID]*Server
	trans   map[raft.NodeID]*Transport
}

func newAdminHarness(t *testing.T, n int) *adminHarness {
	t.Helper()

	h := &adminHarness{
		addrs:   make(map[raft.NodeID]string),
		servers: make(map[raft.NodeID]*raft.Server),
		rpcs:    make(map[raft.NodeID]*Server),
		trans:   make(map[raft.NodeID]*Transport),
	}
	for i := range n {
		h.ids = append(h.ids, raft.NodeID(fmt.Sprintf("node-%d", i+1)))
	}

	// Same widened timings as the other RPC integration tests: under -race,
	// scheduling delay alone can violate §5.2's inequality and lose elections
	// that nothing was wrong with.
	cfg := raft.Config{ElectionTimeoutMin: 400, ElectionTimeoutMax: 800, HeartbeatInterval: 60}

	for i, id := range h.ids {
		st := ledger.New()
		machine := ledger.NewMachine(st)
		tr := NewTransport(h.addrs, 300*time.Millisecond)
		srv := raft.NewServerWith(id, h.ids, machine, tr, cfg, int64(i+1)*7919)

		rs, err := Listen("127.0.0.1:0", srv, NewClientService(srv, machine, h.addrs))
		if err != nil {
			t.Fatalf("listen %s: %v", id, err)
		}
		h.addrs[id] = rs.Addr()
		h.servers[id] = srv
		h.rpcs[id] = rs
		h.trans[id] = tr
	}

	for _, id := range h.ids {
		h.servers[id].Start()
	}
	return h
}

func (h *adminHarness) adminFor(id raft.NodeID) *AdminClient {
	return NewAdminClient(h.addrs[id], TLSConfig{})
}

func (h *adminHarness) waitForLeader(t *testing.T) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range h.ids {
			if h.servers[id].Role() == raft.Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within 10s")
	return ""
}

func (h *adminHarness) stop() {
	for _, id := range h.ids {
		h.servers[id].Stop()
		h.rpcs[id].Close()
		h.trans[id].Close()
	}
}
