package raft

import "testing"

// The server's quorum size must track the configuration, not the startup flags.
func TestQuorumFollowsConfigurationChanges(t *testing.T) {
	s := newMemberServer(t, "n1", "n1", "n2", "n3")
	makeLeader(t, s, 1)

	s.mu.Lock()
	before := s.majority()
	s.mu.Unlock()
	if before != 2 {
		t.Fatalf("majority = %d for 3 servers, want 2", before)
	}

	if _, err := s.AddServer("n4"); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	s.mu.Lock()
	after := s.majority()
	s.mu.Unlock()
	if after != 3 {
		t.Fatalf("majority = %d after growing to 4 servers, want 3 — quorum must "+
			"follow the configuration or the leader commits against the wrong count", after)
	}

	// And the two ways of computing it must agree.
	if got := s.CurrentConfiguration().Majority(); got != after {
		t.Fatalf("Configuration.Majority() = %d but Server.majority() = %d; two "+
			"quorum sizes in one codebase is how a cluster commits against a "+
			"majority that is not one", got, after)
	}
}
