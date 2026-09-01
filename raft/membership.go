package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Cluster membership changes (§6, dissertation §4.1) — G6.
//
// The problem, and why it needs a mechanism at all: switching directly from an
// old configuration to a new one is UNSAFE, because servers cannot all switch at
// the same instant. During the changeover the cluster is briefly two different
// clusters, and if the old and new configurations have disjoint majorities, both
// can elect a leader in the same term. That is Election Safety violated — the
// property everything else rests on. Figure 10 shows exactly this going 3->5:
// {S1,S2} is a majority of the old and {S3,S4,S5} a majority of the new.
//
// [project decision] SINGLE-SERVER CHANGES (dissertation §4.1), not the extended
// paper's joint consensus (§6).
//
// The safety argument is a counting one, and it is what makes the simpler
// mechanism correct: when two configurations differ by ONE server, any majority
// of the old and any majority of the new must overlap in at least one server, and
// a single server never votes twice in one term. Two disjoint majorities
// therefore cannot exist, and joint consensus is unnecessary. Ongaro's
// dissertation adopts this as the primary approach, and it is what etcd and
// Consul actually use. A cluster needing a bigger change makes several
// one-at-a-time changes.
//
// Theory logged in learn/READING_LIST.md §17.

var (
	// ErrNotSingleServerChange rejects a configuration that differs by more than
	// one server. The overlap argument above only holds for a difference of one,
	// so a larger jump is refused rather than attempted unsafely.
	ErrNotSingleServerChange = errors.New(
		"raft: configuration changes must add or remove exactly one server at a time")

	// ErrConfigChangeInFlight rejects a second change while one is uncommitted.
	//
	// Two concurrent changes can compose into a difference of two, which breaks
	// the overlap argument even though each change alone is safe.
	ErrConfigChangeInFlight = errors.New(
		"raft: a configuration change is already in progress")

	// ErrNotInConfiguration means the server proposing the change is not itself a
	// member, so it has no standing to change the cluster.
	ErrNotInConfiguration = errors.New("raft: this server is not in the current configuration")

	// ErrLastServer refuses to remove the final server: a cluster of zero can
	// never elect a leader, so the removal could never itself commit.
	ErrLastServer = errors.New("raft: cannot remove the last server in the cluster")
)

// configPrefix marks a log entry as a configuration change rather than a state
// machine command.
//
// A distinct byte prefix rather than a separate field, because LogEntry.Command
// is opaque bytes by design — raft/ knows nothing about what the application puts
// there (DESIGN.md §7). The prefix is chosen to be a byte no ledger or shard
// command starts with, and isConfigEntry is the single place that knows it.
const configPrefix byte = 0xC0

// Configuration is the set of servers in the cluster.
//
// Held as a sorted slice so that encoding is deterministic: two servers applying
// the same configuration entry must produce byte-identical state, exactly as they
// must for any other replicated command.
type Configuration struct {
	Servers []NodeID
}

// Contains reports membership.
func (c Configuration) Contains(id NodeID) bool {
	for _, s := range c.Servers {
		if s == id {
			return true
		}
	}
	return false
}

// Majority is the number of servers constituting a majority of this
// configuration.
func (c Configuration) Majority() int { return len(c.Servers)/2 + 1 }

// withServer returns a copy with id added, sorted.
func (c Configuration) withServer(id NodeID) Configuration {
	if c.Contains(id) {
		return c
	}
	out := append(append([]NodeID(nil), c.Servers...), id)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return Configuration{Servers: out}
}

// withoutServer returns a copy with id removed.
func (c Configuration) withoutServer(id NodeID) Configuration {
	out := make([]NodeID, 0, len(c.Servers))
	for _, s := range c.Servers {
		if s != id {
			out = append(out, s)
		}
	}
	return Configuration{Servers: out}
}

// differsByOne reports whether other adds or removes exactly one server.
func (c Configuration) differsByOne(other Configuration) bool {
	diff := len(other.Servers) - len(c.Servers)
	if diff != 1 && diff != -1 {
		return false
	}
	// Count members of the smaller set missing from the larger. Exactly zero
	// means the change really is a single addition or removal rather than a
	// swap that happens to preserve the size difference.
	smaller, larger := c, other
	if diff < 0 {
		smaller, larger = other, c
	}
	for _, s := range smaller.Servers {
		if !larger.Contains(s) {
			return false
		}
	}
	return true
}

// encodeConfig serializes a configuration as a log entry command.
//
// Layout: [1 prefix][8 count] then per server [8 len][bytes]. Fixed-width
// little-endian, matching the discipline everywhere else in this package.
func encodeConfig(c Configuration) []byte {
	var b bytes.Buffer
	b.WriteByte(configPrefix)

	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], uint64(len(c.Servers)))
	b.Write(scratch[:])
	for _, s := range c.Servers {
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(s)))
		b.Write(scratch[:])
		b.WriteString(string(s))
	}
	return b.Bytes()
}

// isConfigEntry reports whether a command is a configuration change.
func isConfigEntry(cmd []byte) bool {
	return len(cmd) > 0 && cmd[0] == configPrefix
}

// decodeConfig parses a command produced by encodeConfig.
func decodeConfig(cmd []byte) (Configuration, error) {
	if !isConfigEntry(cmd) {
		return Configuration{}, fmt.Errorf("raft: not a configuration entry")
	}
	if len(cmd) < 9 {
		return Configuration{}, fmt.Errorf("raft: configuration entry too short")
	}
	n := binary.LittleEndian.Uint64(cmd[1:9])
	pos := 9

	servers := make([]NodeID, 0, n)
	for range n {
		if pos+8 > len(cmd) {
			return Configuration{}, fmt.Errorf("raft: truncated configuration entry")
		}
		size := binary.LittleEndian.Uint64(cmd[pos : pos+8])
		pos += 8
		// The declared length is validated before it is used as a bound.
		if uint64(len(cmd)-pos) < size {
			return Configuration{}, fmt.Errorf(
				"raft: configuration entry declares a %d-byte id but %d bytes remain",
				size, len(cmd)-pos)
		}
		servers = append(servers, NodeID(cmd[pos:pos+int(size)]))
		pos += int(size)
	}
	return Configuration{Servers: servers}, nil
}

// CurrentConfiguration returns the configuration this server is operating under.
func (s *Server) CurrentConfiguration() Configuration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Configuration{Servers: append([]NodeID(nil), s.peers...)}
}

// AddServer proposes adding one server to the cluster.
//
// Leader-only: a configuration change is an ordinary log entry, so it goes
// through the same replication path as any other write.
func (s *Server) AddServer(id NodeID) (Index, error) {
	return s.changeConfiguration(func(c Configuration) (Configuration, error) {
		if c.Contains(id) {
			return c, fmt.Errorf("raft: %s is already in the configuration", id)
		}
		return c.withServer(id), nil
	})
}

// RemoveServer proposes removing one server from the cluster.
//
// Removing the CURRENT LEADER is permitted: it keeps serving until the change
// commits — it is still the leader, and refusing to serve would strand the very
// entry that removes it — and then steps down. See applyConfigLocked.
func (s *Server) RemoveServer(id NodeID) (Index, error) {
	return s.changeConfiguration(func(c Configuration) (Configuration, error) {
		if !c.Contains(id) {
			return c, fmt.Errorf("raft: %s is not in the configuration", id)
		}
		if len(c.Servers) == 1 {
			return c, ErrLastServer
		}
		return c.withoutServer(id), nil
	})
}

// changeConfiguration appends a configuration entry to the leader's log.
//
// The entry takes effect on APPEND, not on commit — see useConfigurationLocked.
func (s *Server) changeConfiguration(mutate func(Configuration) (Configuration, error)) (Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != Leader {
		return 0, ErrNotLeader
	}

	current := Configuration{Servers: append([]NodeID(nil), s.peers...)}
	if !current.Contains(s.id) {
		// A server that has been removed but is still acting as leader must not
		// keep reconfiguring the cluster it no longer belongs to.
		return 0, ErrNotInConfiguration
	}

	// Only one change at a time. Two concurrent changes can compose into a
	// difference of two, which breaks the overlap argument that makes
	// single-server changes safe in the first place.
	if s.configChangeIndex > s.commitIndex {
		return 0, ErrConfigChangeInFlight
	}

	next, err := mutate(current)
	if err != nil {
		return 0, err
	}
	if !current.differsByOne(next) {
		return 0, ErrNotSingleServerChange
	}

	idx := s.lastIndex() + 1
	cmd := encodeConfig(next)
	s.log = append(s.log, LogEntry{Term: s.currentTerm, Index: idx, Command: cmd})
	s.matchIndex[s.id] = idx
	s.configChangeIndex = idx

	// Takes effect immediately on append (§6), before this entry commits.
	s.useConfigurationLocked(next)

	if err := s.persistLocked(); err != nil {
		return 0, err
	}
	return idx, nil
}

// useConfigurationLocked switches this server to a configuration.
//
// §6: "a server always uses the latest configuration in its log, regardless of
// whether the entry is committed."
//
// This is counter-intuitive and is the classic membership bug when reversed. If
// servers waited for the entry to commit, they would be VOTING under the old
// configuration while the leader COUNTED under the new one — which is precisely
// the disjoint-majority hazard the whole design exists to prevent.
//
// Caller must hold s.mu.
func (s *Server) useConfigurationLocked(c Configuration) {
	s.peers = append([]NodeID(nil), c.Servers...)

	if s.role != Leader {
		return
	}

	// Leader bookkeeping must cover exactly the new membership: a stale entry for
	// a removed server would keep counting toward quorum, and a missing entry for
	// an added one would never be replicated to.
	next := s.lastIndex() + 1
	for _, p := range c.Servers {
		if _, ok := s.nextIndex[p]; !ok {
			s.nextIndex[p] = next
			s.matchIndex[p] = 0
		}
	}
	for p := range s.nextIndex {
		if !c.Contains(p) {
			delete(s.nextIndex, p)
			delete(s.matchIndex, p)
			delete(s.lastContact, p)
		}
	}
	s.matchIndex[s.id] = s.lastIndex()
}

// adoptConfigFromLogLocked scans the log for the newest configuration entry and
// adopts it.
//
// Called after appending entries as a follower and after restoring from disk,
// because a follower learns about membership changes the same way it learns about
// everything else: they arrive as log entries.
//
// Caller must hold s.mu.
func (s *Server) adoptConfigFromLogLocked() {
	for i := len(s.log) - 1; i >= 1; i-- {
		if !isConfigEntry(s.log[i].Command) {
			continue
		}
		c, err := decodeConfig(s.log[i].Command)
		if err != nil {
			// A malformed configuration entry is not something to guess about: the
			// cluster's membership would be wrong, which is a safety problem rather
			// than an availability one. Counted and skipped.
			s.configErrs++
			s.lastConfigErr = err
			continue
		}
		s.useConfigurationLocked(c)
		s.configChangeIndex = s.log[i].Index
		return
	}
}

// SteppedDownAfterRemoval reports whether this server stepped down because a
// committed configuration removed it.
func (s *Server) SteppedDownAfterRemoval() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removedFromCluster
}

// checkSelfRemovalLocked steps this leader down once a configuration removing it
// has COMMITTED.
//
// Waiting for commit is deliberate and is the one place membership does not act
// on append. A leader removing itself must keep serving until the change commits
// — it is still the leader, and stepping down early would strand the very entry
// that removes it, leaving the cluster with a configuration nobody can complete.
//
// Caller must hold s.mu.
func (s *Server) checkSelfRemovalLocked() {
	if s.role != Leader || s.removedFromCluster {
		return
	}
	if s.configChangeIndex == 0 || s.commitIndex < s.configChangeIndex {
		return
	}
	if (Configuration{Servers: s.peers}).Contains(s.id) {
		return
	}
	s.removedFromCluster = true
	s.becomeFollower(s.currentTerm)
}

// ConfigFailures reports malformed configuration entries seen, with the most
// recent error.
func (s *Server) ConfigFailures() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configErrs, s.lastConfigErr
}

// minElectionTimeout is the floor used by the disruptive-server check.
func (s *Server) minElectionTimeout() time.Duration {
	return time.Duration(s.cfg.ElectionTimeoutMin) * time.Millisecond
}
