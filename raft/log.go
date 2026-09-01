package raft

// Log accessors and the log-comparison rules from §5.3 and §5.4.1.
//
// These are kept separate from the role loop because they are pure functions of
// log state: they are the pieces most directly checkable against the paper, and
// the ones the Figure 3 safety properties are stated in terms of.

// Log indexing with a compacted prefix (§7).
//
// s.log[0] is a sentinel. Before compaction it is the zero entry at index 0, so
// log index and slice position coincide. AFTER compaction the sentinel becomes
// the snapshot boundary: it carries lastIncludedIndex/lastIncludedTerm, the
// entries before it are gone, and index no longer equals slice position.
//
// Every translation between the two goes through these helpers. That was already
// the design intent — the old entryAt carried a comment saying the assumption
// "breaks when snapshotting arrives and is deliberately localized here" — so this
// is the localization being cashed in rather than a new one being invented.

// baseIndex is the index of the sentinel: 0 normally, or lastIncludedIndex once
// the log has been compacted. Entries at or below it are no longer in the log.
func (s *Server) baseIndex() Index {
	return s.log[0].Index
}

// baseTerm is the term of the sentinel — lastIncludedTerm after compaction.
//
// This is what lets AppendEntries still answer a prevLogTerm check for the entry
// immediately before the snapshot. Without it the log would be unmatched at its
// own boundary and replication would stall permanently.
func (s *Server) baseTerm() Term {
	return s.log[0].Term
}

// slot converts a log index to a slice position. The caller must have checked
// the index is in range.
func (s *Server) slot(i Index) int {
	return int(i - s.baseIndex())
}

// lastIndex returns the index of the last entry in the log, or the snapshot
// boundary if the log holds only the sentinel.
func (s *Server) lastIndex() Index {
	return s.log[len(s.log)-1].Index
}

// lastTerm returns the term of the last entry in the log.
func (s *Server) lastTerm() Term {
	return s.log[len(s.log)-1].Term
}

// entryAt returns the entry at index i and whether it exists.
//
// An index at or below the snapshot boundary is NOT available: it has been
// discarded, and the caller must send a snapshot instead of pretending the entry
// is there. The sentinel itself (i == baseIndex) is returned, because
// prevLogIndex checks legitimately land on it.
func (s *Server) entryAt(i Index) (LogEntry, bool) {
	if i < s.baseIndex() || i > s.lastIndex() {
		return LogEntry{}, false
	}
	return s.log[s.slot(i)], true
}

// hasEntry reports whether index i is still in the log rather than compacted
// away. Distinct from entryAt's ok, which is also false for a future index.
func (s *Server) compactedAway(i Index) bool {
	return i < s.baseIndex()
}

// termAt returns the term of the entry at index i, and whether that entry
// exists.
func (s *Server) termAt(i Index) (Term, bool) {
	e, ok := s.entryAt(i)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

// isUpToDate reports whether a candidate's log is at least as up-to-date as this
// server's, per §5.4.1:
//
//	"Raft determines which of two logs is more up-to-date by comparing the index
//	and term of the last entries in the logs. If the logs have last entries with
//	different terms, then the log with the later term is more up-to-date. If the
//	logs end with the same term, then whichever log is longer is more up-to-date."
//
// This is the check that makes Leader Completeness (Figure 3) hold: a candidate
// missing committed entries cannot collect a majority.
func (s *Server) isUpToDate(candLastIndex Index, candLastTerm Term) bool {
	myIndex, myTerm := s.lastIndex(), s.lastTerm()
	if candLastTerm != myTerm {
		return candLastTerm > myTerm
	}
	return candLastIndex >= myIndex
}

// matchesPrevLog reports whether the log contains an entry at prevLogIndex whose
// term matches prevLogTerm — AppendEntries receiver rule 2 (§5.3).
//
// prevLogIndex 0 always matches: it refers to the sentinel, meaning the leader is
// sending from the very start of the log.
func (s *Server) matchesPrevLog(prevLogIndex Index, prevLogTerm Term) bool {
	// An index below our snapshot boundary is already committed and immutable —
	// it cannot conflict, so treating it as a match is safe and is what lets a
	// leader that is behind our snapshot still make progress.
	if s.compactedAway(prevLogIndex) {
		return true
	}
	t, ok := s.termAt(prevLogIndex)
	if !ok {
		return false
	}
	return t == prevLogTerm
}

// appendFrom applies AppendEntries receiver rules 3 and 4 (§5.3):
//
//  3. If an existing entry conflicts with a new one (same index but different
//     terms), delete the existing entry and all that follow it.
//  4. Append any new entries not already in the log.
//
// Rule 3's "and all that follow it" is essential: truncating only the conflicting
// entry would leave later entries that were replicated under a different leader,
// violating Log Matching (Figure 3).
//
// Entries already present and matching are NOT re-appended. This matters because
// AppendEntries can legitimately be delivered twice (the network may duplicate or
// reorder), and a naive append would corrupt the log on a retry.
// Returns whether the log was actually modified, so the caller can skip an
// unnecessary fsync on a pure heartbeat or a duplicate delivery.
func (s *Server) appendFrom(prevLogIndex Index, entries []LogEntry) bool {
	changed := false
	for i, e := range entries {
		idx := prevLogIndex + Index(i) + 1

		if idx <= s.baseIndex() {
			// Already covered by our snapshot: the entry is committed and immutable,
			// so there is nothing to append and nothing that may be truncated.
			continue
		}
		if existing, ok := s.entryAt(idx); ok {
			if existing.Term == e.Term {
				continue // already present and matching; leave it alone
			}
			// Conflict: truncate this entry and everything after it.
			s.log = s.log[:s.slot(idx)]
		}
		s.log = append(s.log, LogEntry{Term: e.Term, Index: idx, Command: e.Command})
		changed = true
	}
	return changed
}

// majority returns the number of servers constituting a majority of the full
// cluster (not merely of those currently reachable).
//
// Counting against the full cluster is what makes Election Safety hold: two
// disjoint majorities cannot exist, so two leaders cannot be elected in one term.
func (s *Server) majority() int {
	return len(s.peers)/2 + 1
}
