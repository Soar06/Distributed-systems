package raft

// Log accessors and the log-comparison rules from §5.3 and §5.4.1.
//
// These are kept separate from the role loop because they are pure functions of
// log state: they are the pieces most directly checkable against the paper, and
// the ones the Figure 3 safety properties are stated in terms of.

// lastIndex returns the index of the last entry in the log, or 0 if the log
// holds only the sentinel.
func (s *Server) lastIndex() Index {
	return s.log[len(s.log)-1].Index
}

// lastTerm returns the term of the last entry in the log, or 0 if the log holds
// only the sentinel.
func (s *Server) lastTerm() Term {
	return s.log[len(s.log)-1].Term
}

// entryAt returns the entry at index i and whether it exists. Index 0 refers to
// the sentinel, which exists but carries no command.
func (s *Server) entryAt(i Index) (LogEntry, bool) {
	if i > s.lastIndex() {
		return LogEntry{}, false
	}
	// The log is contiguous from the sentinel in Phase 1 (no compaction yet), so
	// index maps directly onto slice position. This assumption breaks when
	// snapshotting arrives (LATER.md) and is deliberately localized here.
	return s.log[i], true
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
func (s *Server) appendFrom(prevLogIndex Index, entries []LogEntry) {
	for i, e := range entries {
		idx := prevLogIndex + Index(i) + 1

		if existing, ok := s.entryAt(idx); ok {
			if existing.Term == e.Term {
				continue // already present and matching; leave it alone
			}
			// Conflict: truncate this entry and everything after it.
			s.log = s.log[:idx]
		}
		s.log = append(s.log, LogEntry{Term: e.Term, Index: idx, Command: e.Command})
	}
}

// majority returns the number of servers constituting a majority of the full
// cluster (not merely of those currently reachable).
//
// Counting against the full cluster is what makes Election Safety hold: two
// disjoint majorities cannot exist, so two leaders cannot be elected in one term.
func (s *Server) majority() int {
	return len(s.peers)/2 + 1
}
