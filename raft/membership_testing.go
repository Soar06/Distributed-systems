package raft

// SetConfigurationForTest replaces this server's configuration before it starts.
//
// Exported for tests that need a cluster whose membership differs from the set of
// servers registered on the network — notably "add a server", where the newcomer
// must be running and reachable before it joins, exactly as a real machine is.
//
// Must be called before Start: it does not go through the log, so calling it on a
// running server would diverge that node's membership from everyone else's.
func (s *Server) SetConfigurationForTest(servers []NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.useConfigurationLocked(Configuration{Servers: append([]NodeID(nil), servers...)})
}
