package rpc

import (
	"time"
)

// Graceful shutdown: drain before closing (G7).
//
// A node that exits abruptly leaves clients holding requests whose outcome is
// unknown — manufacturing the Indeterminate hazard deliberately, at the one moment
// it is entirely avoidable. The ordered alternative is to stop accepting new work,
// let what is already in flight finish, and only then close.
//
// This project already learned half the lesson the hard way. The phantom-quorum
// bug was a node that kept ANSWERING after it should have stopped: shut down for a
// rolling restart, it went on acking replication over the leader's cached
// connection, so the leader committed against a quorum that no longer existed.
// raft.Server therefore latches `stopped` and rpc.Server.Close closes established
// connections. Draining extends that path rather than inventing one.
//
// The ORDER is the whole content of this file, and each step exists for a
// specific failure:
//
//  1. Stop admitting new requests — otherwise the drain never finishes, because
//     work keeps arriving.
//  2. Wait for in-flight requests to complete, up to a deadline. A request that
//     completes here gets a real answer instead of a dropped connection.
//  3. Give up leadership. Clients are then redirected to a node that can serve
//     them rather than waiting on one that is going away.
//  4. Close the listener and the connections.
//
// Doing 3 before 2 would strand in-flight proposals that this node is still the
// only one able to commit. Doing 4 before 2 drops answers on the floor, which is
// exactly the outcome draining exists to avoid.

// Drain stops admitting new requests and waits for in-flight ones to finish.
//
// Returns the number still in flight when it returned: zero means a clean drain,
// non-zero means the deadline expired and those clients will see a dropped
// connection. Reported rather than swallowed, because "we shut down while N
// requests were in flight" is exactly what an operator needs in an incident.
func (a *Admitter) Drain(timeout time.Duration) int64 {
	if a == nil {
		return 0
	}

	// Refuse new work for the rest of this process's life. Setting the in-flight
	// limit to a sentinel rather than adding a flag keeps the check on the hot
	// path a single atomic load, which it already was.
	a.draining.Store(true)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n := a.inFlight.Load(); n == 0 {
			return 0
		}
		time.Sleep(5 * time.Millisecond)
	}
	return a.inFlight.Load()
}

// Draining reports whether this node has stopped admitting new requests.
func (a *Admitter) Draining() bool {
	if a == nil {
		return false
	}
	return a.draining.Load()
}
