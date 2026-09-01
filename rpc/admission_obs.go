package rpc

import "github.com/homura/core-bank/obs"

// Reporting admission state to the observability endpoints (G7).
//
// Node-level rather than per-shard because admission control sits at the client
// API, in front of the routing decision: a request is shed before anyone knows
// which shard it belongs to.

// SetAdmitter attaches the admission controller whose state should be reported.
func (h *HostSource) SetAdmitter(a *Admitter) { h.admit = a }

// Admission implements obs.AdmissionSource.
func (h *HostSource) Admission() obs.Admission {
	if h.admit == nil {
		// Available=false rather than zeroes: an operator must be able to tell
		// "backpressure is off" from "backpressure is on and nothing has been shed".
		return obs.Admission{}
	}
	inFlight, admitted, shedBusy, shedRate := h.admit.Stats()
	return obs.Admission{
		InFlight:  inFlight,
		Admitted:  admitted,
		ShedBusy:  shedBusy,
		ShedRate:  shedRate,
		Draining:  h.admit.Draining(),
		Available: true,
	}
}

var _ obs.AdmissionSource = (*HostSource)(nil)
