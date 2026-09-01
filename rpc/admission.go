package rpc

import (
	"sync"
	"sync/atomic"
	"time"
)

// Admission control: backpressure and rate limiting (G7).
//
// The counter-intuitive result this is built on, and the reason it exists at all:
// A BOUNDED QUEUE THAT REJECTS IS MORE AVAILABLE THAN AN UNBOUNDED ONE THAT
// ACCEPTS EVERYTHING.
//
// An unbounded queue does not remove the capacity limit; it converts the limit
// from a visible rejection into invisible latency. Requests keep arriving faster
// than they are served, the queue grows, and every response takes longer — until
// responses arrive after the client has already given up. The system is then
// doing all of the work for none of the useful output. Theory in
// learn/READING_LIST.md §18.
//
// For THIS system the argument is sharper twice over:
//
//   - The Raft leader is the bottleneck by construction. Every write funnels
//     through it (Phase 1 measured 3 nodes at 119.9 tx/s against 5 nodes at 105.9),
//     so a leader cannot shed load by scaling out. The queue in front of it is the
//     only control there is.
//
//   - A client that times out on a write whose entry still commits is exactly the
//     Indeterminate hazard in the client contract: the outcome is unknown, and a
//     client that records it as "did not happen" and reissues under a new key
//     double-sends the money. An unbounded queue MANUFACTURES that hazard at
//     scale. A Busy rejection is strictly better information — nothing was
//     proposed, so retrying is unambiguously safe.

// Limits configures admission control. A zero value disables everything, which is
// what keeps existing tests and local development unchanged.
type Limits struct {
	// MaxInFlight bounds concurrent proposals awaiting commit. 0 disables the
	// bound.
	//
	// [project decision] The bound is on IN-FLIGHT PROPOSALS, not on connections.
	// Connections are cheap; what is scarce is the leader's ability to replicate
	// and commit, which is the resource actually under contention.
	MaxInFlight int

	// PerClientRate is the sustained requests-per-second allowed per client, and
	// PerClientBurst the size of the bucket. 0 for either disables rate limiting.
	//
	// Rate limiting and load shedding are related but distinct, and conflating
	// them is a common mistake: rate limiting is a per-client POLICY enforcing
	// fairness whether or not the system is busy, while shedding is a REACTION to
	// the system's own saturation, applied to everyone.
	PerClientRate  float64
	PerClientBurst int
}

// Enabled reports whether any limit is configured.
func (l Limits) Enabled() bool {
	return l.MaxInFlight > 0 || (l.PerClientRate > 0 && l.PerClientBurst > 0)
}

// Admitter decides whether a request may proceed.
type Admitter struct {
	limits Limits

	// inFlight counts proposals currently awaiting commit.
	inFlight atomic.Int64

	// draining latches on shutdown: no new request is admitted after it is set,
	// or the drain would never finish because work keeps arriving.
	draining atomic.Bool

	mu      sync.Mutex
	buckets map[string]*tokenBucket

	// Counters, exposed so shedding is observable rather than silent. A system
	// that sheds load without saying so looks identical to one that is simply
	// slow.
	shedInFlight atomic.Int64
	shedRate     atomic.Int64
	admitted     atomic.Int64
}

// NewAdmitter builds an admission controller.
func NewAdmitter(l Limits) *Admitter {
	return &Admitter{limits: l, buckets: make(map[string]*tokenBucket)}
}

// Rejection explains why a request was refused, or is empty when admitted.
type Rejection struct {
	Busy       bool
	RateLimted bool
	Reason     string

	// RetryAfter is how long the caller should wait. Advisory, but it is what
	// stops a rejected client from immediately retrying and making the overload
	// worse — the retry-amplification problem.
	RetryAfter time.Duration
}

// Admit reserves capacity for one request. The returned release must be called
// when the request finishes, whatever its outcome.
//
// Rejections are typed rather than a bare error so the client API can map them
// onto the four-outcome contract correctly: a rejected request was NEVER
// PROPOSED, so it is emphatically not Indeterminate.
func (a *Admitter) Admit(client string) (release func(), rej Rejection, ok bool) {
	if a == nil {
		return func() {}, Rejection{}, true
	}

	// Checked even when no limits are configured: shutdown draining must work on
	// a node that never enabled backpressure, or an operator gets an abrupt exit
	// precisely because they did not opt into rate limiting.
	if a.draining.Load() {
		return func() {}, Rejection{
			Busy:   true,
			Reason: "this node is shutting down; nothing was proposed, retry elsewhere",
			// No RetryAfter: the client should go to another node, not wait for
			// this one to come back.
		}, false
	}

	// Rate limit first: a client over its budget should not consume an in-flight
	// slot, or one aggressive caller could hold the whole allowance while being
	// rejected anyway.
	if a.limits.PerClientRate > 0 && a.limits.PerClientBurst > 0 {
		if !a.allowRate(client) {
			a.shedRate.Add(1)
			return func() {}, Rejection{
				RateLimted: true,
				Reason:     "rate limit exceeded for this client",
				RetryAfter: time.Duration(float64(time.Second) / a.limits.PerClientRate),
			}, false
		}
	}

	// The in-flight counter is maintained UNCONDITIONALLY, even when no bound is
	// configured, so Drain always has something to wait on. A node that never
	// enabled backpressure must still shut down cleanly.
	n := a.inFlight.Add(1)
	if a.limits.MaxInFlight > 0 && n > int64(a.limits.MaxInFlight) {
		// Reserved optimistically, then given back: a check-then-increment would
		// let several goroutines pass the same check.
		a.inFlight.Add(-1)
		a.shedInFlight.Add(1)
		return func() {}, Rejection{
			Busy: true,
			Reason: "the leader has too many proposals in flight; " +
				"nothing was proposed, so retrying is safe",
			RetryAfter: 50 * time.Millisecond,
		}, false
	}

	a.admitted.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() { a.inFlight.Add(-1) })
	}, Rejection{}, true
}

// Stats reports admission counters, for metrics.
func (a *Admitter) Stats() (inFlight, admitted, shedBusy, shedRate int64) {
	if a == nil {
		return 0, 0, 0, 0
	}
	return a.inFlight.Load(), a.admitted.Load(), a.shedInFlight.Load(), a.shedRate.Load()
}

// allowRate spends one token from the client's bucket.
func (a *Admitter) allowRate(client string) bool {
	// An empty client id shares one bucket. That is deliberate: unidentified
	// callers are rate-limited as a group rather than being exempt, since exempting
	// them would make the limit trivially bypassable by omitting the field.
	a.mu.Lock()
	b, ok := a.buckets[client]
	if !ok {
		b = newTokenBucket(a.limits.PerClientRate, a.limits.PerClientBurst)
		a.buckets[client] = b
	}
	a.mu.Unlock()

	return b.allow()
}

// tokenBucket is the standard rate-limiting mechanism: tokens accumulate at a
// fixed rate up to a burst capacity, and each request spends one.
//
// The burst allowance is what makes it usable rather than merely correct. Real
// traffic is bursty, and a strict requests-per-second limit rejects legitimate
// spikes the system could absorb without noticing.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64 // tokens per second
	last     time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		tokens:   float64(burst),
		capacity: float64(burst),
		rate:     rate,
		last:     time.Now(),
	}
}

func (b *tokenBucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	// Refill for elapsed time, capped at capacity: an idle client accumulates a
	// full burst but never more, so a long silence cannot be banked into an
	// unbounded spike.
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
