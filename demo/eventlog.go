package demo

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Structured event log, so the UI can filter by account, node and shard.
//
// The original log was a []string of preformatted lines. That is fine to read and
// impossible to filter: "which operations touched Vu" requires parsing the very
// sentence the formatter just built. Recording the fields alongside the message
// keeps the human-readable line while making the same event queryable.
//
// WHAT THE FILTERS MEAN, and why these three:
//
//   - account — the ledger identity a client asked about ("show me Vu")
//   - shard   — the Raft group that owns it, which is where the entry actually
//     lives; this is the unit that has a leader and a quorum
//   - node    — the machine, which is what fails and what gets killed in the demo
//
// Those are the three nouns the whole system is expressed in, and each one
// answers a different question: account = whose money, shard = which log, node =
// which machine. Anything else (op name, term, index) is detail ON an event
// rather than a way to select events, so it stays in the payload.

// EventKind separates operations a user performed from things the cluster did on
// its own.
//
// The distinction matters for the demo's central point: a re-election or a
// catch-up is NOT caused by the user, and mixing them into one stream makes the
// system look like it is responding to clicks when it is actually reacting to
// failure.
type EventKind string

const (
	// KindClient is an operation a client requested: open, deposit, withdraw,
	// transfer.
	KindClient EventKind = "client"

	// KindRaft is consensus activity: elections, leadership changes, replication
	// milestones, configuration changes.
	KindRaft EventKind = "raft"

	// KindControl is operator action from the demo UI: kill, revive, heal,
	// resize, deliberate health changes.
	KindControl EventKind = "control"

	// KindDrift is automatic health drift — the simulated background signal that
	// re-rolls every 5s per machine.
	//
	// Separated from KindControl because it is the only event nobody caused and
	// nothing depends on, and at one line per machine per 5s it buries every other
	// event in the log. Giving it its own kind keeps the signal available while
	// letting the UI leave it out of the default view. Deleting it instead would
	// hide why a leader moved when health biased the election.
	KindDrift EventKind = "drift"
)

// Event is one thing that happened, with the fields needed to filter it.
type Event struct {
	Seq  uint64    `json:"seq"`
	At   string    `json:"at"`
	Kind EventKind `json:"kind"`

	// Text is the human-readable line, unchanged from what the logger produced.
	Text string `json:"text"`

	// Account, Shard and Node are the filter axes. Empty means "not applicable to
	// this event" rather than "unknown" — a re-election has no account, and
	// saying so is more useful than inventing one.
	Account string `json:"account,omitempty"`
	Shard   string `json:"shard,omitempty"`
	Node    string `json:"node,omitempty"`

	// Outcome is the client contract's answer where one applies: ok, refused,
	// indeterminate, aborted. Carried separately from Text because
	// "indeterminate" is the case a reader most needs to pick out of a stream,
	// and searching prose for it is exactly the fragility this type removes.
	Outcome string `json:"outcome,omitempty"`
}

// eventLog is a bounded ring of structured events.
type eventLog struct {
	mu     sync.Mutex
	events []Event
	seq    uint64
	max    int
}

func newEventLog(max int) *eventLog {
	return &eventLog{max: max}
}

// add records one event, trimming the oldest when the bound is reached.
func (l *eventLog) add(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.seq++
	e.Seq = l.seq
	if e.At == "" {
		e.At = time.Now().Format("15:04:05.000")
	}
	l.events = append(l.events, e)

	// Bounded for the same reason the old string log was: this is appended to on
	// every action, and an unbounded buffer in a long-running demo is a leak.
	if len(l.events) > l.max {
		l.events = l.events[len(l.events)-l.max:]
	}
}

// snapshot returns a copy of the current events, oldest first.
func (l *eventLog) snapshot() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

// lines renders the events as the plain strings the existing UI field expects.
//
// Kept so the structured log can be introduced without breaking the current
// view: callers that only want text keep working while the filtering UI is
// built against the structured form.
func (l *eventLog) lines() []string {
	evs := l.snapshot()
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.At+"  "+e.Text)
	}
	return out
}

// EventFilter selects a subset of events. A zero filter matches everything.
type EventFilter struct {
	Account string
	Shard   string
	Node    string
	Kind    EventKind

	// ExcludeKind drops one kind from an otherwise unfiltered view. Needed
	// because "everything except the noisy background signal" is the useful
	// default, and Kind alone can only include.
	ExcludeKind EventKind
}

// matches reports whether an event satisfies the filter.
//
// An empty filter field matches any value INCLUDING empty, so filtering by
// account does not silently hide the raft events that have no account — the
// caller asked to narrow by account, and an election genuinely has none. The UI
// decides whether to show those; this function does not guess.
func (f EventFilter) matches(e Event) bool {
	if f.Account != "" && !strings.EqualFold(e.Account, f.Account) {
		return false
	}
	if f.Shard != "" && !strings.EqualFold(e.Shard, f.Shard) {
		return false
	}
	if f.Node != "" && !strings.EqualFold(e.Node, f.Node) {
		return false
	}
	if f.Kind != "" && e.Kind != f.Kind {
		return false
	}
	if f.ExcludeKind != "" && e.Kind == f.ExcludeKind {
		return false
	}
	return true
}

// Events returns the recorded events matching the filter, newest last.
func (c *Cluster) Events(f EventFilter, limit int) []Event {
	all := c.eventLog.snapshot()

	out := make([]Event, 0, len(all))
	for _, e := range all {
		if f.matches(e) {
			out = append(out, e)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// EventAxes reports the distinct accounts, shards and nodes present in the log,
// so the UI can offer filters without hardcoding names.
func (c *Cluster) EventAxes() map[string][]string {
	all := c.eventLog.snapshot()

	seen := map[string]map[string]bool{
		"accounts": {}, "shards": {}, "nodes": {},
	}
	for _, e := range all {
		if e.Account != "" {
			seen["accounts"][e.Account] = true
		}
		if e.Shard != "" {
			seen["shards"][e.Shard] = true
		}
		if e.Node != "" {
			seen["nodes"][e.Node] = true
		}
	}

	out := make(map[string][]string, len(seen))
	for k, set := range seen {
		vals := make([]string, 0, len(set))
		for v := range set {
			vals = append(vals, v)
		}
		sortStrings(vals)
		out[k] = vals
	}
	return out
}

// logEvent records a structured event and mirrors it into the legacy text log.
func (c *Cluster) logEvent(e Event) {
	if c.eventLog == nil {
		return
	}
	c.eventLog.add(e)

	// The existing []string view stays in sync, so nothing that reads it breaks
	// while the filtering UI is built.
	c.mu.Lock()
	c.events = append(c.events, fmt.Sprintf("%s  %s",
		time.Now().Format("15:04:05.000"), e.Text))
	if len(c.events) > 200 {
		c.events = c.events[len(c.events)-200:]
	}
	c.mu.Unlock()
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
