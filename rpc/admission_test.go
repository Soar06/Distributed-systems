package rpc

import (
	"fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/homura/core-bank/ledger"
	"github.com/homura/core-bank/raft"
)

// Backpressure, rate limiting, and graceful shutdown (G7).
//
// The property that matters most is not "requests get rejected under load" — it
// is WHICH ANSWER a rejected client gets. A shed request was never proposed, so
// it must come back Busy and never Indeterminate.
//
// That distinction is the entire point of bounding the queue. Indeterminate means
// "the entry may yet commit, retry with the same key"; Busy means "no entry
// exists, retry whenever you like". An unbounded queue turns every overloaded
// request into the first, more dangerous answer — and for a bank that is how a
// transfer gets double-sent (learn/READING_LIST.md §18).
//
// Per RULES.md rule 3: normal (limits off, everything works), failure (shed under
// load), concurrent (many callers at once), and retry (a shed request retried
// later succeeds and applies exactly once).

// --- admission unit behaviour ---------------------------------------------

// With no limits configured, nothing is refused.
func TestAdmitterWithoutLimitsAdmitsEverything(t *testing.T) {
	a := NewAdmitter(Limits{})
	for i := range 100 {
		release, rej, ok := a.Admit("client")
		if !ok {
			t.Fatalf("request %d refused with no limits configured: %+v", i, rej)
		}
		release()
	}
}

// The in-flight bound must actually bound.
func TestInFlightLimitShedsExcess(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 3})

	var releases []func()
	for i := range 3 {
		release, rej, ok := a.Admit("c")
		if !ok {
			t.Fatalf("request %d refused within the limit: %+v", i, rej)
		}
		releases = append(releases, release)
	}

	// The fourth must be shed, and the reason must say nothing was proposed.
	_, rej, ok := a.Admit("c")
	if ok {
		t.Fatal("a fourth concurrent request was admitted against a limit of 3")
	}
	if !rej.Busy {
		t.Fatalf("a shed request was not marked Busy: %+v", rej)
	}
	if rej.Reason == "" {
		t.Fatal("a shed request carried no reason")
	}

	// Releasing one frees a slot.
	releases[0]()
	if _, _, ok := a.Admit("c"); !ok {
		t.Fatal("a slot was not freed by releasing an in-flight request")
	}
}

// Release must be idempotent: a double release would leak capacity, letting the
// bound drift upward until it stops bounding anything.
func TestReleaseIsIdempotent(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 1})

	release, _, ok := a.Admit("c")
	if !ok {
		t.Fatal("first request refused")
	}
	release()
	release()
	release()

	if inFlight, _, _, _ := a.Stats(); inFlight != 0 {
		t.Fatalf("inFlight = %d after repeated releases, want 0 — a double release "+
			"leaks capacity and the bound drifts upward until it bounds nothing", inFlight)
	}
}

// The rate limiter must allow a burst and then throttle.
func TestRateLimitAllowsBurstThenThrottles(t *testing.T) {
	a := NewAdmitter(Limits{PerClientRate: 10, PerClientBurst: 5})

	// The burst is spendable immediately: real traffic is bursty, and a strict
	// per-second limit would reject spikes the system could absorb.
	for i := range 5 {
		release, rej, ok := a.Admit("c")
		if !ok {
			t.Fatalf("burst request %d refused: %+v", i, rej)
		}
		release()
	}

	// The sixth exceeds the bucket.
	_, rej, ok := a.Admit("c")
	if ok {
		t.Fatal("a sixth request was admitted against a burst of 5")
	}
	if !rej.RateLimted {
		t.Fatalf("a throttled request was not marked rate-limited: %+v", rej)
	}
	if rej.RetryAfter <= 0 {
		t.Fatal("a rate-limited reply carried no RetryAfter; a client that retries " +
			"immediately makes the overload worse")
	}
}

// Rate limits are PER CLIENT: one aggressive caller must not consume another's
// budget. That is the difference between rate limiting and load shedding.
func TestRateLimitIsPerClient(t *testing.T) {
	a := NewAdmitter(Limits{PerClientRate: 10, PerClientBurst: 2})

	for range 2 {
		if _, _, ok := a.Admit("noisy"); !ok {
			t.Fatal("noisy client refused within its own burst")
		}
	}
	if _, _, ok := a.Admit("noisy"); ok {
		t.Fatal("noisy client exceeded its burst without being throttled")
	}

	// A different client still has its full budget.
	if _, _, ok := a.Admit("quiet"); !ok {
		t.Fatal("a quiet client was throttled by another client's traffic; rate " +
			"limiting is a per-client fairness policy, not a global one")
	}
}

// Tokens must refill over time, or a throttled client is throttled forever.
func TestRateLimitRefills(t *testing.T) {
	a := NewAdmitter(Limits{PerClientRate: 100, PerClientBurst: 1})

	if _, _, ok := a.Admit("c"); !ok {
		t.Fatal("first request refused")
	}
	if _, _, ok := a.Admit("c"); ok {
		t.Fatal("second immediate request was admitted against a burst of 1")
	}

	// At 100/s a token arrives within ~10ms.
	time.Sleep(50 * time.Millisecond)
	if _, _, ok := a.Admit("c"); !ok {
		t.Fatal("the bucket never refilled; a throttled client would be throttled forever")
	}
}

// An unidentified caller must be limited, not exempt: otherwise the limit is
// bypassable by omitting the field.
func TestUnidentifiedCallersShareOneBucket(t *testing.T) {
	a := NewAdmitter(Limits{PerClientRate: 1, PerClientBurst: 2})

	if _, _, ok := a.Admit(""); !ok {
		t.Fatal("first anonymous request refused")
	}
	if _, _, ok := a.Admit(""); !ok {
		t.Fatal("second anonymous request refused within the burst")
	}
	if _, _, ok := a.Admit(""); ok {
		t.Fatal("anonymous callers were exempt from rate limiting, which makes the " +
			"limit bypassable by leaving ClientID empty")
	}
}

// Shedding must be counted, or a system that sheds looks identical to one that
// is merely slow.
func TestAdmissionCountersAreRecorded(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 1, PerClientRate: 1000, PerClientBurst: 1000})

	release, _, _ := a.Admit("c")
	a.Admit("c") // shed: over the in-flight limit
	release()

	_, admitted, shedBusy, _ := a.Stats()
	if admitted != 1 {
		t.Fatalf("admitted = %d, want 1", admitted)
	}
	if shedBusy != 1 {
		t.Fatalf("shedBusy = %d, want 1 — shedding must be observable", shedBusy)
	}
}

// --- the client contract --------------------------------------------------

// THE property: a shed write must be Busy, never Indeterminate.
func TestShedWriteIsBusyNotIndeterminate(t *testing.T) {
	state := ledger.New()
	machine := ledger.NewMachine(state)
	srv := raft.NewServer("n1", []raft.NodeID{"n1"}, machine)
	svc := NewClientService(srv, machine, map[raft.NodeID]string{"n1": "127.0.0.1:0"})
	svc.SetLimits(Limits{MaxInFlight: 1})

	// Occupy the only slot.
	release, _, ok := svc.Admitter().Admit("holder")
	if !ok {
		t.Fatal("could not occupy the in-flight slot")
	}
	defer release()

	var reply TxReply
	if err := svc.Submit(TxArgs{
		Op: "open", IdempotencyKey: "k1", To: "alice", Amount: 100, ClientID: "c",
	}, &reply); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if !reply.Busy {
		t.Fatalf("a shed write was not marked Busy: %+v", reply)
	}
	if reply.Indeterminate {
		t.Fatal("a shed write was marked INDETERMINATE. Nothing was proposed, so " +
			"there is no entry that might still commit. Telling a client the outcome " +
			"is unknown for a request that never existed is how a transfer gets " +
			"double-sent — it is the exact hazard bounding the queue exists to remove")
	}
	if reply.OK {
		t.Fatal("a shed write reported success")
	}
	if reply.RetryAfter <= 0 {
		t.Fatal("a Busy reply carried no RetryAfter")
	}

	// And nothing reached the log.
	if got := len(srv.LogEntries()); got != 1 {
		t.Fatalf("a shed write reached the log: %d entries, want 1 (the sentinel)", got)
	}
}

// A shed request retried later must succeed and apply exactly once.
func TestShedRequestSucceedsOnRetry(t *testing.T) {
	c := startCluster(t, 3)
	_, addr := c.waitLeader(5 * time.Second)

	var open TxReply
	call(t, addr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "alice", Amount: 10000}, &open)
	if !open.OK {
		t.Fatalf("setup open failed: %+v", open)
	}

	// The same key twice: the second must return the first's result, not apply again.
	args := TxArgs{Op: "deposit", IdempotencyKey: "dep-1", To: "alice", Amount: 500}
	var first, second TxReply
	call(t, addr, "Bank.Submit", args, &first)
	call(t, addr, "Bank.Submit", args, &second)

	if !first.OK {
		t.Fatalf("first deposit failed: %+v", first)
	}
	if second.Balance != first.Balance {
		t.Fatalf("a retried deposit changed the balance: %d -> %d",
			first.Balance, second.Balance)
	}
	if first.Balance != 10500 {
		t.Fatalf("balance = %d, want 10500", first.Balance)
	}
}

// Concurrent callers against a tight limit: some are shed, none are lied to, and
// money is exactly conserved.
func TestConcurrentLoadShedsWithoutLosingMoney(t *testing.T) {
	state := ledger.New()
	machine := ledger.NewMachine(state)
	srv := raft.NewServer("solo", []raft.NodeID{"solo"}, machine)
	svc := NewClientService(srv, machine, map[raft.NodeID]string{"solo": "127.0.0.1:0"})
	svc.SetLimits(Limits{MaxInFlight: 2})

	var busy, indeterminate atomic.Int64
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var r TxReply
			svc.Submit(TxArgs{
				Op: "open", IdempotencyKey: fmt.Sprintf("k-%d", i),
				To: fmt.Sprintf("acct-%d", i), Amount: 100, ClientID: "c",
			}, &r)
			if r.Busy {
				busy.Add(1)
			}
			if r.Indeterminate {
				indeterminate.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if indeterminate.Load() > 0 {
		t.Fatalf("%d requests came back Indeterminate under load. Shed requests were "+
			"never proposed and must be Busy; Indeterminate tells a client to retry a "+
			"write that may already have committed", indeterminate.Load())
	}
	t.Logf("%d of 40 concurrent requests shed as Busy", busy.Load())

	// Whatever was admitted must be internally consistent.
	if err := state.VerifyDoubleEntry(); err != nil {
		t.Fatalf("ledger inconsistent after shedding: %v", err)
	}
}

// --- graceful shutdown ----------------------------------------------------

// Draining must stop admitting new work, so the drain can finish.
func TestDrainStopsAdmittingNewWork(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 10})

	if _, _, ok := a.Admit("c"); !ok {
		t.Fatal("request refused before draining")
	}
	// Deliberately not released: one request is still in flight.

	go a.Drain(200 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	_, rej, ok := a.Admit("c")
	if ok {
		t.Fatal("a new request was admitted while draining; the drain would never " +
			"finish because work keeps arriving")
	}
	if !rej.Busy {
		t.Fatalf("a request refused during drain was not marked Busy: %+v", rej)
	}
	if !a.Draining() {
		t.Fatal("Draining() is false during a drain")
	}
}

// A clean drain returns zero once in-flight work finishes.
func TestDrainWaitsForInFlightWork(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 10})

	release, _, ok := a.Admit("c")
	if !ok {
		t.Fatal("request refused")
	}

	done := make(chan int64, 1)
	go func() { done <- a.Drain(2 * time.Second) }()

	// Finish the in-flight request shortly after the drain starts.
	time.Sleep(50 * time.Millisecond)
	release()

	select {
	case remaining := <-done:
		if remaining != 0 {
			t.Fatalf("Drain returned %d still in flight, want 0 — it did not wait "+
				"for the request to finish", remaining)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain never returned")
	}
}

// A drain that times out must REPORT what was still in flight rather than
// pretending it was clean.
func TestDrainReportsUnfinishedWork(t *testing.T) {
	a := NewAdmitter(Limits{MaxInFlight: 10})

	release, _, _ := a.Admit("c")
	defer release()

	remaining := a.Drain(100 * time.Millisecond)
	if remaining != 1 {
		t.Fatalf("Drain returned %d, want 1 — a shutdown that abandons in-flight "+
			"requests must say so, since those clients will see a dropped connection "+
			"and cannot tell whether their write committed", remaining)
	}
}

// Draining must work on a node that never configured any limits: an operator who
// did not opt into backpressure must still get a clean shutdown.
func TestDrainWorksWithoutConfiguredLimits(t *testing.T) {
	a := NewAdmitter(Limits{})

	release, _, ok := a.Admit("c")
	if !ok {
		t.Fatal("request refused with no limits")
	}

	done := make(chan int64, 1)
	go func() { done <- a.Drain(2 * time.Second) }()

	time.Sleep(30 * time.Millisecond)
	if _, _, ok := a.Admit("c"); ok {
		t.Fatal("a new request was admitted while draining a limit-free node")
	}
	release()

	select {
	case remaining := <-done:
		if remaining != 0 {
			t.Fatalf("Drain returned %d on a limit-free node, want 0", remaining)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain never returned on a limit-free node")
	}
}

// Over the wire: a draining node refuses writes with Busy, so a client knows to
// go elsewhere rather than waiting on a node that is leaving.
func TestDrainingNodeRefusesWritesOverTheWire(t *testing.T) {
	c := startCluster(t, 3)
	leader, addr := c.waitLeader(5 * time.Second)
	_ = leader

	var open TxReply
	call(t, addr, "Bank.Submit",
		TxArgs{Op: "open", IdempotencyKey: "o", To: "alice", Amount: 1000}, &open)
	if !open.OK {
		t.Fatalf("setup failed: %+v", open)
	}

	// Start draining the node we are talking to.
	c.clients[c.leaderID(t)].Admitter().Drain(50 * time.Millisecond)

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var reply TxReply
	if err := client.Call("Bank.Submit", TxArgs{
		Op: "deposit", IdempotencyKey: "during-drain", To: "alice", Amount: 100,
	}, &reply); err != nil {
		t.Fatalf("Bank.Submit: %v", err)
	}

	if reply.OK {
		t.Fatal("a draining node accepted a write")
	}
	if !reply.Busy {
		t.Fatalf("a write to a draining node was not marked Busy: %+v", reply)
	}
	if reply.Indeterminate {
		t.Fatal("a write refused by a draining node was marked Indeterminate; " +
			"nothing was proposed")
	}
}
