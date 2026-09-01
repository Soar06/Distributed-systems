package obs

import (
	"strings"
	"testing"

	"github.com/homura/core-bank/raft"
)

// Backpressure must be observable (G7).
//
// A system that sheds load silently looks identical from outside to one that is
// merely slow — and the two call for opposite responses: shedding means the
// capacity limit was reached and traffic should be reduced or capacity added,
// while slowness means something is stuck. Without these counters an operator
// cannot tell which they are looking at.

// admissionSource is a fakeSource that also reports admission state.
type admissionSource struct {
	fakeSource
	adm Admission
}

func (a *admissionSource) Admission() Admission { return a.adm }

func TestAdmissionCountersAreExposedAsMetrics(t *testing.T) {
	src := &admissionSource{
		fakeSource: fakeSource{
			id:     "n1",
			shards: []ShardHealth{healthyShard("shard-0", raft.Leader)},
		},
		adm: Admission{
			InFlight: 3, Admitted: 120, ShedBusy: 7, ShedRate: 2,
			Draining: false, Available: true,
		},
	}
	s := startTestServer(t, src)

	_, body := get(t, s, "/metrics")
	for _, want := range []string{
		`corebank_admission_in_flight{node="n1"} 3`,
		`corebank_admission_admitted_total{node="n1"} 120`,
		`corebank_admission_shed_busy_total{node="n1"} 7`,
		`corebank_admission_shed_rate_total{node="n1"} 2`,
		`corebank_admission_draining{node="n1"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n---\n%s", want, body)
		}
	}
}

// A draining node must say so, so an operator watching a rolling restart can see
// which node is leaving rather than inferring it from a traffic dip.
func TestDrainingIsExposedAsAMetric(t *testing.T) {
	src := &admissionSource{
		fakeSource: fakeSource{
			id:     "n1",
			shards: []ShardHealth{healthyShard("shard-0", raft.Leader)},
		},
		adm: Admission{Draining: true, Available: true},
	}
	s := startTestServer(t, src)

	_, body := get(t, s, "/metrics")
	if !strings.Contains(body, `corebank_admission_draining{node="n1"} 1`) {
		t.Fatalf("a draining node does not report it:\n%s", body)
	}
}

// With no admitter attached, the metrics are OMITTED rather than reported as
// zeroes: "backpressure is off" and "backpressure is on and nothing was shed"
// are different facts, and zeroes would conflate them.
func TestAdmissionMetricsAreOmittedWhenUnavailable(t *testing.T) {
	src := &admissionSource{
		fakeSource: fakeSource{
			id:     "n1",
			shards: []ShardHealth{healthyShard("shard-0", raft.Leader)},
		},
		adm: Admission{Available: false},
	}
	s := startTestServer(t, src)

	_, body := get(t, s, "/metrics")
	if strings.Contains(body, "corebank_admission_") {
		t.Fatalf("admission metrics were emitted with no admitter attached; zeroes "+
			"would make 'backpressure is off' indistinguishable from 'nothing has "+
			"been shed'\n%s", body)
	}
	// The rest of the metrics must still be there.
	if !strings.Contains(body, "corebank_raft_ready") {
		t.Fatal("unrelated metrics went missing")
	}
}
