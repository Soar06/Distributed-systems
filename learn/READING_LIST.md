# Reading List — Distributed Systems Theory

Every theory topic behind this project, logged with what it actually solves and
the primary source(s) to go read. Required by [Agents/RULES.md](../Agents/RULES.md)
rule #1 — this is a map from "knowledge used in this project" back to "where it
came from," not a restatement of the source material itself.

Read in the order listed for Phase 1. Entries are added as new topics come up.

---

## 1. Consensus (Raft)

**What the theory does:** Solves the problem of getting multiple independent,
unreliable nodes to agree on a single, ordered sequence of operations — even when
some nodes crash or messages are delayed — so that every replica ends up with the
identical state if it applies that same sequence in order (state machine
replication). This is the mechanism the whole ledger's correctness rests on: it's
what makes "3-5 copies of the bank" behave as one consistent bank instead of three
independent, possibly-disagreeing ones.

**Primary source (read fully, in order):**
1. Ongaro & Ousterhout, *"In Search of an Understandable Consensus Algorithm
   (Extended Version)"*, Stanford — **the extended tech report, published
   2014-05-20, at https://raft.github.io/raft.pdf**. This exact document is the
   project's canonical citation: it is the extended version of the shorter USENIX
   ATC'14 conference paper, and the two differ in section/figure numbering, so
   always cite section numbers against this one. Figure 2 in it is essentially the
   spec we implement in Go. Ongaro's PhD dissertation (membership changes, log
   compaction) is also linked at **raft.github.io**.
   - §2 "Replicated state machines" is the definition of *log* this project uses:
     a log is a **series of commands the state machine executes in order** — the
     authoritative data, from which ledger balances are derived — *not* diagnostic
     logging. "Keeping the replicated log consistent is the job of the consensus
     algorithm." This is the same structure as the Phase 3 event-sourced ledger.
2. **raft.github.io** — canonical hub; also links reference implementations in
   multiple languages.
3. **thesecretlivesofdata.com/raft** — interactive visual walkthrough of leader
   election / log replication. Not a primary source, but the standard companion
   to read alongside the paper before the formal notation clicks.
4. **github.com/etcd-io/raft** (`raft/doc.go` especially) — production Go
   implementation, useful *after* the paper to see how paper concepts (term, log
   entry, commit index) map onto real structs. Not to copy from — to cross-check
   understanding against.

---

## 2. Distributed systems failure model + consistency (Kleppmann)

**What the theory does:** Builds the vocabulary and mental model for everything
that can go wrong in a distributed system (network partitions, unreliable clocks,
partial failures, split-brain) and what "consistency" precisely means once
multiple copies of data exist (culminating in linearizability — the specific
guarantee Raft provides). This is the conceptual foundation Tier 2 vocabulary
(crash vs Byzantine faults, quorum, split-brain) sits on.

**Primary source:**
- Martin Kleppmann, *Designing Data-Intensive Applications*, Chapters 8
  ("The Trouble with Distributed Systems") and 9 ("Consistency and Consensus").
  Clearer and more practically grounded than most academic papers on these
  specific topics — worth owning as a book, not just borrowing chapters.

---

## 3. CAP theorem

**What the theory does:** States the fundamental trade-off that under a network
partition, a distributed system must choose between Consistency (every read sees
the latest write) and Availability (every request gets a response) — it cannot
guarantee both. This is the formal grounding for why Raft (a CP system) sometimes
must refuse to answer (e.g. no leader during an election) rather than risk
returning stale/wrong data — directly relevant to the same-account collision
behavior we're building toward in the bank app / cluster dashboard.

**Primary source:**
- Eric Brewer, *"CAP Twelve Years Later: How the 'Rules' Have Changed"* (2012) —
  Brewer's own retrospective correcting common misreadings of his original
  conjecture. Read this over the original conjecture talk.

---

## 4. Paxos (optional, comparison only — not required for Phase 1)

**What the theory does:** The original, classical solution to the same consensus
problem Raft solves, but notoriously difficult to reason about and implement
correctly — Raft was explicitly designed as a more understandable alternative
proving the same safety properties. Relevant later per `LATER.md`'s note on
comparing consensus protocols; not needed to build Phase 1.

**Primary source:**
- Leslie Lamport, *"Paxos Made Simple"*.

---

## 5. Linearizable reads (Raft §8)

**What the theory does:** A read served naively by a leader can return stale data,
because that leader may already have been deposed by a newer leader it has not yet
heard from. Linearizability — "each operation appears to execute instantaneously,
exactly once, at some point between its invocation and its response" (§8) — forbids
that. The theory says what a *correct* read costs, which is the honest answer to
"why can't the bank just read the balance quickly."

The paper (§8) requires two precautions for a read that does not go through the log:

1. **The leader must know which entries are committed.** Leader Completeness
   guarantees it *holds* every committed entry, but at the start of its term it does
   not know *which* are committed — because the commit rule (§5.4.2) forbids
   committing a previous term's entry directly. The fix, quoted: *"it needs to commit
   an entry from its term. Raft handles this by having each leader commit a blank
   no-op entry into the log at the start of its term."*
2. **The leader must confirm it has not been deposed**, by exchanging heartbeats with
   a majority before answering. The paper notes a lease-based alternative but warns
   it *"would rely on timing for safety (it assumes bounded clock skew)"* — which is
   why we do not use leases.

§8 also gives the client-side half of exactly-once semantics: *"clients assign unique
serial numbers to every command,"* and the state machine tracks the latest serial per
client with its response, answering a repeat without re-executing. That is precisely
the idempotency-key design in `ledger/`.

**[project decision]** We implement the no-op entry plus the heartbeat-confirmation
read (ReadIndex), and deliberately **not** lease-based reads: this project's whole
point is that safety should not depend on clock assumptions. Follower reads are a
LATER.md item.

**Primary source:** Raft extended tech report §8 ("Client interaction"), plus §5.4.2
for why a leader may not commit a previous term's entry directly.

---

## 6. Chaos engineering (Chaos Monkey / Simian Army)

**What the theory does:** Inverts how failure is tested. Instead of hoping the
system survives rare failures and finding out during a real incident, failures are
injected *deliberately and continuously* so that surviving them becomes routine and
regressions surface immediately. Netflix's Chaos Monkey pioneered this by randomly
terminating instances in production; the Simian Army extended it (Latency Monkey for
degraded network calls, Chaos Gorilla for whole-availability-zone loss).

Why it matters here specifically: Raft's guarantees are *entirely* about behavior
under failure. A Raft implementation that is only tested on the happy path has not
been tested at all — leader crashes, partitions, and dropped/delayed/duplicated RPCs
are the conditions Figure 3's safety properties exist to survive. This is the
methodology behind [Agents/RULES.md](../Agents/RULES.md) rule 3 and the `sim/`
package.

**The five principles** (from principlesofchaos.org, quoted):

> "Chaos Engineering is the discipline of experimenting on a system in order to
> build confidence in the system's capability to withstand turbulent conditions in
> production."

1. **Build a hypothesis around steady-state behavior** — assert on measurable
   output, not internal attributes. *Our steady state:* exactly one leader per term,
   no lost or duplicated money, Figure 3 holds.
2. **Vary real-world events** — inject authentic failures: node crash, network
   partition, message loss/delay/duplication/reordering.
3. **Run experiments in production** — *deliberately not adopted here.* This is the
   one principle that does not transfer: this is a learning project with no
   production, and injecting faults into a real ledger is not defensible. We run
   chaos against a deterministic simulated network instead.
4. **Automate experiments to run continuously** — chaos tests are ordinary `go test`
   cases in CI, not a manual exercise.
5. **Minimize blast radius** — the simulator is the blast radius by construction.

**Primary sources:**
- **principlesofchaos.org** — the canonical five principles, quoted above.
- Netflix Technology Blog, *"The Netflix Simian Army"* (2011) — the original
  Chaos Monkey / Latency Monkey / Chaos Gorilla writeup.
  *(Note: netflixtechblog.com returned HTTP 403 to automated fetching on
  2026-08-29 — read it in a browser.)*
- **github.com/Netflix/chaosmonkey** — the real implementation, plus its docs.
- Basiri et al., *"Chaos Engineering"*, IEEE Software 33(3), 2016 — the peer-reviewed
  writeup of the practice.

**Determinism caveat [project decision]:** our chaos is seeded and reproducible. A
failing run must be replayable from its seed — an unreproducible consensus bug is
close to impossible to fix. This is a deliberate divergence from Netflix's genuinely
random production injection, and it is the right trade for a project whose goal is
understanding rather than uptime.

---

## Not yet logged (topics to add as they come up)

- Sharding / consistent hashing (Phase 2)
- Two-Phase Commit / cross-shard transactions (Phase 2)
- Hybrid Logical Clocks (Phase 3)
- Event sourcing (Phase 3)
