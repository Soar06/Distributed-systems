# Reading List — Distributed Systems Theory

Every theory topic behind this project, logged with what it actually solves and
the primary source(s) to go read. Required by [Agents/RULES.md](../Agents/RULES.md)
rule #1 — this is a map from "knowledge used in this project" back to "where it
came from," not a restatement of the source material itself.

Entries are added as new topics come up. The numbering is the order they were
learned in, which is roughly the order they were needed.

## Where each topic lives in the code

> For the function-by-function version — which exact function implements which
> rule of Figure 2, which test holds each of Figure 3's five safety properties,
> and where this project deliberately departs from the paper — see
> **[CODE_MAP.md](CODE_MAP.md)**. The table below is the index; that document is
> the detail.

All four phases and the G1-G7 hardening sequence are complete, so every entry
below now has an implementation to read alongside the paper. That pairing is the
point of this file: the source says what the idea is, the code says what it cost
to make it true.

| # | Topic | Implemented in |
|---|---|---|
| 1 | Consensus (Raft) | `raft/` — Figure 2 in full |
| 2 | Failure model + consistency | the whole design; `sim/` asserts it |
| 3 | CAP theorem | `obs/` readiness: a node that cannot commit says so |
| 4 | Paxos (comparison only) | not implemented, deliberately |
| 5 | Linearizable reads (§8) | `raft/read.go` — ReadIndex |
| 6 | Chaos engineering | `sim/chaos_test.go`, and `demo/` with a human in the loop |
| 7 | WAL, fsync, crash recovery | `storage/` |
| 8 | Idempotency / exactly-once | `ledger/` keys + request fingerprints |
| 9 | Event sourcing | `ledger.State.history` — balances are a fold over it |
| 10 | Double-entry bookkeeping | `ledger/` sum-to-zero invariant, `VerifyDoubleEntry` |
| 11 | Sharding + consistent hashing | `shard/ring.go` |
| 12 | Two-Phase Commit | `shard/twopc.go`, `shard/coordinator.go` |
| 13 | Auth + mutual TLS | `rpc/security.go` |
| 14 | RPC multiplexing | `rpc/shardnode.go` — one listener, many Raft groups |
| 15 | Snapshotting + log compaction | `raft/snapshot.go`, `ledger/snapshot.go`, `shard/snapshot.go` |
| 16 | Observability, liveness vs readiness | `obs/`, `raft/health.go` |
| 17 | Membership changes | `raft/membership.go` |
| 18 | Backpressure + graceful shutdown | `rpc/admission.go`, `rpc/drain.go` |
| 19 | Hybrid Logical Clocks | `hlc/` |
| 20 | Observing a live cluster | `demo/`, `fe/` |

## What the theory cost in practice

Six findings worth carrying forward, because none of them are in the papers:

1. **A test that passes by doing nothing is worse than no test.** `sim.CountingSM`
   had a `Snapshot() []string` convenience method that COLLIDED with
   `raft.Snapshotter`'s required signature, so it never satisfied the interface —
   `MaybeCompact` silently declined for every simulator cluster and every
   compaction test passed while compacting nothing. Assert the interface with
   `var _ Iface = (*T)(nil)`.

2. **"This shouldn't happen" branches must fail loudly.** The production-hardening
   audit found the same defect everywhere: recovery guessing ABORT when it could
   not reach the coordinator, `CommitDebit` returning OK holding no reservation,
   an outcome applied for a transaction never prepared. For a system whose rule is
   that no scenario may create or destroy a cent, "return success and move on" is
   the inverted default.

3. **The dangerous states are the ones that look healthy.** A partitioned leader
   keeps its role, answers RPCs, and reports a term while committing nothing.
   That is why `/readyz` exists separately from `/healthz`, and why
   `ShardGroup.leader()` has to compare terms — two nodes genuinely report Leader
   after a failover.

4. **Timing bugs masquerade as flakes.** Election timeouts of 60-160ms are tight
   enough that the race detector's instrumentation alone breaks §5.2's inequality,
   producing failures on a different test each run. The signature — "leadership
   lost", "not leader", "no leader" — is a timing artifact, not a correctness
   defect, and the fix is to widen the harness rather than to retry past it.

5. **A snapshot must capture everything derived from the log, not just the
   obvious part.** Balances are the obvious part. Idempotency results, request
   fingerprints, fund reservations, and 2PC promises are the parts whose absence
   only shows up as money, long after compaction discarded the evidence.

6. **Distinguish "it failed" from "the outcome is unknown".** `Indeterminate`
   means the entry may yet commit and must be retried with the SAME key; `Busy`
   means nothing was proposed. Collapsing them is how a transfer gets
   double-sent, and it is the single contract this project defends hardest.

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

## 7. Write-ahead logging, fsync, and crash recovery

**What the theory does:** Makes a change survive a crash. A write that has only
reached the OS page cache is *not* durable — a power loss discards it — so the
system must force it to stable storage (`fsync`) and only then acknowledge. The
write-ahead rule is: record the intent durably *before* acting on it, so recovery
can always tell what was promised.

Why it matters here: Figure 2's requirement that persistent state be saved "before
responding to RPCs" is a WAL discipline, and it is a *safety* requirement, not a
performance note. A server that grants a vote, crashes, forgets, and votes again in
the same term produces two leaders — Election Safety gone. Our `storage/` package is
this theory applied: length-prefixed CRC32-checksummed records, fsync before return,
and torn-write detection so a half-written record is truncated rather than replayed.

**The specific hazards this theory names, all of which `storage/` handles:**
- **Torn writes** — a process killed mid-append leaves a partial record;
  indistinguishable from corruption, so it must be dropped, never replayed.
- **Checksums** — the only way to tell a good record from a plausible-looking bad
  one. Length fields alone are not enough; a corrupt length is itself an attack on
  the reader.
- **fsync is not free and not optional** — the durability/latency trade-off is real,
  and skipping it is the most common way a "durable" system silently is not.

**Primary sources:**
- Kleppmann, *Designing Data-Intensive Applications*, **Chapter 3** ("Storage and
  Retrieval") — log-structured storage and the crash-recovery argument. Same book as
  [[#2]]; this is a different chapter.
- Mohan et al., *"ARIES: A Transaction Recovery Method..."* (1992) — the canonical
  WAL/recovery paper. Heavier than this project needs, but it is where write-ahead
  logging is properly specified.
- Pillai et al., *"All File Systems Are Not Created Equal: On the Complexity of
  Crafting Crash-Consistent Applications"* (OSDI '14) — an empirical catalogue of how
  real applications get fsync and crash-consistency wrong. The practical companion.
- Raft extended paper §2 and Figure 2 ("Updated on stable storage before responding
  to RPCs") — the requirement as it applies to us.

**[project decision]** `storage.RaftState.Save` rewrites the whole state per persist:
obviously correct, O(log size) per append. The incremental-records-plus-compaction
version is the optimization, and it belongs with snapshotting (LATER.md) — not before
the simple version is proven.

---

## 8. Idempotency and exactly-once semantics

**What the theory does:** Reconciles an uncomfortable fact — a client that does not
receive a reply *cannot know* whether its request was executed. The network may have
dropped the request, or executed it and dropped the response. Retrying is therefore
mandatory and dangerous at the same time. Idempotency keys make a retry safe: the
server recognizes the repeat and returns the original outcome instead of executing
again.

Why it matters here: this is the difference between a bank and a broken bank. Raft
guarantees the *log entry* is not duplicated, but that is not enough — the retry
arrives as a brand-new client request, and only an application-level key can tie it
back to the original. §8 is explicit that Raft alone does not give exactly-once:

> "as described so far Raft can execute a command multiple times: for example, if the
> leader crashes after committing the log entry but before responding to the client,
> the client will retry the command with a new leader, causing it to be executed a
> second time. The solution is for clients to assign unique serial numbers to every
> command. Then, the state machine tracks the latest serial number processed for each
> client, along with the associated response."

**The subtle rule we implement:** a retry returns the **original** result, even if
re-evaluating now would give a different answer (the balance may have changed since).
Re-evaluating on retry would make the outcome depend on *when* the retry arrived —
non-deterministic, and therefore fatal inside a replicated state machine.

**Primary sources:**
- Raft extended paper **§8** ("Client interaction") — the quote above; the design we
  implement in `ledger/`.
- Kleppmann, *DDIA*, **Chapter 11** ("Stream Processing"), on exactly-once /
  effectively-once semantics and why end-to-end deduplication is required.
- Helland, *"Idempotence Is Not a Medical Condition"* (ACM Queue, 2012) — the clearest
  practical treatment of why at-least-once delivery plus idempotence is the realistic
  target, rather than true exactly-once delivery.

---

## 9. Event sourcing

**What the theory does:** Stores the append-only sequence of *events* as the system
of record, and treats current state as a fold over that sequence rather than as the
primary data. Nothing is updated in place; the history is immutable and complete.

Why it matters here: this is the same structure Raft's log already is, arrived at
from the domain side. That convergence is the point — NOW.md's Phase 3 note that the
event-sourced ledger "also doubles as the replication log" is not a coincidence but
the same idea twice. For a bank it is also simply correct: the audit trail *is* the
data, and a balance that cannot be re-derived from its transactions is unauditable.

**Status correction:** this was queued as Phase 3 but was in fact implemented in
Phase 1 — `ledger.State` keeps `history []Transaction` as the record, `Balances()` is
a cache of the fold, and `VerifyDoubleEntry()` re-derives balances from history to
prove the cache never drifts. What remains for Phase 3 is hardening, not introducing
it.

**Primary sources:**
- Martin Fowler, *"Event Sourcing"* (martinfowler.com, 2005) — the canonical
  definition.
- Young, *"CQRS Documents"* — event sourcing paired with CQRS, the read/write split
  that follows naturally from it.
- Kleppmann, *DDIA*, **Chapter 11** — event logs as the system of record, and the
  relationship between event sourcing and change data capture.

---

## 10. Double-entry bookkeeping

**What the theory does:** Records every transaction as matched debits and credits
that sum to zero, so money can be moved but never created or destroyed by a transfer.
It is 500-year-old accounting, not distributed systems — logged here because it is the
domain invariant the whole ledger is built to protect, and because it gives us an
assertion (`entries sum to zero`) rather than a hope.

Why it matters here: it converts "no money was lost" from a thing we believe into a
thing the code checks. `ledger.Transaction.balances()` enforces it per transaction and
panics on violation; `VerifyDoubleEntry()` audits the whole history.

**Primary sources:**
- Pacioli, *Summa de Arithmetica* (1494) — the original description. Of historical
  interest; not worth reading for implementation.
- Martin Kleppmann, *"Accounting for Computer Scientists"* (2011, blog) — the
  practical bridge: double-entry explained for engineers, in terms of immutable
  append-only events. The one to actually read.

---

## 11. Sharding and consistent hashing (Phase 2)

**What the theory does:** Splits data across independent groups so the system can
scale writes. Phase 1 proved the constraint hands-on — 3 nodes 119.9 tx/s vs 5 nodes
105.9 tx/s — because every write funnels through one leader. Adding replicas buys
fault tolerance, never write throughput. **Sharding is the only thing that adds write
capacity**, because separate shards have separate leaders committing in parallel.

Consistent hashing decides *which* shard owns a key. Naive `hash(key) % N` is fatal
here: changing N remaps nearly every key, which for a bank means moving nearly every
account at once. Consistent hashing maps both keys and nodes onto a circle, assigning
each key to "the next server that appears on the circle in clockwise order," so
"the addition of the nth server only causes 1/n fraction of the BLOBs to relocate."

**Virtual nodes** solve the follow-on problem: a handful of nodes placed randomly on
a circle divide it unevenly, so some shards get far more keys than others. Virtual
nodes are "multiple labels which point to a single real server," smoothing the
distribution.

Real systems using it: Amazon Dynamo, Cassandra, Riak, Akamai, Discord, Couchbase.

**[project decision]** Phase 2 shards by **account ID**, and each shard is its own
independent Raft group — the design NOW.md names (CockroachDB/Spanner/Vitess do the
same). Consequence to internalize: a transfer *within* a shard stays a single-group
Raft commit, while a transfer *across* shards needs atomic commitment across two
independent logs — which is why 2PC ([[#12]]) is the very next entry.

**Primary sources:**
- Karger et al., *"Consistent Hashing and Random Trees: Distributed Caching Protocols
  for Relieving Hot Spots on the World Wide Web"* (MIT, STOC 1997) — the original.
- DeCandia et al., *"Dynamo: Amazon's Highly Available Key-value Store"* (SOSP 2007)
  — consistent hashing plus virtual nodes in production, and honest about the
  trade-offs.
- Kleppmann, *DDIA*, **Chapter 6** ("Partitioning") — partitioning strategies,
  rebalancing, and request routing. The most directly useful for building this.

---

## 12. Two-Phase Commit (2PC) — atomic commitment across shards (Phase 2)

**What the theory does:** Makes a transaction spanning several independent systems
either commit everywhere or abort everywhere — never half. A coordinator asks every
participant to *prepare* (phase 1); if all vote yes it tells them to *commit*
(phase 2). A participant that votes yes has made a **promise it cannot retract**, even
across a crash — which is why its prepare must be durable before it answers.

Why it matters here: once accounts are sharded, moving money from a shard-A account
to a shard-B account touches two separate Raft groups. Each group can commit its own
half perfectly and the transfer still be catastrophically wrong — debit committed,
credit lost. Raft gives atomicity *within* one group; 2PC is what extends it across
groups.

**The hard part, which is the actual Phase 2 lesson:** 2PC is a **blocking** protocol.
If the coordinator crashes after participants voted yes but before delivering the
decision, those participants are stuck holding locks, unable to commit or abort
unilaterally — the *in-doubt* state NOW.md calls out. This is not an implementation
bug to fix; it is inherent, and it is why 2PC has the reputation it has. Real systems
mitigate it by making the coordinator itself fault-tolerant — in our case, by
replicating the coordinator's decisions through Raft, which is exactly what Spanner
and CockroachDB do.

**Sagas** are the eventual-consistency alternative: a sequence of local transactions
with compensating actions to undo. NOW.md deliberately rejects sagas as the primary
path for money, since a compensation is a *new* transaction, not a rollback — the
intermediate wrong state was really visible. Logged here as the road not taken.

**How Spanner actually does it** (verified against the OSDI 2012 paper, quoted). This
is the design Phase 2 implements, because it is what a real system does:

> "One of the participant groups is chosen as the coordinator: the participant leader
> of that group will be referred to as the coordinator leader."

So there is **no external coordinator process** — one of the shards involved in the
transfer takes the role. Then:

> "A non-coordinator-participant leader first acquires write locks. It then chooses a
> prepare timestamp... and **logs a prepare record through Paxos**."

> "The coordinator leader also first acquires write locks, but skips the prepare
> phase... The coordinator leader then **logs a commit record through Paxos** (or an
> abort if it timed out while waiting on the other participants)."

> "After commit wait, the coordinator sends the commit timestamp to the client and all
> other participant leaders. **Each participant leader logs the transaction's outcome
> through Paxos.**"

And the reason this matters, stated directly:

> "Some authors have claimed that general two-phase commit is too expensive to
> support, because of the performance or availability problems that it brings.
> **Running two-phase commit over Paxos mitigates the availability problems.**"

**[project decision]** We implement exactly this shape, substituting Raft for Paxos:
every 2PC state transition — prepare vote, commit/abort decision, and outcome — is a
replicated log entry in the relevant Raft group, never in-memory state. A coordinator
that crashes is replaced by its own group's next leader, which recovers the decision
from its log. An in-memory coordinator would be a toy: it could not survive the very
failure the protocol exists to handle.

Note we do **not** implement Spanner's TrueTime/commit-wait. That solves external
consistency across geographies using hardware clock bounds; Phase 2 is single-machine,
and the ordering problem it addresses is Phase 3's HLC topic.

**Primary sources:**
- Corbett et al., *"Spanner: Google's Globally-Distributed Database"* (OSDI 2012),
  **§2.1 and §4.2.1** — the quotes above; 2PC layered over Paxos groups.
- Gray, *"Notes on Data Base Operating Systems"* (1978) — where 2PC is first laid out.
- Bernstein, Hadzilacos & Goodman, *Concurrency Control and Recovery in Database
  Systems* (1987), **Chapter 7** — the rigorous treatment of atomic commitment,
  including why the blocking problem is unavoidable.
- Kleppmann, *DDIA*, **Chapter 9** ("Consistency and Consensus"), the "Atomic Commit
  and Two-Phase Commit" section — clearest modern explanation, and explicit about
  coordinator failure and in-doubt transactions. Same chapter as [[#2]].
- Garcia-Molina & Salem, *"Sagas"* (SIGMOD 1987) — the alternative we are not taking.

---

## 13. Authentication, mutual TLS, and the trusted-network fallacy (G4)

**What the theory does:** Establishes *who* is on the other end of a connection, and
keeps a third party from reading or altering what crosses it. For a consensus system
this is not a generic security checkbox — it is a **correctness** requirement, because
Raft's safety proofs assume every message came from a genuine cluster member. The
paper says so directly (§2, "Byzantine" is explicitly out of scope): Raft tolerates
crashes and network faults, *not* participants that lie. An unauthenticated port turns
"no Byzantine faults" from an assumption into a wish.

**Why it matters here, concretely:** both our ports speak unauthenticated plaintext
gob. Anyone who can reach the inter-node port can send `AppendEntries` with a term
higher than the cluster's current one. Figure 2's unconditional rule — "if RPC request
or response contains term T > currentTerm, set currentTerm = T, convert to follower" —
means every honest node *must* obey it. The attacker becomes leader by the protocol's
own rules and can then rewrite the ledger. No amount of Raft correctness helps: the
implementation is behaving exactly as specified, against an adversary the spec never
contemplated.

**The fallacy being corrected** is the "trusted network" / perimeter model: the idea
that being inside the datacenter makes a caller legitimate. Google's BeyondCorp papers
are the canonical repudiation — trust is established per *request*, from credentials,
never from network location. Real banking systems are additionally required to do
this by regulation (PCI-DSS §4 requires encrypting cardholder data in transit over
open networks; §8 requires identifying and authenticating access).

**Mutual TLS vs. one-way TLS.** Ordinary HTTPS authenticates only the *server*: the
client verifies it reached the right host, and the server has no idea who called.
Inter-node Raft traffic needs the opposite property too — a node must know the peer
sending `AppendEntries` really is the peer it claims to be. mTLS gives both sides a
certificate, and the peer's identity becomes its certificate subject rather than a
self-asserted field in the message. This is what closes forged `AppendEntries`
*structurally*: the forgery is rejected at the transport, before any Raft code runs.

**[project decision]** Two distinct trust relationships, deliberately not merged:
- **Node↔node: mutual TLS.** The node id in `-peers` must match the certificate's
  subject Common Name. A valid certificate for the *wrong* node id is rejected —
  otherwise any cluster member could impersonate any other, and a compromised
  follower could forge a leader's messages.
- **Client→node: a bearer token**, checked before any command is proposed. Deliberately
  *not* per-account authorization; that is application-level and belongs with the bank
  domain, not the consensus layer. The goal here is only that unauthorized callers
  cannot reach the cluster at all.

**A related hardening point that is not about TLS:** `encoding/gob` decodes directly
into Go types and its decoder is not written to be adversary-facing — a hostile stream
can drive allocation before any of our code sees it. TLS keeps strangers out but does
nothing about a *compromised* client, so the framing itself must validate lengths
before allocating. `shard.Command.Encode`/`DecodeCommand` already work this way and
are the in-repo model.

**Primary sources:**
- Ongaro & Ousterhout, *Raft (extended)*, **§2** — the explicit statement that Raft
  assumes non-Byzantine participants. This is the sentence that makes authentication a
  correctness concern rather than a security nicety.
- Rescorla, *"The Transport Layer Security (TLS) Protocol Version 1.3"*,
  **RFC 8446** — the protocol itself; §4.4.2 covers client certificates (mTLS).
- Ward & Beyer, *"BeyondCorp: A New Approach to Enterprise Security"* (;login: 2014),
  and the follow-on papers — the repudiation of perimeter/trusted-network security.
- Saltzer & Schroeder, *"The Protection of Information in Computer Systems"* (1975) —
  the design principles, especially *complete mediation* (every access checked) and
  *fail-safe defaults* (deny unless permitted), both of which our current ports violate.
- PCI-DSS v4.0, **Requirements 4 and 8** — why a real bank has no discretion here.
- Go stdlib `crypto/tls`, `Config.ClientAuth` (`RequireAndVerifyClientCert`) — the
  implementation surface; no third-party dependency needed.

---

## 14. RPC multiplexing: many Raft groups over one transport (G1)

**What the theory does:** Lets one process host replicas of *many* independent
consensus groups over a single listener and a single connection per peer, by tagging
every message with the group it belongs to and demultiplexing on arrival. It is
plumbing rather than deep theory, but getting it wrong breaks shard independence,
which *is* the theory Phase 2 exists to demonstrate.

**Why it matters here:** Phase 2 proved sharding works with each shard on its own
in-process network. A real deployment cannot give every shard its own TCP port and its
own connection to every peer — with S shards and N nodes that is S×N connections per
process, and the per-connection cost is what starts dominating. Real systems solve
this the same way: **CockroachDB** multiplexes many Ranges (each its own Raft group)
over shared node-to-node gRPC connections, and coalesces heartbeats across groups so
that idle Ranges cost nothing; **TiKV** does the same for its Regions under the name
Multi-Raft.

**The property that must be preserved:** shards are independent failure and
performance domains. Multiplexing them onto one transport creates a shared resource
they did not previously share, so it can *introduce* coupling that the sharding design
specifically claims does not exist. Two consequences worth stating before writing code:
- A slow or blocked handler for shard A must not stall shard B's messages on the same
  connection. Our existing transport already learned this lesson once, at a different
  layer: dialing under the shared mutex let one dead peer stall every other peer's
  RPCs (measured 2.95s to a *healthy* peer), and dropping a connection on timeout
  killed 91 of 200 concurrent healthy calls.
- Per-group heartbeats multiply: S groups × N peers × heartbeat rate. This is exactly
  why CockroachDB coalesces them. We are not implementing coalescing yet, but the cost
  is real and should be measured in G2 rather than discovered later.

**[project decision]** Multiplex by **`net/rpc` service name** — each hosted shard
registers as `Raft.<shard-id>` — rather than inventing a framed header. `net/rpc`
already demultiplexes by service name and by sequence number over one connection, and
it already runs each call in its own goroutine, which gives us the non-blocking
property above for free. This keeps the change to *registration and addressing*, and
touches no consensus code — the same property that made `raft.Transport` worth
abstracting in the first place.

**Primary sources:**
- Taft et al., *"CockroachDB: The Resilient Geo-Distributed SQL Database"*
  (SIGMOD 2020), **§2** — Ranges as independent Raft groups, and the node-level
  transport shared across them.
- CockroachDB design docs / source on **coalesced heartbeats** — the concrete
  mitigation for per-group heartbeat multiplication.
- Huang et al., *"TiDB: A Raft-based HTAP Database"* (VLDB 2020) — Multi-Raft over
  Regions, the same pattern in a second independent system.
- Ongaro & Ousterhout, *Raft (extended)*, **§9.1** — why one Raft group per partition
  is the standard way to scale beyond a single group's throughput.
- Go stdlib `net/rpc` — `RegisterName`, and its per-call goroutine and sequence-number
  demultiplexing, which is what makes the service-name approach sufficient here.

---

## 15. Snapshotting and log compaction (§7, G3)

**What the theory does:** Bounds the cost of a replicated log that would otherwise
grow forever. A consensus log is append-only by construction — that is what makes
it the authoritative history — but the state it describes is usually far smaller
than the history that produced it. Snapshotting writes the *state* to stable
storage, records which log entry it covers, and discards everything up to that
point. The paper states the problem plainly: "as the log grows longer, it occupies
more space and takes more time to replay."

**Why it matters here, with the number:** `storage.RaftState.Save` rewrites the
entire persistent state on every persist. That is O(n) bytes per append and
therefore O(n²) over a run — measured at **481x write amplification by 800
entries**. This is a wall, not a slope: nothing degrades gracefully, the node
simply stops keeping up. It also makes restart time unbounded, since a restarting
node replays its whole history, which is why cluster membership changes depend on
snapshotting landing first (a newly added server has to catch up in bounded time
or adding it temporarily *reduces* availability).

**The mechanism (§7):**
- Each server snapshots **independently**. There is no cluster-wide coordination:
  a snapshot is a purely local optimization of a log every server already holds.
- The snapshot records `lastIncludedIndex` and `lastIncludedTerm`. These are not
  bookkeeping — they are what lets `AppendEntries` still answer a
  `prevLogIndex`/`prevLogTerm` consistency check for the entry immediately before
  the snapshot. Without them the log is unmatched at its own boundary and
  replication stalls permanently.
- A follower that has fallen behind the leader's discarded prefix cannot be caught
  up by `AppendEntries` — the entries it needs no longer exist — so the leader
  sends **`InstallSnapshot`** (Figure 13) instead.
- Compaction may only cover entries the state machine has **applied**, never
  merely committed. A snapshot is a picture of the state machine; an entry that is
  committed but unapplied would be lost entirely, gone from the log and absent
  from the snapshot.

**The hard part, which is not in the Raft paper at all:** the snapshot must
capture *everything* the state machine derived from the log. Anything omitted is
silently destroyed the moment the prefix is discarded — and no amount of Raft
correctness catches it, because the log is still perfect. For this project that
means balances **and** idempotency results **and** request fingerprints **and**
fund reservations **and** the 2PC transaction records. The reservations and the
2PC records are the dangerous ones: losing them makes a participant forget an
unretractable promise and frees money already committed to another transaction.

**[project decision]** The snapshot is sent whole rather than in Figure 13's
offset-chunked form. Chunking exists because a snapshot may be very large; at this
project's scale one fits comfortably in a single message, and chunking would add
partial-transfer state needing its own correctness argument. Field names still
mirror Figure 13 so the code reads against the paper.

**[project decision]** Snapshots live in a separate file from the Raft state file.
The state file is rewritten on every persist, so folding a snapshot into it would
multiply exactly the write amplification snapshotting exists to remove.

**Primary sources:**
- Ongaro & Ousterhout, *Raft (extended)*, **§7 and Figure 13** — the mechanism and
  the `InstallSnapshot` RPC. §7 also covers the two questions that decide the
  implementation: when to snapshot, and how to avoid the snapshot competing with
  normal operation.
- Ongaro, *"Consensus: Bridging Theory and Practice"* (PhD dissertation, 2014),
  **Chapter 5** — the fuller treatment, including incremental/copy-on-write
  approaches and snapshotting for state machines too large to serialize whole.
- Chandra, Griesemer & Redstone, *"Paxos Made Live"* (PODC 2007) — the same
  problem in a production Paxos system, and specifically the engineering around
  taking a snapshot without stalling the replica.

---

## 16. Observability: liveness vs readiness, and the degraded-quorum blind spot (G5)

**What the theory does:** Makes the internal state of a distributed system
externally visible, so that "is it working?" is a question with an answer rather
than an inference. The distributed-systems content is not the metrics plumbing —
it is *which* states are distinguishable from outside, and the discovery that the
most dangerous states are the ones that look healthy.

**Why it matters here, concretely:** a Raft cluster that has lost quorum still
accepts TCP connections, still answers RPCs, still reports a role, and still has a
leader that believes it is the leader. Every superficial signal is green. What it
cannot do is commit anything. From outside, a cluster committing against a
degraded quorum is indistinguishable from a healthy one — and for a bank that
means writes that appear accepted and are not, which is the same class of failure
as the `Indeterminate` hazard in the client contract.

**The distinction that carries the weight: liveness vs readiness.**
- **Liveness** ("am I alive?") asks whether the process should be restarted. A
  live process that is stuck is a candidate for a kill.
- **Readiness** ("should traffic come to me?") asks whether this instance can
  *serve*. A node that is perfectly alive but cannot reach quorum is live and NOT
  ready.

Conflating them produces two distinct and opposite failures, which is why the
split exists at all:
- A readiness check wired to liveness **restarts a node that was merely waiting**
  — during a partition, that turns a recoverable degradation into a restart storm
  that destroys the cluster's remaining quorum.
- A liveness check wired to readiness **keeps routing traffic to a node that
  cannot commit** — the degraded-quorum blindness above.

Kubernetes formalized this split (liveness/readiness/startup probes) and its
documentation is explicit that using the wrong one is a common and damaging
mistake. The underlying idea predates it: it is the difference between a process
being *up* and a service being *available*, which is exactly the distinction CAP
forces a partitioned system to confront.

**What to measure, and the reason for each:** Brendan Gregg's USE method (for
resources: Utilization, Saturation, Errors) and Tom Wilkie's RED method (for
services: Rate, Errors, Duration) are the standard answers. For a consensus system
the load-bearing signals are the ones that reveal *disagreement*:
- role and current term per node — term churn is leadership instability, the
  liveness cost §5.2's timing inequality warns about
- commitIndex vs lastApplied — a growing gap means the state machine is behind
- log length — the input to the compaction decision (§15)
- in-doubt transaction count — 2PC's blocking, made visible rather than inferred
- apply and persist error counts — "this shouldn't happen" branches, counted
  instead of silently swallowed

**On structured logging:** the point is not tidiness, it is *correlation*. A
sharded multi-process cluster produces interleaved logs from many Raft groups
across many processes; a line that does not carry (node, shard, term, index)
cannot be placed in the sequence of events it belongs to. Go's `log/slog` is
stdlib, so this costs no dependency.

**[project decision]** Prometheus **text exposition format** emitted by hand
rather than via the client library. The format is a few lines of text, the metric
set here is small and fixed, and the project's zero-dependency rule is worth more
than the library's convenience. This is the same reasoning that chose `net/rpc`
over gRPC in Phase 1.

**[project decision]** The HTTP port is separate from the RPC port. An
observability endpoint that shares the consensus port cannot be scraped while the
consensus path is saturated — which is exactly when the metrics are needed. It
also keeps the auth story clean: the RPC port requires mutual TLS (§13), while
metrics are read by a scraper that holds no cluster credentials.

**Primary sources:**
- Beyer, Jones, Petoff & Murphy, *Site Reliability Engineering* (O'Reilly 2016),
  **Chapter 6, "Monitoring Distributed Systems"** — the four golden signals
  (latency, traffic, errors, saturation), and the argument for alerting on
  symptoms rather than causes.
- Kubernetes documentation, *"Configure Liveness, Readiness and Startup Probes"* —
  the canonical statement of the liveness/readiness split and of the failure modes
  from confusing them.
- Gregg, *"The USE Method"* (2012), and Wilkie's **RED method** — the two standard
  answers to "which metrics actually matter".
- Prometheus documentation, *"Exposition formats"* — the text format this emits.
- Ongaro & Ousterhout, *Raft (extended)*, **§5.2** — why term churn is the signal
  that matters for leadership stability, and why it costs liveness but never
  safety.

---

## 17. Cluster membership changes (Raft §6 and dissertation §4.1, G6)

**What the theory does:** Lets the set of servers in a consensus cluster change —
replacing a dead machine, growing from three nodes to five — **while the cluster
keeps serving**. Without it, replacing a failed node means stopping every node,
editing a config file, and starting again: full-cluster downtime to recover from a
single-node failure, which inverts the whole point of running consensus.

**Why it is dangerous, and the reason the paper devotes a section to it:**
switching directly from an old configuration to a new one is unsafe **because
servers cannot all make the switch at the same instant.** During the changeover
the cluster is briefly two different clusters, and if the old and new
configurations have disjoint majorities, both can elect a leader in the same term.
That is Election Safety violated — the property everything else rests on. The
paper's Figure 10 shows exactly this: going 3→5, `{S1,S2}` is a majority of the
old configuration while `{S3,S4,S5}` is a majority of the new one, so two leaders
are elected for the same term.

**Two solutions, and why this project picks the second.**

*Joint consensus* (§6, the extended paper). The cluster transitions through an
intermediate configuration C-old,new in which agreement requires **separate
majorities of both** the old and new configurations. Since no decision can be made
without both, disjoint majorities are impossible. It is fully general — it can
replace an arbitrary set of servers at once — at the cost of a two-phase
transition with its own log entries and its own edge cases.

*Single-server changes* (dissertation §4.1). Add or remove **one server at a
time.** The safety argument is a counting one and is worth stating precisely,
because it is what makes the simpler mechanism correct: when the configuration
differs by one server, any majority of the old configuration and any majority of
the new one **must overlap in at least one server**, and a single server never
votes twice in one term. So two disjoint majorities cannot exist and joint
consensus is unnecessary. Ongaro's own dissertation adopts this as the primary
approach, and it is what production Raft implementations actually use (etcd,
Consul); a cluster needing a bigger change makes several one-at-a-time changes.

**[project decision]** Single-server changes. Simpler, sufficient, and what real
systems do. The extended paper's joint consensus is recorded here as the more
general mechanism we are deliberately not implementing.

**The counter-intuitive rule that is the classic bug:** a configuration change
takes effect **when the entry is APPENDED to the log, not when it is committed.**
A server uses the latest configuration in its log regardless of commit status. The
paper is explicit, and getting it backwards is the standard membership bug —
because if servers waited for commit, they would be voting under the old
configuration while the leader counted under the new one, which is precisely the
disjoint-majority hazard the design exists to prevent.

**Three problems that only show up once you build it**, all from dissertation §4:

1. **A new server starts with an empty log.** Counting it toward quorum
   immediately makes the cluster *less* available: a 3-node cluster becoming
   4-node needs 3 votes instead of 2, and the new server can supply none until it
   catches up. The fix is to add it as a **non-voting learner** first, replicate
   until it is close, and only then count it. This is why membership depends on
   snapshotting (§15) landing first: `InstallSnapshot` is what makes catch-up
   bounded rather than proportional to the whole history.

2. **Removing the leader.** A leader that is removing itself must keep serving
   until the change commits — it is still the leader — and then step down. It also
   must not count itself in the new configuration's majority.

3. **A removed server disrupts the cluster.** No longer receiving heartbeats, it
   times out, increments its term, and sends `RequestVote`. Its higher term forces
   the real leader to step down even though the caller is not a cluster member any
   more. The paper's answer is the **minimum-election-timeout check**: a server
   ignores `RequestVote` received within the minimum election timeout of hearing
   from a current leader. That single rule is what stops a departed member from
   repeatedly deposing a healthy leader.

**Primary sources:**
- Ongaro & Ousterhout, *Raft (extended)*, **§6 and Figure 10/11** — the safety
  problem, joint consensus, and the "takes effect on append, not on commit" rule.
  Figure 10 is the picture of the disjoint-majority hazard.
- Ongaro, *"Consensus: Bridging Theory and Practice"* (PhD dissertation, 2014),
  **Chapter 4** — single-server changes as the primary mechanism, the overlap
  argument that makes them safe, learners/catch-up, removing the leader, and the
  disruptive-server problem with the minimum-election-timeout answer.
- etcd and HashiCorp Consul source/design docs — two independent production
  systems that use single-server changes rather than joint consensus, which is the
  practical evidence for the choice.

---

## 18. Backpressure, load shedding, and graceful shutdown (G7)

**What the theory does:** Decides what a system does when demand exceeds what it
can serve. Every system has a capacity ceiling; the only choice is whether it hits
that ceiling by *degrading predictably* or by *collapsing*. Backpressure is the
mechanism for choosing the first.

**The counter-intuitive result, and the reason this is theory rather than
plumbing: a bounded queue that REJECTS is more available than an unbounded one
that accepts everything.** An unbounded queue does not remove the limit, it
converts the limit from a visible rejection into invisible latency. Requests still
arrive faster than they are served, so the queue grows, and every response takes
longer — until responses arrive after the client has already given up. The system
is then doing 100% of the work for 0% useful output. Nygard calls the failed state
*congestion collapse*; the queue is "unbounded" only until memory runs out, at
which point the failure is a crash rather than a rejection.

**Why it is sharper for a consensus system.** The leader is the bottleneck **by
construction** — every write funnels through it (measured in Phase 1: 3 nodes
119.9 tx/s vs 5 nodes 105.9 tx/s). Adding replicas does not help; only sharding
does. So a Raft leader cannot shed load by scaling out mid-incident, and the queue
in front of it is the only control.

**And sharper still for a bank.** A client that times out on a write whose entry
still commits is precisely the `Indeterminate` hazard in this project's client
contract: the outcome is unknown, the money may or may not have moved, and a
client that records it as "did not happen" and reissues under a new key
double-sends. An unbounded queue manufactures that hazard at scale, because every
overloaded request becomes a timeout on an entry that may still commit. A `Busy`
rejection is strictly better information: nothing was proposed, so retrying is
unambiguously safe.

**Little's Law** gives the quantitative version: L = λW. With arrival rate λ
exceeding service rate μ, queue length L and therefore wait W grow without bound.
Bounding L is what bounds W — the queue limit *is* the latency limit.

**Load shedding vs. rate limiting** — related but distinct, and worth not
conflating:
- **Rate limiting** is a *policy* applied per client: this caller may issue N
  requests per second, regardless of whether the system is busy. It enforces
  fairness, so one aggressive client cannot consume the whole budget.
- **Load shedding** is a *reaction* to the system's own state: refuse work when
  saturated, whoever is asking. It protects the system.

A token bucket is the standard rate-limiting mechanism: tokens accumulate at a
fixed rate up to a burst capacity, and each request spends one. The burst
allowance is what makes it usable — real traffic is bursty, and a strict
requests-per-second limit rejects legitimate spikes that the system could easily
absorb.

**Graceful shutdown** is the same concern at the end of a process's life. A node
that exits abruptly leaves clients holding requests whose outcome is unknown —
manufacturing `Indeterminate` again, this time deliberately. The ordered
alternative: stop accepting new work, finish what is already committed, persist,
then close. This project already learned half of it the hard way: the
phantom-quorum bug was a node that kept *answering* after it should have stopped,
so `raft.Server` already latches `stopped` and `rpc.Server.Close` closes
established connections. Draining extends that path rather than inventing one.

**[project decision]** Reject with a typed `Busy` outcome rather than blocking the
caller. Blocking moves the queue from the server into the client's connection
pool, where it is invisible to both — the server looks healthy, and the client
looks slow.

**[project decision]** The bound is on **in-flight proposals at the leader**, not
on total connections. Connections are cheap; what is scarce is the leader's
ability to replicate and commit, which is the actual resource under contention.

**Primary sources:**
- Nygard, *Release It!* (2nd ed., 2018) — the **Circuit Breaker**, **Bulkhead**,
  and **Fail Fast** patterns, and the "blocked threads" and "unbounded result
  set" antipatterns. The clearest statement of why unbounded queues cause
  congestion collapse.
- Google SRE, *Site Reliability Engineering* (2016), **Chapter 21, "Handling
  Overload"** — load shedding, graceful degradation, and why retries make
  overload worse (the retry amplification problem).
- Little, *"A Proof for the Queuing Formula L = λW"* (Operations Research, 1961) —
  the reason bounding queue length is the same act as bounding latency.
- Cormode et al. / standard networking literature on the **token bucket** — the
  rate-limiting mechanism, and why a burst allowance is required for real traffic.
- Ongaro & Ousterhout, *Raft (extended)*, **§8** — the client interaction rules
  this must not break: a rejected request must be distinguishable from one whose
  outcome is unknown.

---

## 19. Hybrid Logical Clocks — cross-shard event ordering (Phase 3)

**What the theory does:** Gives every event a timestamp that is *both* causally
correct and close to real time. Logical clocks (Lamport) order causally related
events perfectly but drift arbitrarily far from wall-clock time; physical clocks
read like wall-clock time but can go backwards and disagree between machines. HLC
gives up almost nothing from either: it is a physical timestamp that is *nudged
forward* whenever causality requires it.

**Why this project needs it, concretely.** Each shard's ledger already assigns
`Transaction.Seq`, a per-shard monotonic counter — deterministic and perfectly
ordered *within* one shard. Across shards it says nothing. Two transactions with
`Seq=7` on shard-0 and `Seq=7` on shard-1 have no relationship at all, so the
system cannot answer questions any bank actually asks:

- "Show this customer's transactions in order" — when their accounts span shards.
- "What did the books look like at 14:32?" — a consistent cut across all shards.
- "Did the debit leg happen before the credit leg?" — the two legs of a
  cross-shard transfer live in *different Raft logs* by construction, which is why
  Phase 2 had to resolve double-entry as a **global** invariant rather than a
  per-shard one.

**Why not just use wall-clock time.** Clocks on different machines disagree, and a
single machine's clock can jump backwards (NTP correction, VM migration). A
transfer stamped at the debit shard could then carry a *later* timestamp than the
credit leg it caused — reversing cause and effect in the audit trail. Worse, this
project's determinism rule forbids reading the clock at apply time at all: two
replicas applying the same log entry would produce different state, and the ledger
would diverge across a shard's own replicas. DESIGN.md states the constraint and
names HLC as the answer: *"if a timestamp is needed it must be in the command,
assigned by the leader before the entry is appended."*

**The mechanism.** An HLC timestamp is a pair `(l, c)`: `l` is a physical time
component in milliseconds, `c` a logical counter breaking ties within the same
millisecond. Three rules:

- **Local event / send:** `l' = max(l, physicalNow)`. If the physical clock
  advanced, take it and reset `c` to 0; otherwise keep `l` and increment `c`.
- **Receive a message with timestamp `(lm, cm)`:**
  `l' = max(l, lm, physicalNow)`, and `c` is chosen so the result strictly exceeds
  both the local and the received timestamp.
- **Compare** lexicographically: `(l, c) < (l', c')` iff `l < l'` or (`l == l'` and
  `c < c'`).

Two properties follow, and both matter here:

1. **Causality is respected.** If event A causally precedes B, then `hlc(A) <
   hlc(B)`. This is Lamport's happened-before, preserved exactly.
2. **`l` stays within clock skew of physical time.** The counter absorbs
   causality *without* letting the timestamp run away from reality — which is what
   makes an HLC timestamp usable as an approximate wall-clock reading for humans
   and for "as of 14:32" queries.

**What HLC does NOT give you, and why that matters for a bank.** HLC provides no
*external consistency* guarantee: a transaction that finished before another
started in real time can still receive a larger timestamp if the two were never
causally connected. Spanner solves that with **TrueTime** — hardware clocks (GPS
and atomic) giving a bounded uncertainty interval, plus a deliberate *commit wait*
that stalls until the uncertainty has passed. That costs specialized hardware and
adds latency to every commit. CockroachDB explicitly chose HLC instead and accepts
the weaker guarantee.

**[project decision]** HLC, not TrueTime. TrueTime's guarantee needs hardware this
project does not have, and its commit-wait would add latency to every transaction
to solve a problem — externally-consistent ordering of *causally unrelated*
transactions — that a single-machine learning system cannot even exhibit. The
weaker guarantee is stated plainly here rather than papered over: this gives
causal ordering across shards, not external consistency.

**[project decision]** The timestamp is assigned by the **leader, before the entry
is appended**, and travels *inside* the command. Every replica then applies the
same timestamp from the log, so determinism is preserved — the same rule that
already forbids reading the clock at apply time.

**Primary sources:**
- Kulkarni, Demirbas, Madappa, Avva & Leone, *"Logical Physical Clocks and
  Consistent Snapshots in Globally Distributed Databases"* (OPODIS 2014) — the
  paper that introduces HLC. §3 has the algorithm; the consistent-snapshot
  application in §5 is exactly the "what did the books look like at 14:32"
  use case.
- Lamport, *"Time, Clocks, and the Ordering of Events in a Distributed System"*
  (CACM 1978) — happened-before, and the logical clock HLC extends.
- Corbett et al., *"Spanner"* (OSDI 2012), **§3** — TrueTime and commit-wait, the
  alternative this project is not taking, and the clearest statement of what
  external consistency costs.
- CockroachDB design documentation on HLC — a production system making the same
  trade-off, and useful for what it says about the limits.
- Kleppmann, *DDIA*, **Chapter 8** ("The Trouble with Distributed Systems"), the
  "Unreliable Clocks" section — why wall-clock timestamps are unsafe for ordering,
  including clocks running backwards.

---

## 20. Observing a live cluster: server push, and the observer effect (Phase 4)

**What this is about:** getting a distributed system's *internal* state onto a
screen, live, and letting an operator perturb it — kill a node, watch the cluster
react — without the act of watching changing what is being watched.

**Why a demo UI is a distributed-systems topic rather than frontend work.** The
interesting claims in this project are all about states that are hard to see:
which node leads, which follower lags, which transaction is in doubt, what
happens to a key's placement when a shard disappears. Every one was previously
verified only by a test assertion. A live view makes them *observable*, which is
the difference between "the tests say Raft elects a new leader in ~200ms" and
watching it happen.

**Push vs. poll, and why polling is the wrong default here.** Polling samples
state at a fixed interval, so anything shorter-lived than the interval is
invisible. That is not an edge case for this system — it is where all the
interesting behaviour lives: a candidate state lasts one election timeout
(150-300ms), a follower's `pending → committed` transition lasts one round trip,
and an in-doubt 2PC transaction may resolve in milliseconds. Poll at 1s and the
UI shows only the *outcomes*, never the mechanism. Push emits an event when the
state actually changes, so transient states are visible because they were sent
when they happened.

**Server-Sent Events (SSE)** is the stdlib-shaped answer for server→client push:
an ordinary HTTP response with `Content-Type: text/event-stream` that is never
closed, carrying `data:` lines. The browser's `EventSource` reconnects
automatically. It is unidirectional by design, which is the right shape here —
the *stream* is one-way, and the control actions (kill a node, move money) are
one-shot commands that fit ordinary HTTP requests. WebSocket's bidirectional
framing solves a problem this does not have.

**[project decision] SSE + HTTP control, not WebSocket.** NOW.md's frontend
decision named `gorilla/websocket` or `nhooyr.io/websocket`. Both are third-party,
and this module has zero third-party dependencies — a property the README
advertises and that has already shaped two earlier decisions (net/rpc over gRPC,
hand-written Prometheus text over the client library). SSE gets the same
user-visible result in ~20 lines of `net/http`. The deviation from NOW.md is
recorded rather than silent.

**The observer effect is real and must be designed around.** A monitoring path
that takes the same locks as the consensus path changes the timing of the thing
it measures — and this project has already measured that happening twice:
`reportRole` polled the server mutex on every tick, contending with the role loop
and every `AppendEntries` handler; and the client API once busy-waited at 2ms,
taking the Raft mutex twice per iteration, so client traffic degraded consensus
itself. For a UI streaming state continuously, the rule follows: **snapshot under
the lock, serialize outside it**, and never let a slow or absent reader block a
producer.

**Slow consumers, and why the queue must be bounded.** A browser tab that stops
reading — backgrounded, throttled, network stalled — will otherwise make the
server buffer without limit. That is the same congestion-collapse argument as
§18, in a different costume, and the same answer applies: bound the per-client
buffer and **drop** for a slow consumer rather than blocking the producer. Dropping
state updates is safe in a way dropping commands is not, because each update is a
complete snapshot rather than a delta — a client that misses three frames and
receives the fourth is fully current.

**Fault injection as a first-class feature.** Letting the operator kill a node
from the UI is Chaos Engineering's principle (§6) applied interactively: the
value is in *observing the steady-state hypothesis hold* under a fault you chose.
It also demands a clear boundary — this is a demo control plane, so it must be
opt-in and clearly separated from the production surface, or "kill this node" is
an unauthenticated endpoint on a bank.

**Primary sources:**
- WHATWG HTML Living Standard, **Server-Sent Events** section, and MDN's
  `EventSource` documentation — the protocol and its automatic reconnection
  semantics.
- Fielding, *Architectural Styles and the Design of Network-based Software
  Architectures* (2000), **Chapter 5** — REST, and why one-shot state-changing
  commands are a natural fit for plain HTTP requests.
- Basiri et al., *"Chaos Engineering"* (IEEE Software, 2016) — already logged at
  §6; the interactive fault-injection here is the same principle with a human in
  the loop rather than a test harness.
- Nygard, *Release It!* — already logged at §18; the slow-consumer/bounded-buffer
  argument is the same one, applied to a streaming endpoint.

---

## Not yet logged (topics to add as they come up)

- Follower reads (LATER.md)

## 21. Re-replication: healing a shard that lost a replica but kept quorum

**What it is.** When a machine holding a replica dies permanently, the shard is
left under-replicated: it still works, but it can now survive fewer further
failures. Re-replication restores the replication factor by adding a replica on
a spare machine and dropping the dead one.

**What problem it solves.** RF=3 tolerates one failure. After one machine dies
you are at 2 replicas — still a quorum, but the *next* failure breaks the shard.
Without healing, a cluster degrades monotonically: every crash permanently lowers
the fault tolerance of whatever shards that machine held. Re-replication returns
the shard to full RF so the tolerance budget is renewed.

**The precondition that makes it safe — and it is absolute.**

> Re-replication requires a **surviving majority** of the shard.

A new replica is populated by the ordinary Raft catch-up path (AppendEntries, or
InstallSnapshot when the leader has compacted past what the follower needs, §7).
Both are driven by a **leader**, and a leader only exists with a majority. So:

- lose 1 of 3 → 2 alive → majority holds → a leader exists → **healing works**
- lose 2 of 3 → 1 alive → no majority → no leader → **nothing to copy from**

There is no mechanism, in Raft or outside it, that reconstructs committed data
from a below-majority set. The data is not gone (it is on disk in the dead
machines' logs) but it is unreadable until enough of them return.

**The tempting wrong answer.** When a shard has lost majority, a system could
"heal" it by creating a fresh empty replica set on healthy machines. The shard
becomes writable again and the dashboard turns green. This **silently resets
every balance in that shard to zero** — it does not recover the data, it discards
it and hides the loss. That is an AP choice (stay available, lose consistency)
in a system that has committed to CP, and for a ledger it is the worst possible
failure mode: money vanishes with no error returned.

This project therefore refuses to heal a below-majority shard, and reports it as
unreachable until its machines come back.

**How it uses §6 rather than working around it.** Adding and removing a replica
are ordinary configuration changes: `AddServer` / `RemoveServer` append a config
entry to the leader's log, and it takes effect on append, not on commit. Healing
is therefore not a new consensus mechanism — it is a policy layer that decides
*when* to call the membership API the paper already specifies.

**Order matters: add first, then remove.** Going the other way (remove the dead
replica, then add the spare) passes through a moment at RF-1 with the
configuration already shrunk. Adding first means the cluster is never less
redundant than it started, and §6's one-change-at-a-time rule is respected by
waiting for each change to commit before making the next.

**Primary sources**
- Ongaro, *Consensus: Bridging Theory and Practice* (PhD dissertation, 2014),
  **§4.1** — single-server membership changes; the catch-up phase for a new
  server before it is added to the configuration.
- Ongaro & Ousterhout, Raft extended paper, **§6** (membership changes) and
  **§7** (snapshotting — how a lagging or brand-new replica is brought up to
  date when the leader has already compacted its log).
- Google SRE Book, ch. 23 *Managing Critical State* — why durable systems
  re-replicate on failure rather than waiting for repair.

**Where it lives in the code:** `demo/heal.go` (policy: when to heal),
`raft/membership.go` (mechanism: §6 config changes). See
[CODE_MAP.md](CODE_MAP.md) §8.

## 22. Does consensus scale? Read/write asymmetry, and why this project serves reads from the leader

**The question this answers.** Raft allows writes only on the leader. So where
does scaling come from — and if reads could be served by every replica, is Raft
"built for huge reads"?

**Raft is a fault-tolerance algorithm, not a scaling algorithm.** It makes three
machines behave like one *reliable* machine. It is strictly SLOWER than a single
machine: every write pays a round trip to a majority. Nothing about Raft makes a
system faster than the one node it replaces.

Distributed systems solve three separable problems, and conflating them is the
usual source of this confusion:

| Problem | Solved by | Not solved by |
|---|---|---|
| Surviving machine failure | replication (Raft) | sharding |
| Getting past one machine's throughput | **sharding** | Raft |
| Putting data near users | placement/geo | either |

**So scaling comes from sharding, and Raft is applied per shard.** One leader per
shard is a bottleneck for THAT SHARD ONLY. Four shards means four leaders, four
logs, four independent commit paths. Measured in this project: **3.3-4.6x
throughput at 4 shards** — not because Raft got faster, but because there were
four Raft groups instead of one.

The limit worth stating honestly: **a single account is still capped at one
leader's throughput.** Sharding scales the aggregate, never a single hot key.

**The wrong answer, and why it is wrong.** A load balancer routing writes to
whichever node is least busy would break consensus outright: two nodes accepting
writes for the same account is precisely the split-brain Election Safety forbids.
This project does have a router (`shard.Coordinator`), but it routes on DATA
OWNERSHIP — `Ring.Lookup(key)` -> shard -> that shard's leader — never on load.

### The read/write asymmetry

Writes must go through one leader. Reads *could* be served by every replica, so
reads scale ~RF× without any sharding at all. That asymmetry is real and is why
read replicas are ubiquitous.

The catch: **a follower can be behind.** Replication is asynchronous — a follower
acknowledges an entry when it APPENDS it and applies it slightly later. So:

```
leader   : Vu = 9500    (committed and applied)
follower : Vu = 10000   (entry replicated, not yet applied)
```

Both are legal Raft states. Reading the follower yields a value that was true a
moment ago — a **stale read**, which breaks linearizability. A client can commit a
withdrawal, immediately read, and not see its own write.

Three read modes, and what each costs:

| Mode | Served from | Guarantee | Scales |
|---|---|---|---|
| Leader read (`ReadIndex`, §8) | leader only | linearizable | no |
| Follower read, stale | any replica | eventual | ~RF× |
| Follower read + `ReadIndex` | any replica | linearizable | partly |

The third is how TiKV and etcd get read scaling without abandoning correctness:
the follower asks the leader only for its current `commitIndex` (a tiny RPC),
waits until its own `lastApplied` reaches it, then serves the read LOCALLY. The
leader stays a coordination point rather than a data path.

### [project decision] Reads are served from the leader, and staleness is refused

**This project serves reads only from a replica known to be current, and never
offers a stale-read mode.** `ReadIndex` (`raft/read.go:52`) and
`LinearizableRead` (`:146`) are the only read paths, and the demo UI additionally
picks `freshestMachine()` and flags `Unreachable` so a balance is never rendered
from a lagging replica.

**Why, given that this costs the ~RF× read scaling:**

- **A stale balance is a wrong balance.** For a feed or a product catalogue,
  200ms of staleness is invisible. For a ledger it is the difference between "you
  have 10000" and "you had 10000 a second ago" — and an overdraft check against a
  stale balance lets money leave an account that no longer holds it.
- **It would contradict the system's stated contract.** This project is CP and
  advertises linearizable operations. Adding a read path that silently returns
  older data would make the guarantee conditional on which endpoint a client
  happened to hit — the guarantee would become a coincidence.
- **The demo's purpose is to show the trade-off, not to hide it.** The UI exists
  to make CAP visible. A read path that stays "available" during a partition by
  serving stale data would demonstrate the opposite of what the system claims.

**What is deliberately left open.** The *mechanism* for follower reads already
exists — every replica runs a full state machine, and `LinearizableRead` reads
under that machine's lock. What is missing is only the routing decision and a
staleness policy. Adding follower reads later would therefore be an ADDITION on
top of the theory (Rules §4), not a contradiction of it, PROVIDED it is opt-in
per request and the weaker guarantee is stated in the response rather than
inferred by the caller. A silent downgrade of every read would be the opposite.

**Primary sources**
- Ongaro & Ousterhout, Raft extended paper, **§8** — client interaction, the
  `ReadIndex` optimisation, and why a leader must confirm leadership before
  serving a read.
- Ongaro, *Consensus: Bridging Theory and Practice*, **§6.4** — lease-based and
  index-based read-only queries, including serving them from followers.
- Herlihy & Wing (1990), *Linearizability: A Correctness Condition for Concurrent
  Objects* — the guarantee a stale read gives up.
- etcd docs, *serializable vs linearizable reads*; TiKV docs, *follower read* —
  two production systems exposing exactly this choice to the caller.

**Where it lives in the code:** `raft/read.go` (`ReadIndex`, `LinearizableRead`),
`demo/demo.go` (`freshestMachine`, `Unreachable`). See [CODE_MAP.md](CODE_MAP.md)
§10.
