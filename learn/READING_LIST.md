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

**Primary sources:**
- Gray, *"Notes on Data Base Operating Systems"* (1978) — where 2PC is first laid out.
- Bernstein, Hadzilacos & Goodman, *Concurrency Control and Recovery in Database
  Systems* (1987), **Chapter 7** — the rigorous treatment of atomic commitment,
  including why the blocking problem is unavoidable.
- Kleppmann, *DDIA*, **Chapter 9** ("Consistency and Consensus"), the "Atomic Commit
  and Two-Phase Commit" section — clearest modern explanation, and explicit about
  coordinator failure and in-doubt transactions. Same chapter as [[#2]].
- Corbett et al., *"Spanner: Google's Globally-Distributed Database"* (OSDI 2012) —
  2PC layered over Paxos groups; the production shape of what Phase 2 builds.
- Garcia-Molina & Salem, *"Sagas"* (SIGMOD 1987) — the alternative we are not taking.

---

## Not yet logged (topics to add as they come up)

- Hybrid Logical Clocks (Phase 3)
- Snapshotting / log compaction (LATER.md; Ongaro's dissertation covers it)
- Cluster membership changes (LATER.md; Raft §6, joint consensus)
- Follower reads (LATER.md)
- Backpressure / load shedding / rate limiting (LATER.md)
