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

## Not yet logged (topics to add as they come up)

- Sharding / consistent hashing (Phase 2)
- Two-Phase Commit / cross-shard transactions (Phase 2)
- Hybrid Logical Clocks (Phase 3)
- Event sourcing (Phase 3)
