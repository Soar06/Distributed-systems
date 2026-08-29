# DESIGN — Current Work Spec

The concrete design for **what is being built right now**. Scope vs spec:
[NOW.md](NOW.md) says what we build and in what order; this file says how the thing
being built is actually specified — states, transitions, message shapes, structures,
invariants. Read NOW.md to decide what to work on; read this while writing the Go.

**Current phase: Phase 2 — sharding + cross-shard transfers.** (Phase 1 complete; its
spec is retained below as the record of what was built.)

Source of truth for everything Raft below: Ongaro & Ousterhout, *"In Search of an
Understandable Consensus Algorithm (Extended Version)"*, tech report published
2014-05-20, <https://raft.github.io/raft.pdf>. Section/figure numbers cite **that**
document (the extended version, not the shorter ATC'14 paper — they differ). Logged
in [learn/READING_LIST.md](../learn/READING_LIST.md) §1 per RULES.md rule 1.

Where this file states a Raft rule, it is a restatement of Figure 2 for
implementation convenience — **the paper wins on any disagreement.** Design decisions
that are *ours* (ledger domain, Go types, package boundaries) are marked
**[project decision]** so they're never confused with what the paper mandates.

---

## 1. What a node is

One node = one Go process = one `raft.Server` + one `ledger.State` + one gRPC server.
Nodes share nothing — no memory, no disk, no globals — even though Phase 1 runs them
all on one machine. Communication is exclusively via the two Raft RPCs plus a client
API. This discipline is what makes the same binary work unchanged on real separate
machines later (LATER.md).

```
┌─────────────── node process ───────────────┐
│  client API (gRPC)                         │
│        ↓ command                           │
│  raft.Server ── AppendEntries/RequestVote ─┼──→ peers
│        ↓ apply committed entries in order  │
│  ledger.State  (derived, never mutated     │
│                 directly by clients)       │
└────────────────────────────────────────────┘
```

The critical arrow is the third one: **clients never touch `ledger.State`.** They
submit commands; the log is the authority; state is what you get by replaying it.

---

## 2. Raft state (Figure 2, "State")

### Persistent on all servers
Updated on stable storage **before** responding to RPCs. Getting this wrong is a
silent correctness bug that only shows up after a crash.

| Field | Meaning |
|---|---|
| `currentTerm` | latest term server has seen (init 0, increases monotonically) |
| `votedFor` | candidateId that received vote in current term, or null |
| `log[]` | log entries; each holds a command for the state machine + the term when the entry was received by the leader. **First index is 1.** |

### Volatile on all servers
| Field | Meaning |
|---|---|
| `commitIndex` | highest log entry known committed (init 0) |
| `lastApplied` | highest log entry applied to the state machine (init 0) |

### Volatile on leaders (reinitialized after election)
| Field | Meaning |
|---|---|
| `nextIndex[]` | per server, index of next log entry to send (init: leader last log index + 1) |
| `matchIndex[]` | per server, highest log entry known replicated on that server (init 0) |

**[project decision]** Go shape — one mutex guarding all Raft state, no lock-free
cleverness in Phase 1. Readability over performance; this is a learning
implementation and races here are the hardest class of bug to find.

```go
type Server struct {
    mu sync.Mutex

    // persistent
    currentTerm uint64
    votedFor    *NodeID
    log         []LogEntry   // log[0] is a sentinel; real entries start at index 1

    // volatile, all servers
    commitIndex uint64
    lastApplied uint64

    // volatile, leaders only
    nextIndex  map[NodeID]uint64
    matchIndex map[NodeID]uint64

    role  Role   // Follower | Candidate | Leader
    peers []NodeID
}

type LogEntry struct {
    Term    uint64
    Index   uint64
    Command ledger.Command   // the actual bank operation
}
```

**[project decision]** `log[0]` is a zero-value sentinel so Go's 0-based slice
indexing lines up with the paper's 1-based log. Avoids off-by-one bugs in every
`prevLogIndex` comparison.

---

## 3. Server states and transitions (Figure 4)

Three roles. Followers only respond to requests. A follower that hears nothing
becomes a candidate and starts an election. A candidate winning a majority of the
**full cluster** becomes leader. Leaders typically run until they fail.

```
        times out,            receives votes from
        starts election       majority of cluster
Follower ──────────→ Candidate ──────────────→ Leader
   ↑                     │                        │
   │  discovers current  │                        │ discovers server
   │  leader or new term │                        │ with higher term
   └─────────────────────┴────────────────────────┘
```

### Rules for servers (Figure 2)

**All servers**
- If `commitIndex > lastApplied`: increment `lastApplied`, apply `log[lastApplied]`
  to the state machine.
- If any RPC request or response contains term `T > currentTerm`: set
  `currentTerm = T`, convert to follower. *(This rule is unconditional and applies
  everywhere — it's the single most commonly missed line in Figure 2.)*

**Followers**
- Respond to RPCs from candidates and leaders.
- If election timeout elapses with no `AppendEntries` from the current leader and no
  vote granted to a candidate: convert to candidate.

**Candidates**
- On conversion, start election: increment `currentTerm`, vote for self, reset
  election timer, send `RequestVote` to all other servers.
- Majority of votes → become leader.
- `AppendEntries` from a new leader → convert to follower.
- Election timeout elapses → start a new election.

**Leaders**
- On election: send empty `AppendEntries` (heartbeat) to each server; repeat during
  idle periods to prevent election timeouts.
- Command from client: append to local log, respond after the entry is **applied to
  the state machine**.
- If last log index ≥ `nextIndex[i]`: send `AppendEntries` starting at `nextIndex[i]`.
  On success update `nextIndex`/`matchIndex`; on failure from log inconsistency,
  decrement `nextIndex` and retry.
- If ∃ N such that `N > commitIndex`, a majority of `matchIndex[i] ≥ N`, and
  **`log[N].term == currentTerm`**: set `commitIndex = N`. *(The term check is a
  safety requirement, not an optimization — §5.4.2.)*

---

## 4. RPCs (Figure 2)

Two node-to-node RPCs. Nothing else crosses the node boundary.

### AppendEntries — leader → followers (§5.3), also heartbeat (§5.2)

Args: `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]` (empty for
heartbeat), `leaderCommit`.
Results: `term`, `success` (true if follower contained an entry matching
`prevLogIndex`/`prevLogTerm`).

Receiver:
1. Reply false if `term < currentTerm`.
2. Reply false if log has no entry at `prevLogIndex` whose term matches `prevLogTerm`.
3. If an existing entry conflicts with a new one (same index, different terms),
   delete that entry **and all that follow it**.
4. Append any new entries not already in the log.
5. If `leaderCommit > commitIndex`, set
   `commitIndex = min(leaderCommit, index of last new entry)`.

### RequestVote — candidate → all (§5.2)

Args: `term`, `candidateId`, `lastLogIndex`, `lastLogTerm`.
Results: `term`, `voteGranted`.

Receiver:
1. Reply false if `term < currentTerm`.
2. If `votedFor` is null or `candidateId`, **and** the candidate's log is at least as
   up-to-date as the receiver's, grant vote (§5.4 defines "up-to-date").

**[project decision]** protobuf lives in `rpc/`; field names mirror the paper's
exactly (`prev_log_index`, not `previousIdx`) so the code reads against Figure 2
line by line.

---

## 5. Safety properties to test against (Figure 3)

These are the assertions chaos tests must never violate:

- **Election Safety** — at most one leader per term. (§5.2)
- **Leader Append-Only** — a leader never overwrites or deletes entries in its own
  log; it only appends. (§5.3)
- **Log Matching** — if two logs contain an entry with the same index and term, the
  logs are identical in all entries up through that index. (§5.3)
- **Leader Completeness** — if an entry is committed in a term, it is present in the
  logs of leaders for all higher terms. (§5.4)
- **State Machine Safety** — if a server has applied an entry at a given index, no
  other server will ever apply a *different* entry for that index. (§5.4.3)

**[project decision]** `sim/` asserts these directly rather than only checking
balances. A balance check tells you *something* broke; a Log Matching assertion tells
you *what*.

**Binding:** [RULES.md](../Agents/RULES.md) rule 3 makes this mandatory per feature,
not just as a final phase-level pass. Every new function/feature ships with multiple
flows — normal, failure (leader killed mid-operation, partition, lost/duplicated/
reordered RPCs), concurrent (same account, two clients), and retry (same request
twice) — asserted at the API level against both these properties and real-world
ledger sanity (no lost or duplicated money). Happy-path-only ⇒ not done.

---

## 6. Ledger domain model **[project decision — entirely ours, not the paper]**

The state machine Raft replicates. Must be **deterministic**: same log ⇒ same state
on every node, always. This constrains the domain more than it first appears.

### Commands (what goes in a log entry)

```go
type Command struct {
    Op             Op        // Deposit | Withdraw | Transfer
    IdempotencyKey string    // client-supplied, dedupes retries (Phase 3 hardens)
    From, To       AccountID // From empty for Deposit; To empty for Withdraw
    Amount         Money
}
```

### Determinism rules (non-negotiable)
- **No wall-clock time, no RNG, no map-iteration order** in command application. If a
  timestamp is needed it must be *in the command*, assigned by the leader before the
  entry is appended — never read from the local clock at apply time. (This is
  precisely the problem HLC solves in Phase 3.)
- Validation (e.g. sufficient funds) happens **at apply time against replicated
  state**, not at request time against a possibly-stale read. A withdrawal that
  passes a pre-check on the leader can still legitimately fail on apply — and it must
  fail *identically on every node*.

### Money
`Money` is an integer count of minor units (cents). Never `float64`. Non-negotiable.

### Double-entry (Phase 3 enforces; model it correctly now)
Every state change is a matched debit + credit; balances are never bare mutations.
Modeling this from the start avoids a rewrite, even though enforcement is Phase 3.

### Derived state
```go
type State struct {
    balances map[AccountID]Money  // derived by applying the log in order
    applied  map[string]Result    // idempotency key → result, for retry dedup
}
```

`balances` is a **cache of a fold over the log**, not the source of truth. Any node
must be able to rebuild it from the log alone. If that ever stops being true, the
design has been violated.

---

## 7. Package boundaries **[project decision]**

Per NOW.md's layout. Phase 1 touches only these:

| Package | Owns | Must not know about |
|---|---|---|
| `raft/` | Figure 2 in full: state, roles, timers, RPC handlers | banking — commands are opaque bytes/interface |
| `ledger/` | accounts, commands, double-entry, determinism | Raft, terms, indices |
| `storage/` | persistent state + log durability before RPC reply | domain meaning of a command |
| `rpc/` | protobuf defs, gRPC wiring, client API | consensus rules |
| `node/` | wires the above into one process; config, peer list | — |
| `sim/` | deterministic chaos: kill leader, partition, assert Fig. 3 | — |

The `raft/` ↔ `ledger/` separation is the important one: Raft must be testable with a
trivial state machine (e.g. a counter), and the ledger must be testable with no
consensus at all.

---

## 8. Open questions — deliberately not decided

Carried from NOW.md; do **not** resolve these by guesswork:

1. **Concurrent same-account behavior** (two bank-app windows, same account,
   simultaneous withdrawals). NOW.md leaves this open until the consensus core is
   understood well enough to decide it intentionally. Whatever is chosen must be
   justified against CAP/linearizability, not chosen for UI convenience.
   *Partly settled by [RULES.md](../Agents/RULES.md) rule 3:* the decision is made
   on the backend and proven by API-level concurrent tests; the UI is then corrected
   to display whatever the backend actually does. What remains open is the *backend
   choice itself*, not who decides it.
2. **Linearizable read strategy** — leader-only reads vs. ReadIndex vs. lease reads
   (§8). Not yet chosen. **Per RULES.md rule 1, this gets a `learn/` entry before
   it's designed**, since it's a distinct mechanism.
3. **Client routing/retry** — how a client finds the leader and retries on redirect.
   Interacts with idempotency (Phase 3).

---

## Status

**Phase 1 is complete.** 79 tests pass under `-race` across six packages.

- [x] `raft/` — Figure 2 in full: state, both RPC receivers, the Rules for Servers
      role loop, randomized election timeouts, `nextIndex` decrement-and-retry
      replication, and the commit rule with the `log[N].term == currentTerm` check.
- [x] **Persistence** — `storage/` WAL (length-prefixed, CRC32-checksummed,
      fsync-before-return) with torn-write detection and truncation on recovery.
      Every Figure 2 persistent-state mutation is durable *before* the RPC reply.
      Proven by a test where a node votes, restarts, and refuses to vote again in
      the same term.
- [x] **No-op entry on election** (§8) — required, not optional: without it a new
      leader can never commit entries carried over from a previous term, so a
      restarted cluster stalls at commit 0. Found by a failing test, fixed against
      the paper.
- [x] **Linearizable reads** (§8 ReadIndex) — leader confirms with a majority
      before answering. Lease-based reads deliberately NOT implemented: they
      "rely on timing for safety."
- [x] `ledger/` — accounts, double-entry with a sum-to-zero invariant, integer
      cents, idempotency keys, and a `VerifyDoubleEntry` audit that re-derives
      balances from history.
- [x] `rpc/` + `node/` — nodes are real OS processes over TCP with per-node WALs.
      Verified by hand: 3 processes elected a leader, moved money, a follower
      redirected to the leader's real address, the leader process was killed and
      n2 took over in term 2 with balances intact, then the dead node restarted
      from its WAL and caught up.
- [x] **The throughput demo** — measured: 3 nodes = 119.9 tx/s, 5 nodes = 105.9
      tx/s (88%). Adding replicas made writes *slower*, never faster.
- [x] `sim/` — chaos harness with all five Figure 3 properties asserted
      executably, plus crash-and-restart tests that persistence made possible.

**Deliberately not done** (Phase 2+ / LATER.md): sharding and 2PC, HLC,
snapshotting and log compaction, cluster membership changes, gRPC/protobuf (see
the `rpc/` note below), TLS, service discovery, follower reads, and the Phase 4
UIs — the `fe/` mockups are still unwired.

**[project decision] `rpc/` uses net/rpc + gob, not gRPC/protobuf as NOW.md
specifies.** protoc is an external build dependency and the wire format is not
what Phase 1 teaches. `raft.Transport` is unchanged, so swapping in gRPC touches
no consensus code.

**Known limitation:** `storage.RaftState.Save` rewrites the entire state on every
persist. Correct but O(log size) per append — fine at Phase 1 scale, and the
reason log compaction is a real LATER.md item rather than a nicety.

---

# Phase 2 — Sharding + cross-shard transfers

Phase 1 measured the constraint it exists to fix: **3 nodes = 119.9 tx/s, 5 nodes =
105.9 tx/s.** Every write funnels through one leader, so replicas buy fault tolerance,
never write throughput. Sharding is the only thing that adds write capacity, because
independent shards have independent leaders committing in parallel.

Theory logged in [learn/READING_LIST.md](../learn/READING_LIST.md) §11 (sharding /
consistent hashing) and §12 (2PC), both before this design, per RULES.md rule 1.

## 9. Shard topology

A **shard** is one independent Raft group with its own leader, its own log, its own
WAL files, and its own slice of the accounts. Nothing is shared between shards but the
network.

```
   shard-0 (Raft group)      shard-1 (Raft group)
   n0a  n0b  n0c             n1a  n1b  n1c
    ^ own leader, own log     ^ own leader, own log
    └── accounts hashing here └── accounts hashing here
```

**[project decision] Fixed shard count for Phase 2.** This is not a shortcut — it is
what real systems do at this layer: Spanner and CockroachDB separate *placement* from
the *transaction protocol*, and rebalancing is its own subsystem. Live resharding is a
LATER.md item, recorded rather than faked. Consistent hashing is still implemented,
because it is the correct structure and it is what the dashboard's ring view renders.

## 10. Placement: consistent hashing

Keys and shards are mapped onto a 2^32 ring. An account belongs to the first shard
clockwise from `hash(accountID)`.

Naive `hash % N` is rejected for the reason §11 gives: changing N remaps nearly every
key, which for a bank means relocating nearly every account at once.

**Virtual nodes are required, not optional.** A handful of shards placed once on the
ring divides it very unevenly; each shard therefore gets many virtual points
(`vnodes`), which is what makes the distribution roughly even. Dynamo's design.

Determinism requirement: placement must be a pure function of `(accountID, ring
config)`. Every node must independently agree on who owns what, with no coordination —
otherwise two nodes disagree about which shard is authoritative for an account, which
is a correctness bug, not a performance one.

## 11. Cross-shard transfers: 2PC over Raft

A transfer within one shard stays a single Raft commit — unchanged from Phase 1. A
transfer *across* shards touches two independent logs, and each could commit its half
while the other fails. That is the problem 2PC solves.

**We implement Spanner's shape exactly** (§12, quoted from the OSDI 2012 paper),
substituting Raft for Paxos:

- **No external coordinator process.** *"One of the participant groups is chosen as
  the coordinator."* The debit shard's leader takes the role.
- **Participants log a prepare record through Raft** — *"logs a prepare record through
  Paxos"*. The vote is a replicated log entry, durable before it is sent. A `yes` vote
  is an unretractable promise, so it must survive the voter crashing.
- **The coordinator logs the commit/abort decision through Raft** — *"The coordinator
  leader then logs a commit record through Paxos (or an abort if it timed out)"*.
- **Participants log the outcome through Raft** — *"Each participant leader logs the
  transaction's outcome through Paxos."*

Nothing about the 2PC state lives in memory. That is the whole point: an in-memory
coordinator cannot survive the failure the protocol exists to handle.

### The blocking problem is demonstrated, not hidden

If the coordinator crashes after participants voted `yes` but before the decision is
delivered, those participants are **in doubt**: holding funds reserved, unable to
commit or abort unilaterally. This is inherent to 2PC, not a bug (§12, Bernstein ch.7).

Two things follow, and both are Phase 2 deliverables:
1. **Recovery works.** The coordinator's replacement leader reads the decision from
   its own Raft log and re-delivers it. An in-doubt participant can also query the
   coordinator group for the outcome.
2. **A chaos test proves the window exists** — kill the coordinator mid-protocol,
   observe participants correctly refusing to guess, then observe recovery resolving
   them. Hiding this would misrepresent 2PC.

### Reserved funds, not locks

The debit is **reserved** at prepare time (deducted from available balance, not yet
committed), so a concurrent transfer cannot spend the same money. On commit the
reservation becomes a real debit; on abort it is released. This is the double-entry
model extended over two shards — money is never in two places, and never in neither.

## 12. Money conservation across shards

The Phase 1 invariant `TotalMoney()` must now hold **across all shards combined**,
including while transfers are mid-flight. A transfer in the prepare state has money
reserved on the debit side and not yet credited — the sum must still be correct, which
means reserved funds count toward the total until the transfer resolves.

This is the single most important assertion in Phase 2: no chaos scenario, at any
point, may create or destroy a cent.

## 13. Phase 2 status

**Core protocol complete.** 101 tests pass under `-race` across seven packages.

- [x] `shard/` — consistent hash ring with virtual nodes. Measured against theory:
      adding a 5th shard moved **21.9%** of keys (theory ~1/5); consistent hashing
      moved **1096** keys where modulo would move **4011**; virtual nodes cut
      max/min shard skew from **579.88x to 1.17x**.
- [x] Multi-group cluster — independent Raft groups, each with its own network, its
      own leader, and **one state machine per node**.
- [x] Intra-shard transfers as a single Raft commit, no 2PC.
- [x] 2PC over Raft in Spanner's shape: prepare, decision, and outcome are each
      replicated log entries; the debit shard's leader is the coordinator.
- [x] Coordinator crash recovery and in-doubt resolution — honours a durable COMMIT
      decision when one was logged, aborts safely when none was.
- [x] Chaos: coordinator killed mid-protocol, participants correctly blocked, then
      resolved by recovery.
- [x] Cross-shard money conservation asserted after **every** transfer.

### Bugs this phase surfaced

1. **Cross-shard double-entry cannot balance per-shard** — the two legs live in
   different Raft logs by construction. Resolved as correspondent banking does: each
   side books its leg against an implicit settlement account, and conservation is a
   **global** invariant. The `VerifyDoubleEntry` panic caught this.
2. **Every replica shared one state machine** in the harness, so each committed entry
   applied once per node — a 3x debit on a 3-node shard. Now one ledger per node,
   which the project's own "nodes share nothing" rule requires anyway.
3. **`Propose` returned a canned success**, conflating "the entry replicated" with
   "the operation succeeded" — so every 2PC NO vote read as YES. Fixed by keying
   results to the applied log index (`raft.IndexedStateMachine`).
4. **`InDoubt()` excluded decided-but-unapplied transactions**, so recovery skipped
   exactly those holding a durable COMMIT decision.

### Deliberately NOT claimed: sharded write-throughput numbers

NOW.md predicts sharding adds write capacity where replicas do not. Phase 1 measured
the second half over real TCP (3 nodes 119.9 tx/s vs 5 nodes 105.9 tx/s).

The first half is **not measurable in this in-process simulator**. Evidence: one shard
running a fixed workload does ~169k tx/s in a 1-shard cluster and ~55k tx/s in a
4-shard cluster while the other three shards are **idle**. Idle Raft groups cannot
slow another group — no shared lock, network, or state machine between them. The
slowdown is harness overhead: every node runs a 5ms ticker and spawns a goroutine per
peer per replication round, so scheduling cost grows with total node count.

`TestShardsCommitIndependently` verifies the structural property instead, and it holds
exactly: 20 writes to shard-0 grew shard-0's log by 20 entries and shard-2's by **0**.
The throughput claim needs a real multi-process benchmark over `rpc/` — **outstanding**.

### Still outstanding for Phase 2

- [ ] Multi-process sharded deployment over TCP (`node/` currently runs one group)
- [ ] The real sharded throughput benchmark, per the note above
- [ ] Persistence for 2PC state — `shard.Machine.txs` is in-memory, so a prepared
      participant forgets its promise on restart. `storage/` exists and is wired for
      Raft, so this is plumbing rather than new design.
- [ ] Live resharding / rebalancing — LATER.md, deliberately out of scope
