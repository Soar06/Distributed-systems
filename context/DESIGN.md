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

**Core protocol complete.** 121 tests pass under `-race` across seven packages.

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
- [x] **Persistence for 2PC state — done (2026-08-30).** The diagnosis in this
      line was wrong in an instructive way, and the corrected version is worth
      keeping. `shard.Machine.txs` is not in-memory-only state: every field of a
      `TxRecord` is derived by applying `OpPrepare`/`OpDecision`/`OpOutcome` log
      entries, exactly as balances are derived from ledger entries, and
      `raft.Restore` already replays the log into the state machine. The promise
      was durable *by construction* wherever the group had storage attached.

      The real defect was in the **harness**: `sim.NewShardCluster` built every
      shard group with no storage at all and never called `Restore`, so no test
      could observe 2PC state crossing a restart — the gap was in the coverage,
      not the protocol. Fixed by `sim/shardcluster_storage.go`
      (`NewShardClusterWithStorage` + `ShardCluster.RestoreAll`), which gives each
      node its own `<node>.wal` and `<node>.applied`, mirroring `node/main.go`.

      Proven by four flows in `sim/twopc_persist_test.go`: a prepared participant
      keeps its `PhasePrepared` promise and its reservation across a full-cluster
      restart; a durable COMMIT is still honoured after the restart; a transaction
      with no logged decision still aborts safely; and an outcome redelivered after
      the restart applies once. Each asserts cross-shard money conservation.

      The tests were verified to fail against the non-persistent constructor before
      being accepted — the restarted shard came back with **0 accounts and 0
      in-doubt transactions**, i.e. the vote and the held funds both vanished.
- [ ] Live resharding / rebalancing — LATER.md, deliberately out of scope


---

# Production hardening pass (2026-08-29)

Three parallel audits - raft/storage, ledger/2PC, and rpc/node/sim - against the
"production ready, no compromise" standard. **Every bug below passed the previous
test suite**, which is the point: the suite proved the happy path and ordinary
recovery, while the adversarial half was missing.

The unifying defect across all three areas: **"this shouldn't happen" branches
returned success or guessed**, instead of failing loudly. For a system whose
governing invariant is that no scenario may create or destroy a cent, that default
is inverted.

## Money bugs (each reproduced, fixed, and covered by a regression test)

| Bug | Consequence |
|---|---|
| Recovery guessed ABORT when it could not *reach* the coordinator | Unilaterally reversed committed transactions - debit stood, credit never landed |
| Outcome applied for a transaction a shard never prepared | Credit applied unconditionally while debit no-opped: money created |
| CommitDebit returned OK holding no reservation | A missing debit leg reported success |
| A later prepare erased a durable COMMIT decision | An aborted transaction id became reusable |
| PrepareDebit ignored account/amount on a duplicate txID | A second prepare claimed a larger sum against a smaller hold |
| Internal 2PC keys shared the client idempotency namespace | A client key of tx9:credit swallowed a real credit leg |
| No overflow checks | Depositing onto MaxInt64 wrapped a balance negative |
| **Idempotency key not bound to its request** | A withdrawal from bob returned ok=true carrying **alice's** balance; the client records a debit that never happened |

## Safety and availability bugs

- **RaftState.Save could destroy all persistent state.** Truncate-then-append left
  a window where the file was durably EMPTY. The comment above it claimed this was
  safe because "a node that loses its state is indistinguishable from a brand-new
  node" - wrong, and the most dangerous line in the codebase. A brand-new node has
  a new identity; a node that keeps its id but forgets votedFor grants a second
  vote in a term it already voted in. Now an atomic write-temp / fsync / rename /
  fsync-dir.
- **PHANTOM QUORUM.** A shut-down node kept voting and acking over the leader's
  cached connection, so a lone leader committed and told the customer their money
  moved against a quorum that no longer existed. raft.Server now latches stopped;
  rpc.Server.Close closes established connections and waits.
- **Single-node clusters could never elect a leader** - the vote tally was only
  checked inside a per-peer goroutine.
- **-peers accepted duplicate ids, missing ports, empty ids.** A one-character typo
  produced a cluster that reported healthy, tolerated zero failures, and could
  later have committed entries overwritten.
- **RPC timeout (500ms) exceeded the election timeout (150-300ms)** - the section
  5.2 inequality inverted, guaranteeing spurious elections. Timings are now flags,
  validated at startup.
- **Transport dialed while holding the shared mutex**, so one dead peer stalled
  every other peer's RPCs (measured: 2.95s to a healthy peer). And **dropping on
  timeout killed concurrent healthy calls** on the shared connection (91/200).

## Simulator honesty

- **Fault selection is now reproducible** (per-link PRNGs). It previously used one
  shared PRNG reached in scheduler-dependent order, so even the fault pattern
  differed between runs of the same seed.
- **Full run reproducibility is NOT achieved and is no longer claimed.** Election
  timers run on wall-clock time, so RPC counts still vary ~2% between runs of one
  seed. Closing that needs a logical-time discrete-event scheduler - a substantial
  redesign, recorded as outstanding rather than papered over.
- **Latency is now modelled** (SetLatency). Zero-latency delivery is why the
  transport bugs above were invisible to the suite. New test: with the timing
  inequality deliberately violated, terms churned to 25 while Election Safety and
  Log Matching held - timing costs liveness, never safety, exactly as the paper says.

## Still outstanding

**Each item below is designed in [Gap-closure design](#gap-closure-design-2026-08-30)
at the end of this file, including the order to do them in and why.**

- [ ] Multi-process **sharded** deployment. node/ runs one Raft group; a sharded
      cluster needs shard-multiplexed RPC (a wire-format change), a real
      shard.Group over the network, and a durably-placed coordinator. A substantial
      piece of work, not wiring.
- [ ] The sharded write-throughput benchmark (needs the above).
- [ ] **Auth and TLS.** Both ports are unauthenticated plaintext gob: anyone who
      can reach them can read every balance and inject AppendEntries. The single
      highest-risk gap for a banking system.
- [ ] Metrics, health/readiness endpoints, structured logging.
- [ ] Snapshotting and log compaction (section 7). RaftState.Save rewrites the
      whole log per persist - measured O(n^2): 481x write amplification at 800
      entries. The hard scalability wall.
- [ ] Membership changes (section 6) - no node replacement without full downtime.
- [ ] Rate limiting / backpressure; graceful shutdown draining.

---

# Gap-closure design (2026-08-30)

Every outstanding item above, designed to the point where implementation is
mechanical. Written after 2PC durability was closed, because that work changed
the picture: the defect there was in the *test harness*, not the protocol, and it
is worth assuming the same is true elsewhere until checked. Sequencing matters
more than any individual design here, so it comes first.

## G0. Sequencing, and why

The items are not independent. Three facts drive the order:

- **G1 (shard-multiplexed RPC) unblocks G2 (the throughput benchmark)** and is a
  prerequisite for honestly claiming Phase 2 complete. It is also the largest
  single piece.
- **G3 (snapshotting) gets harder the later it is done.** It changes
  `raft.Storage`, `Restore`, and `AppendEntries` — all of which G1 also touches.
  Doing G3 *after* G1 means editing the new multiplexed paths twice.
- **G4 (auth/TLS) is a wire-format change**, and G1 is already changing the wire
  format. Doing them together costs one migration instead of two.

The resulting order — and the reason each slot is where it is:

| # | Item | Why here |
|---|---|---|
| 1 | **G4 auth + TLS** | Highest risk, and cheapest while the wire format is already being opened by G1. Do it *with* G1, not after. |
| 2 | **G1 multi-process sharded deployment** | Unblocks G2; the structural centre of the remaining work. |
| 3 | **G2 sharded throughput benchmark** | Pure measurement once G1 lands. Closes the one claim NOW.md makes that is not yet evidenced. |
| 4 | **G3 snapshotting / log compaction** | The hard scalability wall (481x write amplification). Deferred behind G1 only because doing it first means doing it twice. |
| 5 | **G5 observability** | Needed to *interpret* G2's numbers and to run G6 safely. |
| 6 | **G6 membership changes** | Depends on G3: a node added to a large cluster needs a snapshot to catch up in bounded time. |
| 7 | **G7 backpressure / graceful shutdown** | Refines behaviour the earlier items establish. |

Phase 3 (HLC) and Phase 4 (UIs) sit after this list. G5 is what makes Phase 4's
dashboard possible, so it is the natural hinge between the two.

## G1. Multi-process sharded deployment

**The gap.** `node/main.go` builds one `raft.Server` over one `ledger.Machine`.
Sharding and 2PC exist only inside `sim/`, driven in-process. The 2PC durability
work made this precise: `shard.Machine` is correct and durable, but no production
binary ever instantiates one.

**Design.** One process hosts N shard *replicas*, not one node:

```
node process
├── shardRegistry: map[shard.ID]*replica
│     replica = { raft.Server, shard.Machine, own .wal/.applied }
├── one TCP listener, one transport
└── every RPC frame carries a shard ID
```

Three concrete changes:

1. **The wire format gains a shard header.** Every Raft RPC becomes
   `{ShardID, payload}`; the transport demultiplexes to the right `raft.Server`.
   This is the wire-format change the outstanding list refers to. Do it in the
   same pass as G4 so the format is opened once.
2. **`node/` gains a real `shard.Group` over the network.** Today `sim.ShardGroup`
   is the only implementation. The production one has the same three methods
   (`Propose`, `Machine`, `IsLeader`) but resolves the leader over RPC and
   forwards there, reusing the `NotLeader`/`LeaderAddr` redirect the client API
   already speaks.
3. **The coordinator becomes durably placed.** `shard.Command.Coordinator` already
   names the coordinating shard explicitly — it was made explicit rather than
   inferred from `Participants[0]` precisely for this. Recovery must then run
   automatically **on leadership acquisition**, not only when a test calls
   `RecoverInDoubt()` by hand: a new leader of any group scans its machine for
   in-doubt transactions and resolves them. Without this, real in-doubt
   transactions block until a human intervenes.

**Config.** `-shards shard-0=host:port,...` alongside the existing `-peers`, plus
`-replica-of shard-0,shard-1` naming which shards this process hosts. Validated at
startup with the same severity as `-peers`: a process that believes it hosts a
shard it does not is a phantom-quorum-class bug, and the existing validation
precedent says these are rejected outright rather than warned about.

**Testing (rule 3).** Normal: money moves cross-shard between real processes.
Failure: kill a participant's leader mid-prepare; kill the coordinator's leader
after its decision is logged. Concurrent: two clients transferring between the
same shard pair. Retry: the same idempotency key replayed after a redirect. Every
one asserts cross-shard conservation. The four restart flows added for 2PC
durability port directly to this layer and should be re-run there, since a
harness-only proof is exactly the weakness that work uncovered.

## G2. The sharded write-throughput benchmark

**The gap.** NOW.md predicts sharding adds write capacity where replicas do not.
Phase 1 measured the second half over real TCP (3 nodes 119.9 tx/s, 5 nodes 105.9
tx/s). The first half is **unmeasured**, and DESIGN.md already explains why the
in-process number is not evidence: one shard does ~169k tx/s alone and ~55k tx/s
in a 4-shard cluster *while the other three shards are idle* — harness scheduling
cost, not contention.

**Design.** After G1, run 1/2/4-shard clusters as real processes, 3 replicas each,
with a workload of **single-shard transfers only** — cross-shard 2PC measures a
different thing and would confound the result. Report tx/s against shard count,
then the cross-shard fraction as a separate curve, because the honest expected
finding is "sharding scales writes *until* transfers cross shards". That second
curve is the more valuable result: it is why real banks shard by customer, so that
most activity stays within one shard.

**Honesty rule, carried forward.** If the numbers do not show scaling they get
published as-is with the reason, exactly as the in-process result was.
`TestShardsCommitIndependently` stays as the structural claim that holds
regardless.

## G3. Snapshotting and log compaction (§7)

**The gap.** `storage.RaftState.Save` rewrites the entire state on every persist —
measured O(n²), **481x write amplification at 800 entries**. It is a wall, not a
slope: nothing degrades gracefully, the node simply stops keeping up. It also
makes restart time unbounded, which G6 depends on.

**Design**, following §7:

- `raft.StateMachine` gains `Snapshot() ([]byte, error)` and `Restore([]byte) error`.
  Both `ledger.State` and `shard.Machine` implement it. For `shard.Machine` the
  snapshot **must include `txs` and the ledger's `reserves`** — otherwise a
  compacted node loses exactly the 2PC promise the durability work just secured.
  That is the one place this item can create a money bug, so it is the first thing
  to test, not the last.
- A snapshot records `lastIncludedIndex` and `lastIncludedTerm`, and the log is
  truncated through it. Those two fields are what let `AppendEntries` still answer
  `prevLogIndex`/`prevLogTerm` for the entry immediately before the snapshot.
- `InstallSnapshot` RPC for a follower that has fallen behind the leader's
  compacted prefix.
- Trigger on log size crossing a threshold, checked after apply — not time-based,
  since the cost is a function of size.

**Testing (rule 3).** Snapshot, restart, and get byte-identical state including
reservations and in-doubt transactions; a follower behind the compacted prefix
catches up via `InstallSnapshot`; Log Matching and State Machine Safety asserted
across a compaction boundary; and the write-amplification measurement repeated to
show the wall is actually gone rather than moved.

## G4. Authentication and TLS

**The gap, stated plainly.** Both ports are unauthenticated plaintext gob. Anyone
who can reach them can read every balance and inject `AppendEntries` — and a
forged `AppendEntries` at a high term makes the attacker the leader, which lets
them rewrite the ledger. Correctly listed as the highest-risk gap.

**Design.**

- **Mutual TLS between nodes.** A peer's identity is its certificate, and the node
  id in the peer list must match the certificate subject. This closes forged
  `AppendEntries` structurally rather than by validation.
- **A bearer token per client**, checked before any command is proposed.
  Deliberately *not* per-account authorization — that is an application concern
  and scope creep here. The goal is that only authorized callers reach the cluster
  at all.
- **`gob` is a liability independent of TLS.** It decodes into Go types and its
  decoder is not written to be adversary-facing. Since the wire format is already
  being opened for G1, replace the framing with explicit length-delimited encoding
  whose decoder validates lengths *before* allocating. `shard.Command.Encode`
  already works this way and is the model to copy.

**Testing (rule 3).** An unauthenticated peer is rejected; a peer with a valid
certificate but the wrong node id is rejected; a client with no token cannot read
a balance; a truncated or oversized frame is refused rather than allocating. Plus
a regression that the cluster still elects and commits normally with TLS on — a
security change that breaks liveness has only traded one outage for another.

## G5. Metrics, health endpoints, structured logging

**The gap.** A cluster committing against a degraded quorum looks identical from
outside to a healthy one. This is what makes G2's numbers uninterpretable and G6
unsafe to attempt.

**Design.** An HTTP port per process, separate from the RPC port:

- `/metrics` — Prometheus text format. No client library needed for a handful of
  values: role, current term, commit index, applied index, log size, in-doubt
  count, per-op latency histogram, apply-error count.
- `/healthz` — the process is alive. `/readyz` — this node is in a quorum that can
  commit. **The distinction is the whole point**: a node can be perfectly alive and
  unable to make progress, and conflating the two is precisely the degraded-quorum
  blindness above.
- **Structured logging** via `log/slog` (stdlib, no dependency) carrying node id,
  shard id, term, and index on every line. Today's logs cannot be correlated across
  processes, which is exactly what debugging a multi-process sharded cluster needs.

`Bank.Status` already exposes much of this per node; this is the machine-readable
form of the same data, and it is what Phase 4's dashboard consumes.

## G6. Cluster membership changes (§6)

**The gap.** A failed node cannot be replaced without full-cluster downtime.

**Design.** **Single-server changes** — add or remove one server at a time — from
Ongaro's dissertation §4.1, *not* the joint-consensus form in the extended paper's
§6. Single-server changes are simpler, sufficient, and what production Raft
implementations actually use. The safety argument: adding or removing one server
cannot produce two disjoint majorities, which is the thing joint consensus exists
to prevent.

The configuration is stored **as a log entry and takes effect on append, not on
commit.** The paper is specific and counter-intuitive here, and getting it
backwards is the classic membership bug.

Two dependencies worth stating: a new server must catch up **before** being counted
in quorum, or adding a node temporarily *reduces* availability — which needs G3's
`InstallSnapshot` to make catch-up bounded. And removing the current leader
requires it to step down after committing the change.

**Testing (rule 3).** Add a fourth node to a running 3-node cluster while it keeps
committing; remove a node; remove the leader; attempt two concurrent changes and
confirm the second is rejected. Election Safety and Leader Completeness asserted
across every reconfiguration.

## G7. Backpressure, rate limiting, graceful shutdown

**The gap.** No load shedding, and shutdown does not drain.

**Design.** Bound the leader's in-flight proposal queue; when it is full, reject
with a typed `Busy` result rather than queueing without limit. **A bounded queue
that rejects is more available than an unbounded one that collapses** — an
unbounded queue converts overload into unbounded latency, and a client that has
given up on a request whose entry still commits is exactly the `Indeterminate`
hazard the client contract warns about. Add per-client token-bucket rate limiting
at the RPC layer.

On shutdown: stop accepting proposals, finish applying what is committed, persist,
then close. The phantom-quorum fix already latches `stopped`, so this extends that
path rather than inventing one.

`bankcli` gains a sixth exit code for `Busy`, distinct from the existing five: a
client must be able to tell "rejected, safe to retry" from `Indeterminate`.

## What this design does not cover

Deliberately out of scope: live resharding and rebalancing, geographic sharding,
follower reads, and service discovery (all LATER.md), plus Phase 3 HLC and Phase 4
UIs.

**Rule 1 applies before any of these is built.** Snapshotting, membership changes,
follower reads, and backpressure are all currently sitting in
`learn/READING_LIST.md`'s "not yet logged" list, and each must be logged with its
primary source before the corresponding item is implemented.

---

# Stage 1 delivered: G4 + G1 + G2 (2026-08-30)

The first three items of the sequencing above, implemented and measured. 147 tests
pass under `-race`.

## G4 — authentication and TLS: DONE

`rpc/security.go`, enforced in `rpc/rpc.go` and both client APIs.

- **Mutual TLS between nodes.** `ClientAuth: RequireAndVerifyClientCert` (not
  `VerifyClientCertIfGiven`, which authenticates only callers who volunteer a
  certificate — the same as nothing, for an attacker who omits one). TLS 1.3
  minimum.
- **Peer identity is bound to the node id.** The certificate's Common Name must
  match the peer being dialled. TLS alone proves only that the peer holds a
  certificate signed by our CA — which every node does — so without this check any
  member could impersonate any other. Verified load-bearing: with the check
  disabled, `TestPeerWithValidCertificateForWrongNodeIsRejected` fails.
- **Half-configured TLS is refused at startup**, following the `-peers` precedent.
  A node with a certificate but no CA would accept any peer while *looking*
  protected.
- **Client bearer tokens**, compared with `subtle.ConstantTimeCompare` so a
  byte-wise comparison cannot leak the matching prefix. Checked before anything is
  proposed: an unauthenticated entry that reaches the log is indistinguishable to
  the ledger from an authorized one.
- `shardnode` warns loudly at startup when either is disabled.

12 tests in `rpc/security_test.go` covering the normal, failure, and regression
paths. The one that matters most: a plaintext `AppendEntries` at term 99 is
refused by a TLS listener, and the forged term is never applied.

## G1 — multi-process sharded deployment: DONE

`rpc/shardnode.go`, `rpc/shardclient.go`, `cmd/shardnode/`.

**[project decision] Multiplexing is by net/rpc SERVICE NAME** — each hosted shard
registers as `Raft-<shard-id>` — rather than a framed header. `net/rpc` already
demultiplexes by service name and sequence number over one connection and runs
each call in its own goroutine, which gives shard independence on a shared
transport for free. The change is confined to registration and addressing; no
consensus code was touched.

A stronger property falls out: an `AppendEntries` for shard-0 *cannot* be
delivered to shard-1's Raft server, because the method does not exist. That beats
validating a shard field inside the message, which is a check that can be
forgotten.

- `ShardHost` hosts N replicas behind one listener; each gets its own Raft server,
  state machine, `.wal` and `.applied`.
- `Transport.ForShard` shares one connection **pool** across shards — N shards on a
  peer cost one connection, not N. The pool is referenced, never copied: copying a
  `Transport` would copy its mutex while sharing its maps, giving each view a
  different lock over the same data.
- `NetworkGroup` is the production `shard.Group`, the counterpart to
  `sim.ShardGroup`.
- **Recovery now runs on leadership acquisition**, not only when a test calls
  `RecoverInDoubt()`. Phase 2's tests invoked it by hand, which is fine for a test
  and useless in production — a real in-doubt transaction would have blocked until
  a human intervened.

### Bug found: single-account operations were misrouted into 2PC

`Coordinator.Transfer` routed on `(From, To)` for every op. For open, deposit, and
withdraw the unused side is the empty account id, and `hash("")` lands on an
arbitrary shard — so every one of them became a cross-shard 2PC between the real
account and an unrelated shard, and aborted.

`sim/` never caught it because its harness proposes single-account operations
straight to the owning group and only calls `Transfer` for real transfers. The bug
was reachable the moment a client API routed everything through one entry point.
Fixed by routing on the account each op actually touches.

## G2 — the sharded write-throughput benchmark: DONE, and it scales

The claim NOW.md has carried unmeasured since Phase 1. Dedicated nodes per shard,
600 writes per shard, 8 concurrent clients per shard, three consecutive runs:

| Shards | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| 1 | 9,992 tx/s | 6,597 tx/s | 6,438 tx/s |
| 2 | 2.09x | 1.98x | 2.30x |
| 4 | 3.35x | 4.64x | 3.33x |

**Sharding adds write capacity.** Together with Phase 1's replica measurement
(3 nodes 119.9 tx/s vs 5 nodes 105.9 tx/s) the full argument is now evidenced in
both directions: replicas buy survivability, shards buy capacity.

Three methodological problems had to be solved first, and each is a finding:

1. **Spurious elections from CPU starvation.** With 12 Raft groups and 32 client
   goroutines on 12 cores, 150-300ms election timeouts fired because goroutines
   were not *scheduled* in time — 26 of 240 writes lost to "lost leadership
   mid-propose", elapsed time inflated 4x. That is §5.2's inequality violated by
   the machine, not by the network. Timings widened for the benchmark; with them
   widened, zero writes are lost.
2. **Directory-fsync contention.** `RaftState.Save` fsyncs the *containing
   directory* on every persist, so nodes sharing a directory serialize their
   fsyncs against each other. Per-node directories cut per-shard time from 2.3s to
   1.2s. This is a deployment lesson, not a test artifact: co-locating nodes' data
   directories couples their write paths.
3. **Runs too short to measure.** At several thousand tx/s a 60-write run finishes
   in ~5ms. The early numbers were noise, and one draft asserted scaling that the
   next run contradicted. Raised to 600 writes per shard.

`TestShardScalingIsBoundedByFsyncOnOneDisk` records the durability cost
separately: fsync-on runs at ~245-250 tx/s for one shard against ~6,500-12,000
detached, so **durability costs 25-50x in absolute throughput** — but sharding
still scales *through* it at ~2.2-2.8x across 4 shards. An earlier draft of that
comment claimed one disk was a hard ceiling sharding could not overcome; the
numbers do not support it, and the claim is corrected in place rather than left
standing.

The absolute cost is also the strongest argument for G3: `RaftState.Save` rewrites
the whole state per persist, so this dominant cost grows with log length.

### Bug found: a data race in the RPC timeout path

`-race` under sustained sharded load caught it. On timeout, `Transport.call`
returned while the background goroutine still held the caller's `&reply`;
`net/rpc` then wrote the late response into that struct while the caller was
reading it.

Pre-existing — the extra concurrency only made it observable. Fixed by decoding
into a scratch reply and copying to the caller only on the success path, where
nothing else can still be writing.

## Still outstanding

G3 (snapshotting), G5 (observability), G6 (membership changes), and G7
(backpressure), in that order. G3's snapshot must include `shard.Machine.txs` and
the ledger's `reserves`, or compaction destroys the 2PC promise the durability
work secured — that remains the first thing to test there.

## Stage 1 completion audit (2026-08-30)

Before starting G3, stage 1 was audited for anything left unfinished. Three gaps
were found and closed. 154 tests pass under `-race`.

### The §8 client redirect was missing from the sharded API — fixed

The single-group `ClientService` has always implemented §8's redirect: a
non-leader replies `NotLeader` plus `LeaderAddr` and the client retries there. The
sharded client API shipped without it. A write to a node that did not lead the
target shard came back as the opaque error `shard: X is not led by this node`,
with no address to retry at.

Worse, it came back **`Indeterminate: true`** — the most dangerous field in the
client contract. `Indeterminate` means "the entry may yet commit, retry with the
same key"; here nothing was ever proposed. The reply told clients to treat a
non-event as an unknown outcome.

This was a real availability gap: with three nodes and one leader per shard, two
thirds of client requests land on a non-leader. The concurrent cross-shard test
had been quietly recording 4 of 12 transfers committing, and that was read as
correct backpressure rather than as the symptom it was.

Fixed with a typed `shard.ErrNotLeader` (typed, not a formatted string, so the
client API can tell a misroute from a genuine failure), address resolution in
`ShardClientService`, and the same treatment for linearizable reads — a follower
cannot promise linearizability, so it redirects rather than serving a stale value
as authoritative. Stale-tolerant reads are still served locally and flagged.

Seven tests in `rpc/shard_redirect_test.go`. With redirects followed, the
concurrent test now commits 12 of 12.

**A guessed address is worse than none.** `leaderAddrFor` returns "" when this
node holds no replica of the owning shard, because it then has no view of that
group's leadership at all. Inventing one sends the client into a retry loop
against a node that cannot serve it, indistinguishable from a slow leader.

### Server-side forwarding: considered and rejected — recorded

`NetworkGroup` carried a `forward` field that was never assigned, so every branch
guarded by it was dead. Rather than implement it, the decision is now explicit in
the type's doc comment: **a non-leader redirects, it does not forward.**
Forwarding hides which node actually served the write, doubles the hops a timeout
can hide behind, and makes an `Indeterminate` result ambiguous between two
nodes' logs. The idempotency key already makes retrying at a new address safe.

Dead `commitTimeout` fields were removed alongside it.

### Reservations limit concurrent transfers — pinned down, not a bug

Found by a test whose premise was wrong. It funded an account with exactly
`n*amount` and asserted all n concurrent cross-shard transfers would commit; two
failed with "insufficient funds".

That is the ledger being **correct**. A cross-shard transfer reserves its debit at
prepare time and holds it until the transaction resolves, so n concurrent
transfers each hold their amount against the available balance at once. The money
is not gone, it is spoken for — which is precisely the property that stops the
same money being spent twice.

`TestConcurrentTransfersAreLimitedByReservedFunds` now asserts it deliberately: 6
concurrent transfers of 10.00 against a 30.00 balance commit exactly 3, refuse 3,
and conserve the total. The lesson generalises: a test that asserts "everything
succeeds" under concurrency is often asserting that safety is absent.

# G3 — snapshotting and log compaction: DONE (2026-08-30)

§7 implemented, wired into both node binaries, and proven end to end. 182 tests
pass under `-race`.

## What landed

- **`raft/snapshot.go`** — `Snapshotter` and `SnapshotStorage` interfaces,
  `MaybeCompact`, `InstallSnapshot`, and the snapshot record codec.
- **Compaction-aware log indexing** (`raft/log.go`). `log[0]` was always a
  sentinel at index 0, so index and slice position coincided; after compaction the
  sentinel becomes the snapshot boundary and carries
  `lastIncludedIndex`/`lastIncludedTerm`. Every translation goes through
  `baseIndex`/`baseTerm`/`slot`. This was the localization the old `entryAt`
  comment promised — "this assumption breaks when snapshotting arrives and is
  deliberately localized here" — being cashed in rather than a new one invented.
- **`ledger/snapshot.go` and `shard/snapshot.go`** — full state capture.
- **`storage/raftstate.go`** — snapshots in a separate CRC32-checksummed file
  written atomically. Separate from the state file deliberately: the state file is
  rewritten on every persist, so folding a large snapshot into it would multiply
  exactly the write amplification snapshotting removes.
- **`Restore` loads the snapshot before replaying**, and replay starts at
  `baseIndex()+1`. Starting at index 1 would double-count everything the snapshot
  covers — the state machine would look plausible and be wrong by exactly the
  compacted prefix.
- **Both binaries compact periodically** (`-snapshot-threshold`, default 1000;
  `-snapshot-interval`, default 30s), on a timer but deciding on SIZE, since the
  cost compaction bounds is a function of log length.

## The wall is gone

`TestCompactionReducesPersistedStateSize`: persisted state drops from **10,729
bytes to 39** after compacting 400 entries — **275x smaller**. The 481x write
amplification measured at 800 entries no longer grows without bound.

## The dangerous part, tested first

G3's design named it: a snapshot must capture everything the state machine holds,
not merely balances. `shard.Machine` also holds 2PC promises and the ledger holds
reservations.

Every field earns its place, and the reasoning is recorded in each file: dropping
`applied` makes a retry execute twice; dropping `fingerprints` lets a key be
reused for a different request; dropping `reserves` frees money already promised
to a 2PC transaction; dropping `history` destroys the audit trail so a compacted
node can no longer prove its own books. `byIndex` is deliberately excluded — its
keys are the log indices being discarded.

**Verified the tests are load-bearing.** With `txs` deliberately dropped from the
snapshot, `TestSnapshotPreservesPreparedPromiseAndReservation` and
`TestSnapshotPreservesDurableDecision` both fail with exactly the right
diagnosis. `TestCompactionPreservesInDoubtPromiseAcrossRestart` proves the same
thing end to end on a real cluster: compact while a transaction is in doubt,
restart, and the promise and its reservation both survive.

## Bug found and fixed: recovery raced the apply loop

Not a G3 bug in origin — G3's changes altered timing enough to expose it.

`Coordinator.RecoverInDoubt` scanned each leader's in-doubt set immediately. A
freshly elected leader, especially one still replaying its log after a restart,
may not yet have APPLIED the prepare entries that put transactions in doubt. The
scan then saw an empty set, reported success, and left real blocked transactions
holding customer funds. It reproduced in roughly **2 runs out of 10**.

The intermittency is what made it dangerous: it looked like flakiness, and the
obvious "fix" of retrying in the test would have hidden a real defect in recovery.

Fixed by committing a no-op through each group and waiting for it to apply before
scanning — the same device §8 uses for a new leader's commit index. After our own
entry applies, everything ordered before it has applied too, so the in-doubt set
is complete rather than partial.

`TestRecoveryIsNotRacedByTheApplyLoop` runs 25 restart-and-recover rounds.
Verified load-bearing: with the catch-up removed it fails 2 of 3 runs; with it,
it passes consistently.

## Test corrected: a follower may legitimately lag

`TestRedeliveredOutcomeAfterRestartIsIdempotent` began failing intermittently.
The diagnosis was NOT a compaction bug: `ShardCluster.Balance` reads the current
leader's state machine, and after a restart a replica that had not yet applied the
newest entry can win the election and serve a stale balance until replication
catches it up.

That is correct Raft behaviour — followers are allowed to lag — and is exactly why
§8 makes a linearizable read confirm with a majority instead of reading local
state. A test asserting a balance immediately after an election is asserting
against whichever replica happened to win. It now waits for convergence, and the
reasoning is recorded in `waitForBalance`.

## Still outstanding

G5 (observability), G6 (membership changes), and G7 (backpressure), in that
order. G6's dependency on G3 is now satisfied: `InstallSnapshot` makes a new
server's catch-up bounded, which is what lets a node be added without temporarily
reducing availability.

## G3 completion: three issues found by the full parallel suite

Everything above passed in isolation. Running the whole suite under `-race`
surfaced three more, each a different kind of problem and worth separating.

**1. A test asserting against whichever replica won the election.**
`TestRedeliveredOutcomeAfterRestartIsIdempotent` failed intermittently.
`ShardCluster.Balance` reads the current leader's state machine, and after a
restart a replica that had not yet applied the newest entry can win and serve a
stale balance until replication catches it up. That is correct Raft — followers
are allowed to lag — and is exactly why §8 makes a linearizable read confirm with
a majority instead of reading local state. The test now waits for convergence.

**2. Setup fragile under CPU starvation.** `TestRecoveryIsNotRacedByTheApplyLoop`
failed at `open acct-2: shard shard-1 has no leader` — during SETUP, not
recovery. Under a full parallel `-race` run the simulator's 60-120ms election
timers lose elections to scheduling delay, and 18 rounds build 36 clusters, which
amplifies the exposure. Setup now retries; the property under test is unchanged.

**3. Two throughput benchmarks measuring each other.** Split across two tests,
they ran back to back and contended for the same cores: the 1-shard case measured
13,712 tx/s while a later 4-shard case was starved to 8,703, producing an apparent
0.63x that said nothing about sharding. Merged into one test that measures each
configuration once. A throughput benchmark has to own the machine while it runs.

Related, and recorded rather than quietly worked around: **the two throughput
assertions now skip under `-race`.** The detector instruments every memory access,
making 12 Raft groups on 12 cores CPU-bound, so shards contend instead of running
in parallel and 4 shards measure *slower* than 1 (0.79x observed). That is the
instrumentation being measured — the same class of artifact as the in-process
benchmark this project already rejected. Every correctness test still runs under
`-race`, including all the sharded ones; only the two timing assertions opt out.

Verified: four consecutive clean full-suite runs, two with `-race` and two
without.

# G5 — metrics, health endpoints, structured logging: DONE (2026-08-30)

209 tests pass under `-race`. Verified on real processes, not only in tests.

## What landed

- **`obs/`** — a new package (rule 2: genuinely new scope, not a variant of an
  existing one) serving `/metrics`, `/healthz`, `/readyz`, and `/status` on its
  own port.
- **`raft.Server.Health`** — the primitive everything else reads. Answers the
  question no existing method could: *can this node actually commit?*
- **`lastContact` tracking** — a leader records which peers have acknowledged it
  recently, which is what makes quorum visible without a round trip.
- **Structured logging** via `log/slog`, tagged with node and shard on every line.
- Both binaries wired: `-obs-listen`, `-log-format`, `-log-level`.

## The distinction that carries the weight

`/healthz` answers **liveness** (is the process running?). `/readyz` answers
**readiness** (can this node serve?). Conflating them produces opposite failures,
which is why they are separate endpoints rather than one:

- A readiness check wired to liveness **restarts a node that was merely waiting
  out a partition** — turning a recoverable degradation into a restart storm that
  destroys the cluster's remaining quorum.
- A liveness check wired to readiness **keeps routing writes to a node that cannot
  commit** — the degraded-quorum blindness this closes.

Proven on real processes: a 3-node cluster reduced to one node reported

```
readyz:  HTTP 503   shard-0: NOT READY (leader has heard from 1 of 2 needed for quorum; cannot commit)
healthz: HTTP 200
corebank_raft_ready{node="n1",shard="shard-0"} 0
```

The reason travels with the status. A bare 503 forces whoever is paged to guess.

## Bug found in G5's own code: contact recorded on failed replies

The first implementation recorded quorum contact on **any** reply, reasoning that
a consistency-check failure is a healthy peer disagreeing about its log rather
than an unreachable one. That reasoning is wrong, and only the end-to-end test
caught it.

A **stopped** node also replies, with `Success=false`: `raft.Server` latches
`stopped` and refuses to participate while its RPC handlers keep answering over
the shared listener — the phantom-quorum fix. Counting those replies meant a
leader whose every peer had been shut down still reported a full quorum, which is
precisely the blindness the feature exists to remove. The unit tests passed
because they set `lastContact` directly.

Now recorded only on `Success`. A peer genuinely repairing its log converges
within a few rounds and then succeeds, so the cost is a brief not-ready window
during repair. That is the right trade: reporting NOT ready while repairing is
conservative; reporting ready while isolated is dangerous.

## Latent panic fixed: nil transport

`replicateTo` and the vote path dereferenced `s.transport` without a nil check,
while `NewServer` explicitly documents that a nil transport is permitted for
testing receiver rules in isolation. A documented configuration crashed the
process. Guarded in all three call sites.

## Metric set, and why each earns its place

Chosen for what reveals **disagreement**, which is what matters in a consensus
system: `corebank_raft_ready` (the alertable form of the degraded state),
`quorum_contact`/`quorum_needed`, `apply_lag` (commit minus applied — the state
machine falling behind, invisible otherwise), `raft_term` (churn is §5.2's
liveness cost), `log_entries` and `snapshot_index` (the compaction decision), and
`corebank_txn_in_doubt` — 2PC blocking made visible, the one number that
distinguishes "slow" from "stuck holding customer funds".

**[project decision]** Prometheus text format emitted by hand rather than via the
client library, keeping the zero-dependency rule — the same reasoning that chose
`net/rpc` over gRPC. The test parses the output and checks every sample line has a
preceding HELP and TYPE, so "well-formed" is asserted rather than assumed.

**[project decision]** A separate port from RPC. An endpoint sharing the consensus
port cannot be scraped while the consensus path is saturated, which is exactly
when the numbers are needed. It also keeps the auth story clean: the RPC port
requires mutual TLS, while metrics are read by a scraper holding no cluster
credentials.

## Still outstanding

G6 (membership changes) and G7 (backpressure). G6's dependencies are now both
satisfied: `InstallSnapshot` bounds a new server's catch-up (G3), and `/readyz`
makes it possible to tell whether a reconfiguration left the cluster able to
commit (G5) — which the design noted as the reason G6 was unsafe to attempt
before this.

## Pre-existing test flakiness, found and fixed during G5

Running the full suite repeatedly under `-race` exposed intermittent failures in
tests that predate this work. Measured at **HEAD, with every G5 change stashed:
roughly one failure per three full runs.** Different test each time —
`TestClusterSurvivesFullRestart`, `TestIdempotencyKeyIsBoundToItsRequest`,
`TestRetriedWriteAppliesOnce`, `TestRepeatedCompactionStaysCorrect` — with the
same signature every time: "leadership lost", "not leader", "no leader".

That signature is not a correctness defect. It is §5.2's inequality being violated
by the machine:

```
broadcastTime  <<  electionTimeout  <<  MTBF
```

The race detector instruments every memory access, inflating broadcastTime until
it is no longer well below the election timeout. Followers then time out on a
perfectly healthy leader, and a test that finds a leader and submits to it is
racing an election it did not cause. The same effect was measured directly in the
sharded throughput benchmark, where widening the timings took lost writes from
26-of-240 to zero.

Two fixes, matching the two shapes of the problem:

1. **Timings widened for the test harness.** `rpc` clusters went 80-160ms to
   400-800ms; the four `sim` constructors now share one documented `simConfig()`
   at 250-500ms. Still far below any real deployment, so nothing these tests
   exercise changes. The chaos tests that *deliberately* violate the inequality set
   their own timings and are untouched — their point is that bad timing costs
   liveness and never safety, and they still make it.

2. **Submit through leadership changes.** `Cluster.SubmitWithRetry` retries at
   whoever currently leads, which is exactly what the client contract prescribes on
   `NotLeader`. A test that instead fails is asserting "no election happened during
   this window", which is not a property Raft offers.

Verified: **six consecutive clean full-suite runs** (four under `-race`, two
without) where the same command previously failed about one run in three.

## Pre-G6 gap audit: InstallSnapshot was never sent (2026-08-30)

Auditing before starting G6 found that **G3 shipped half-finished**, and the way
it hid is the most instructive part.

### The gap

`InstallSnapshot` was implemented as a RECEIVER — Figure 13's rules, tested, and
correct — but **nothing ever sent one.** No leader called it, and
`raft.Transport` had no method for it. A follower that fell behind the leader's
compacted prefix could therefore never converge.

Worse, the missing send path was not merely absent. `replicateTo` computed
`s.slot(next)` for a follower below the snapshot boundary, which is a **negative
slice index**, and the leader panicked:

```
panic: runtime error: slice bounds out of range [-49:]
```

A follower falling behind a compacted leader is routine — it crashes and
restarts, or is briefly partitioned — so an ordinary event crashed the leader
process.

### Why every G3 test passed anyway

`sim.CountingSM` had a method `Snapshot() []string`, a convenience returning what
it had applied. `raft.Snapshotter` requires `Snapshot() ([]byte, error)`. The two
signatures **collide**, so `CountingSM` could never satisfy the interface —
`MaybeCompact` silently declined for every simulator cluster, and every
compaction test passed **by doing nothing at all**.

That is the real lesson: a "did it compact?" assertion is not the same as an
assertion that compaction *happened*. The convenience method is now
`AppliedCopy()`, the serializing pair carries the interface names, and a
compile-time `var _ raft.Snapshotter = (*CountingSM)(nil)` asserts the interface
is satisfied rather than assuming it.

### Fixes

- **`raft.SnapshotTransport`** — an optional Transport extension, so a transport
  predating snapshotting still satisfies the base interface. A leader whose
  transport lacks it counts the failure rather than pretending, because the
  alternative is a permanently lagging replica that still counts toward cluster
  size.
- **`Server.sendSnapshotTo`** — the send path, including advancing
  `nextIndex`/`matchIndex` on success (without which the leader resends the same
  snapshot every heartbeat forever) and recording quorum contact.
- **Both transports implement it**: `sim.Network` subject to the same drops,
  delays and duplicate delivery as any other RPC, and `rpc.Transport` with a
  longer timeout (10x, floor 5s) because a snapshot is larger than a heartbeat and
  the per-RPC timeout would make every install fail on a large state machine.
- **Dead code removed**: `raft.encodeSnapshot`/`decodeSnapshot`, superseded by
  `storage`'s checksummed record format.
- **`HasSnapshot` wired into `/status`**, distinguishing "never compacted" from
  "compacted at index 0".

### The safety checkers were stale too

With compaction now actually happening, `CheckLogMatching` and
`CheckLeaderCompleteness` reported a **Log Matching violation** — and it was the
checkers, not the code.

Both walked logs by **slice position**, which equalled log index only because
nothing had ever compacted. A compacted node's slice starts at its snapshot
boundary, so position *k* means a different entry on each node. Both now index by
log index, which is what the properties are stated over.

A second, subtler bug in the same place: `entries[0]` is the sentinel, and after
compaction it carries a real index and term but no command — it stands in for an
entry whose content now lives in the snapshot. Comparing it against the real entry
another node still holds reported `"" vs "part-23"` at index 25, both term 1.
Skipping by **position** rather than by index is what makes it correct in both the
compacted and uncompacted cases.

Verified: three consecutive clean full-suite runs under `-race`, and
`TestCrashedFollowerCatchesUpBySnapshotAfterCompaction` shows a crashed follower
going from 0 to 30 entries via InstallSnapshot with all five Figure 3 properties
holding.

# G6 — cluster membership changes: DONE (2026-08-30)

236 tests pass under `-race`. A cluster can now replace a failed node without
full-cluster downtime.

## What landed

- **`raft/membership.go`** — `Configuration`, `AddServer`, `RemoveServer`, the
  single-server safety rule, and the disruptive-server check.
- **Configuration as a log entry**, marked by a `0xC0` prefix. A prefix rather
  than a new field because `LogEntry.Command` is opaque bytes by design (DESIGN.md
  §7) — `raft/` still knows nothing about what the application puts there.
- **Followers adopt on append**, leaders check self-removal on commit, and
  `Restore` re-learns membership from the log.

## [project decision] Single-server changes, not joint consensus

The extended paper's §6 describes joint consensus, a two-phase transition through
a configuration requiring separate majorities of both old and new. This implements
the dissertation's §4.1 alternative instead: **add or remove exactly one server at
a time.**

The safety argument is a counting one, and it is what makes the simpler mechanism
correct. When two configurations differ by one server, any majority of the old and
any majority of the new **must overlap in at least one server**, and a single
server never votes twice in one term. Two disjoint majorities therefore cannot
exist, so joint consensus is unnecessary. Ongaro's own dissertation adopts this as
the primary approach, and it is what etcd and Consul use.

Enforced rather than assumed: `differsByOne` refuses anything else, and
`ErrConfigChangeInFlight` refuses a second change while one is uncommitted —
because two concurrent changes can compose into a difference of two, breaking the
overlap argument even though each alone is safe.

## The counter-intuitive rule, and proof the test catches it

§6: *"a server always uses the latest configuration in its log, regardless of
whether the entry is committed."* Applying on **append**, not on commit.

Getting it backwards is the classic membership bug: servers would be VOTING under
the old configuration while the leader COUNTED under the new one, which is exactly
the disjoint-majority hazard the design exists to prevent.

**Verified load-bearing.** With the change made to wait for commit,
`TestAddServerAppendsConfigEntryAndTakesEffectImmediately` fails with the right
diagnosis, and `TestLeaderRemovingItselfKeepsServingUntilCommit` fails too.

## The one place it does NOT act on append

A leader removing **itself** keeps serving until the change commits, then steps
down. Stepping down on append would strand the very entry that removes it, leaving
the cluster with a configuration nobody can complete. `checkSelfRemovalLocked`
runs on commit, and the test proves both halves: still Leader immediately after
proposing, Follower once committed.

## The disruptive server

A removed server stops receiving heartbeats, times out, increments its term, and
campaigns. Its higher term forces the real leader to step down even though the
caller is no longer a member — indefinitely.

The answer (dissertation §4.2.3) is to ignore `RequestVote` received within the
minimum election timeout of hearing from a current leader. Two details matter:

- **It runs BEFORE `observeTerm`.** Adopting the term *is* the damage — a vote
  refused after the term has already been adopted has still deposed the leader.
- **It is not gated on "is the caller a member".** A server that legitimately fell
  out of contact must still be able to campaign once the leader really is gone,
  and by then the heartbeat window will have lapsed. `TestVoteIsGrantedOnceTheLeaderStopsReachingUs`
  holds that line.

## Tested end to end

`sim/membership_test.go` proves the cluster keeps working through changes, which
is the entire point: remove a follower and writes keep flowing; add a server and it
catches up to 11 entries; remove the **leader** and it steps down while survivors
take over; and reconfigure under 5% message loss with all five Figure 3 properties
still holding.

## Still outstanding

G7 (backpressure, rate limiting, graceful shutdown) — the last item on the
sequenced list. After that, Phase 3 (HLC) and Phase 4 (the UIs), for which G5's
`/status` endpoint is the data source.

**Not yet wired to the binaries.** `AddServer`/`RemoveServer` exist on
`raft.Server` and are exercised in tests, but no admin RPC exposes them, so a
running cluster cannot be reconfigured from outside yet. That is deliberate
scope: the consensus mechanism is what G6 is about, and the admin surface belongs
with the operational work in G7.

## Post-G6 gap audit (2026-08-30)

Three gaps closed before starting G7.

**1. Membership was invisible from outside.** `configErrs` was counted and
`ConfigFailures()` existed, but neither reached an operator. A node left behind on
an old configuration is dangerous in a specific way — it counts quorum against a
membership that no longer exists — and nothing in the role/term/commit signals
reveals it. A reconfiguration that reached two of three nodes looked exactly like
one that reached all three.

Now exposed as `corebank_raft_cluster_size` and
`corebank_raft_config_failures_total`, with the member ids named in `/status`.
Counting a size is not enough on its own: an operator diagnosing a stuck
reconfiguration needs to know *which* server is in or out.

**2. Two quorum computations, unverified against each other.**
`Configuration.Majority()` and `Server.majority()` both compute a majority.
`TestQuorumFollowsConfigurationChanges` now asserts they agree, and that the
server's quorum tracks a configuration change — 2-of-3 becoming 3-of-4 after an
`AddServer`. Two quorum sizes in one codebase is how a cluster commits against a
majority that is not one.

**3. A latent flake with a misleading symptom.** `TestCrossShardTransferRetryIsIdempotent`
failed about one full-suite run in four with `first transfer: shard: transaction
aborted`.

The abort was real and correct: the account had never been opened. Seven call
sites discarded the error from `c.open()`, so a setup failure surfaced much later
as an unrelated assertion about insufficient funds — a symptom two steps removed
from its cause. The underlying cause was the familiar one: an election lost to
scheduling delay between finding the leader and proposing.

Fixed at both levels. `open` now retries while leadership is in flux, and
`mustOpen` fails the test where the setup actually breaks. It distinguishes
transient failures from real ones — a ledger refusal such as "already open" is
returned immediately rather than retried, since retrying would only mask it.

Verified: **five consecutive clean full-suite runs under `-race`**, where the same
command previously failed roughly one run in four.

# G7 — backpressure, rate limiting, graceful shutdown: DONE (2026-08-30)

The last item on the sequenced list. 258 tests pass under `-race`.

## The result this is built on

**A bounded queue that REJECTS is more available than an unbounded one that
accepts everything.** An unbounded queue does not remove the capacity limit; it
converts a visible rejection into invisible latency, and eventually into timeouts
on requests the system is still working on.

For this system the argument is sharper twice over. The Raft leader is the
bottleneck **by construction** — Phase 1 measured 3 nodes at 119.9 tx/s against 5
at 105.9 — so it cannot shed load by scaling out, and the queue in front of it is
the only control. And a client that times out on a write whose entry still commits
is exactly the `Indeterminate` hazard in the client contract: the outcome is
unknown, and a client that records it as "did not happen" and reissues under a new
key double-sends the money.

**An unbounded queue manufactures that hazard at scale.** Bounding it converts
those requests into `Busy` — nothing was proposed, so retrying is unambiguously
safe. That is the whole point, and the reason `Busy` is a distinct outcome rather
than a flavour of failure.

## What landed

- **`rpc/admission.go`** — `Limits`, `Admitter`, a token-bucket rate limiter.
- **`Busy` as a fifth client outcome**, with `RetryAfter`. `bankcli` gains exit
  code **6**, distinct from `4` (indeterminate) because the correct response
  differs completely.
- **`rpc/drain.go`** — ordered shutdown.
- Both binaries: `-max-in-flight`, `-client-rate`, `-client-burst`,
  `-drain-timeout`, and a startup warning when admission control is off.
- **Admission metrics** in `obs`, omitted entirely when no admitter is attached —
  zeroes would make "backpressure is off" indistinguishable from "backpressure is
  on and nothing was shed".

## Design decisions

**The bound is on in-flight proposals, not connections.** Connections are cheap;
what is scarce is the leader's ability to replicate and commit.

**Rate limiting and load shedding are kept distinct.** Rate limiting is a
per-client *policy* enforcing fairness whether or not the system is busy; shedding
is a *reaction* to saturation, applied to everyone. Conflating them is a common
mistake — a global rate limit lets one aggressive client consume everyone's budget.

**Unidentified callers share one bucket** rather than being exempt, since exempting
them would make the limit bypassable by leaving `ClientID` empty.

**The in-flight counter is maintained even with no limits configured**, so
graceful shutdown works on a node that never opted into backpressure. An operator
should not get an abrupt exit as a consequence of declining rate limiting.

**Shutdown order is the whole content of `drain.go`**: stop admitting → wait for
in-flight → give up leadership → close. Giving up leadership first would strand
proposals this node is still the only one able to commit; closing first drops
answers on the floor, which is what draining exists to avoid. A drain that times
out **reports** what was still in flight rather than pretending it was clean.

**Verified load-bearing.** Changing the shed reply from `Busy` to `Indeterminate`
makes `TestShedWriteIsBusyNotIndeterminate` fail with exactly the right diagnosis.

## Bug found: an abort with no reason

Chasing an intermittent failure — `transaction aborted (res={OK:false Err:})` —
turned up a real diagnostic gap rather than a flake.

The 2PC credit-side abort collapsed **three distinct causes** into a bare
`ErrTxAborted` with an empty `Err`: the ledger refusing the operation, the shard
being unreachable, and this node simply not leading the credit shard. Those call
for three different responses, and the reply distinguished none of them.

The `!isLeader` case is now typed as `ErrNotLeader`, so the client API redirects
instead of reporting a failure; the transport-error and no-reason cases carry
their own text. The intermittent failure was the familiar leadership race, and it
now surfaces as a **redirect** rather than an unexplained abort.

Verified: **eight consecutive clean full-suite runs under `-race`.**

## The sequenced list is complete

G1 through G7 are all done. What remains is Phase 3 (HLC) and Phase 4 (the UIs),
plus the items LATER.md deliberately defers: live resharding, geographic
sharding, follower reads, and service discovery.

One piece of G6 is still not exposed on the wire: `AddServer`/`RemoveServer` work
and are tested, but no admin RPC calls them, so a running cluster cannot yet be
reconfigured from outside. Recorded in the README's gap list.

# Phase 3 — Hybrid Logical Clocks: DONE (2026-08-30)

The last unbuilt item of Phase 3. Its other three — idempotency keys, double-entry
enforcement, and the event-sourced ledger — were built early, during Phases 1-2.
276 tests pass under `-race`.

## The gap HLC closes

`Transaction.Seq` is a per-shard monotonic counter. It orders events perfectly
*within* one shard and says nothing across shards: `Seq=7` on shard-0 and `Seq=7`
on shard-1 are unrelated numbers. So the system could not answer questions any
bank asks — show a customer's transactions in order when their accounts span
shards, or what the books looked like at 14:32.

That is not hypothetical here: **the two legs of a cross-shard transfer live in
different Raft logs by construction**, which is why Phase 2 had to resolve
double-entry as a global invariant rather than a per-shard one.

Wall-clock time cannot supply the answer. Clocks disagree between machines and can
jump backwards (NTP, VM migration), so a debit could carry a later timestamp than
the credit it caused. And the determinism rule forbids reading the clock at apply
time at all — two replicas would produce different state from the same log.

## What landed

- **`hlc/`** — `Timestamp` (wall + logical counter), `Clock` with `Now` and
  `Update`, injectable physical time so tests can drive a clock *backwards*.
- **`ledger.Command.Timestamp`**, assigned by the leader before the entry is
  appended, and carried into `Transaction`.
- **Wire compatibility preserved**: the timestamp is APPENDED after every existing
  field, so a record written before it existed decodes with the zero value. A
  partially-present timestamp is refused as truncation rather than half-read.
  Inserting the field mid-record would have orphaned every existing WAL — and for
  a bank the log *is* the audit trail.

## Design decisions

**[project decision] HLC, not TrueTime.** Spanner's external consistency needs GPS
and atomic clocks plus a commit wait on every transaction. That is hardware this
project does not have, solving a problem — externally-consistent ordering of
*causally unrelated* transactions — a single-machine system cannot exhibit.
CockroachDB made the same trade. The weaker guarantee is stated plainly rather
than papered over: **this gives causal ordering across shards, not external
consistency.**

**The timestamp is in the command, not read at apply time.** This is the whole
safety argument. `TestEveryReplicaRecordsTheSameTimestamp` asserts it directly:
all three replicas of a shard record an identical timestamp, because they apply
the same bytes from the same log.

**The idempotency fingerprint deliberately excludes the timestamp.** A retry is
stamped fresh, so including it would make every retry look like a different
request and return `Conflict` instead of the original result. The fingerprint
answers "is this the same operation" — when it happened is not part of that.

## Gap found and closed: the 2PC legs were unstamped

The first implementation stamped client commands but not the internal legs
`CommitDebit`/`CommitCredit` book. Those are *precisely* the transactions Phase 3
exists to order — they are the ones in different Raft logs — so HLC would have
been wired everywhere except where it mattered.

`TestBothLegsOfACrossShardTransferShareATimestamp` caught it, and the first
version of that test **skipped** rather than failing. Skipping would have left the
gap in place looking green, so the test now fails outright and asserts the
stronger property: both legs carry an **identical** timestamp, because they are one
transaction booked in two logs.

`outcomeTimestamp` falls back to the prepare-time timestamp when an outcome
carries none, which matters for recovery: a re-delivered outcome is rebuilt from
the stored record, and booking it at "no time" would leave exactly the
transactions that needed recovery missing from the ordering.

Verified load-bearing — with leg stamping removed, the test fails with the right
diagnosis; with the backwards-clock handling removed,
`TestClockGoingBackwardsNeverProducesAnEarlierTimestamp` fails.

## Phase 3 is complete

What remains is **Phase 4** — the two UIs under `fe/`, still static mockups on
fake data. G5's `/status` endpoint is their data source, and Phase 2's ring is
what the dashboard's hash-ring view renders.

# Phase 4 — the demo UIs: DONE (2026-08-30)

286 tests pass under `-race`. `fe/` is no longer mockups on fake data: both UIs
drive a real cluster.

## What landed

- **`demo/`** — an in-process sharded cluster with an HTTP surface: an SSE stream
  of the whole cluster state, plus control endpoints for moving money, killing
  nodes, reviving them, and running 2PC recovery.
- **`cmd/demo/`** — the binary. `go run ./cmd/demo`, then open the two pages.
- **`fe/bank-app/`** — multi-window client. Each window is an independent session
  with its own id; "New window" opens another.
- **`fe/cluster-dashboard/`** — live shards, nodes, hash ring, transactions
  ordered by HLC across shards, and an event feed.
- **`shard.HashKey`** exported, so the ring view draws the REAL placement rather
  than a decorative approximation.

## [project decision] SSE + HTTP control, not WebSocket

NOW.md's frontend stack decision named `gorilla/websocket` or
`nhooyr.io/websocket`. Both are third-party, and this module has zero third-party
dependencies — a property the README advertises and that already shaped two
earlier decisions (`net/rpc` over gRPC, hand-written Prometheus text over the
client library).

Server-Sent Events gets the same user-visible result in a few lines of
`net/http`. The stream is one-way, and the control actions are one-shot commands
that fit ordinary HTTP requests; WebSocket's bidirectional framing solves a
problem this does not have. **The deviation from NOW.md is recorded rather than
silent**, and the user chose it explicitly when the conflict was raised.

Push rather than polling, and that is not taste: a candidate state lasts one
election timeout, a follower's pending-to-committed transition lasts one round
trip, and an in-doubt 2PC transaction may resolve in milliseconds. Poll at one
second and the UI shows outcomes, never the mechanism. The stream ticks at 100ms
for that reason.

## The two questions NOW.md deliberately left open

Both were settled by the backend, and the UI now displays what the backend
actually does — which is the direction RULES.md rule 3 mandates.

**Concurrent same-account behaviour.** The ledger serializes withdrawals through
the Raft log, so exactly the available funds can be withdrawn and the balance
never goes negative. `TestConcurrentWithdrawalsNeverOverdraw` asserts it: six
concurrent withdrawals of 10.00 against a 30.00 balance commit at most three, and
the total is exactly right.

**Backend wiring.** Every request goes through `shard.Coordinator`, so the UI uses
the same path as any other client, including 2PC for cross-shard transfers.

## A complete snapshot per frame, not a delta

That is what makes a dropped frame harmless — a client that misses three and
receives the fourth is fully current — which in turn is what lets the server skip
for a slow consumer rather than buffering without bound. Same argument as §18's
bounded queue, in a different costume.

## Bug found: the sim harness picked stale leaders

`ShardGroup.leader()` returned the first node reporting `Leader`, ignoring terms.

After a failover there are genuinely **two** servers reporting Leader: the
partitioned one still in the old term, and the real one in the new term — Raft
does not take leadership away from a node that merely stops receiving replies.
Returning whichever came first in the id list picked the stale one about half the
time, and every proposal to it then timed out, because it cannot reach a majority.

This is the same phantom-leader class the phantom-quorum fix addressed at the RPC
layer, and the answer is the one Raft itself uses: **the higher term wins.**
Verified load-bearing — without the term comparison,
`TestKilledNodeIsReportedDownAndShardFailsOver` fails 3 of 3 runs.

It was a real defect in shared test infrastructure, not merely a demo problem.

## Verified live, not only in tests

Ran the binary and drove it over HTTP: opened accounts across three shards,
committed a cross-shard transfer, killed the leader of shard-0, watched
`shard-0-n1` take over in term 2 with the dead node reported `Down`, and committed
a write immediately afterwards. Reviving the node showed it rejoin and catch up to
the same applied index.

## SAFETY BOUNDARY

The demo exposes unauthenticated endpoints that kill nodes and move money. It is a
**separate binary** that must be started deliberately, never a flag on `node/` or
`shardnode/`. The startup banner says so.

## All four phases are complete

Phases 1-4 and the whole G1-G7 hardening sequence are done. What remains is
LATER.md's deliberately deferred work: live resharding, geographic sharding,
follower reads, and service discovery — plus exposing membership changes on the
wire, which is the one item still on the README's gap list.
