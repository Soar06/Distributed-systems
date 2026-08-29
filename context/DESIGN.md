# DESIGN — Current Work Spec

The concrete design for **what is being built right now**. Scope vs spec:
[NOW.md](NOW.md) says what we build and in what order; this file says how the thing
being built is actually specified — states, transitions, message shapes, structures,
invariants. Read NOW.md to decide what to work on; read this while writing the Go.

**Current phase: Phase 1 — single replicated ledger (consensus core).**

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

- [x] Phase 1 design written against Figure 2 / Figure 3 of the extended paper.
- [x] `raft/` — Figure 2 complete: state, both RPC receiver implementations, and the
      Rules for Servers role loop (randomized election timeouts, candidate
      elections, leader heartbeats, `nextIndex` decrement-and-retry replication, and
      the commit rule including the `log[N].term == currentTerm` safety check).
- [x] `sim/` — deterministic seeded network with fault injection (crash, partition,
      packet loss, duplication), a cluster harness, and executable assertions for
      **all five** Figure 3 safety properties. Leader Completeness became testable
      once elections existed.
- [x] Cluster view (`sim/view.go`) — per-node role/term/commit/applied/log snapshot,
      rendered on test failure. Built in the shape NOW.md's Phase 4 dashboard needs,
      so wiring the UI later is a rendering job, not a redesign.
- [ ] **Not implemented:** persistence to stable storage (`storage/`), gRPC
      transport (`rpc/`), the ledger domain (`ledger/`), the node binary (`node/`),
      and the client API. Raft state is currently in-memory only, so a crash loses
      votes — Figure 2's "before responding to RPCs" durability requirement is
      **not yet met**.
- Next: `storage/` (WAL for persistent state) or `ledger/` (the real state machine),
  then `rpc/` + `node/` to make nodes separate processes.
