# Code map: where the theory actually lives

Every claim below names a real function and a real line. If a line number has
drifted, the function name is the durable anchor — search for that.

Primary source throughout: **Ongaro & Ousterhout, "In Search of an Understandable
Consensus Algorithm (Extended Version)", 2014-05-20.** Section numbers written
`§5.2` refer to that paper. Numbers above §12 are this project's own
`READING_LIST.md` topics, not the paper.

A note on how to read this: Raft's paper is organised around **Figure 2** (the
state and the two RPCs) and **Figure 3** (five safety properties). This document
follows the same split — first the mechanism, then the properties the mechanism
is there to guarantee.

---

## 1. Figure 2 — persistent and volatile state

| Paper | Field | Where |
|---|---|---|
| `currentTerm` | `s.currentTerm` | `raft/server.go` |
| `votedFor` | `s.votedFor` (`*NodeID`, nil = none) | `raft/server.go` |
| `log[]` | `s.log` | `raft/server.go` |
| `commitIndex` | `s.commitIndex` | `raft/server.go` |
| `lastApplied` | `s.lastApplied` | `raft/server.go` |
| `nextIndex[]` | `s.nextIndex` | `raft/server.go` |
| `matchIndex[]` | `s.matchIndex` | `raft/server.go:33` |

**`votedFor` is a pointer, not a sentinel.** The paper says "candidateId that
received vote in current term (or null if none)". Using `*NodeID` makes "no vote"
unrepresentable as a valid node ID, so the null case cannot be confused with a
vote for a node that happens to be the zero value.

**Persistence.** The paper requires `currentTerm`, `votedFor`, and `log[]` to be
"updated on stable storage **before responding to RPCs**". That ordering is the
whole point — persisting after replying would let a node forget a vote it already
promised and vote twice in one term.

- `encodeState` / `decodeState` — `raft/persist.go:49` / `:86`
- `persistLocked` — `raft/persist.go:163` (called before the reply is returned)
- `mustPersistLocked` — `raft/persist.go:176`
- `Restore` — `raft/persist.go:189`

---

## 2. Figure 2 — AppendEntries RPC

`Server.AppendEntries` — **`raft/server.go:281`**

The paper's five receiver rules, in the order the code applies them:

1. **"Reply false if term < currentTerm"** — `raft/server.go:297`.
2. **"Reply false if log doesn't contain an entry at prevLogIndex whose term
   matches prevLogTerm"** — `matchesPrevLog`, `raft/log.go:105`. This is the
   consistency check that makes the Log Matching Property inductive.
3. **"If an existing entry conflicts with a new one, delete the existing entry
   and all that follow it"** — `appendFrom`, `raft/log.go:134`.
4. **"Append any new entries not already in the log"** — same function.
5. **"If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of
   last new entry)"** — end of `AppendEntries`.

**Rules 3 and 4 are one function on purpose.** Truncating and appending must be a
single atomic step under the lock. Splitting them leaves a window where the log
is truncated but not yet extended, and a concurrent read sees a log that never
existed.

**The subtle part — `appendFrom` truncates only on a real conflict.** A duplicate
or reordered retry carrying entries the node already has must **not** truncate.
The paper is explicit that this matters, and it is easy to get wrong by
truncating unconditionally at `prevLogIndex`. Two tests exist purely to hold this
line:
- `TestAppendEntries_DuplicateDeliveryIsIdempotent` — `raft/server_test.go:176`
- `TestAppendEntries_ReorderedStaleRetryDoesNotTruncate` — `raft/server_test.go:201`

---

## 3. Figure 2 — RequestVote RPC

`Server.RequestVote` — **`raft/server.go:370`**

1. **"Reply false if term < currentTerm"** — `raft/server.go:380`.
2. **"If votedFor is null or candidateId, and candidate's log is at least as
   up-to-date as receiver's log, grant vote"** — `raft/server.go:415`:

```go
alreadyVoted := s.votedFor != nil && *s.votedFor != args.CandidateID
if alreadyVoted || !s.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
    return RequestVoteReply{Term: s.currentTerm, VoteGranted: false}
}
```

`alreadyVoted` allows re-granting to the **same** candidate. That is the paper's
"or candidateId", and it exists so a dropped reply can be safely retried — the
candidate asks again, gets the same yes, and no split-brain results.

### §5.4.1 — the up-to-date check

`isUpToDate` — **`raft/log.go:92`**

> "If the logs have last entries with different terms, then the log with the
> later term is more up-to-date. If the logs end with the same term, then
> whichever log is longer is more up-to-date."

Term first, then length. **Length alone is wrong** and dangerously plausible: a
node can have a *longer* log full of uncommitted entries from a deposed leader,
while a shorter log holds every committed entry. Comparing length first elects
the wrong node and loses committed writes.

This one function is the load-bearing element of Leader Completeness (§5.4), and
it is what makes health-weighted election safe (§9 below).

### §6 — the fourth vote condition

`RequestVote` also refuses a vote while a leader is still reaching this node,
even for a higher term — `raft/membership.go`, with `minElectionTimeout` at
`raft/membership.go:378`.

This is **not** in Figure 2. It comes from §6's discussion of disruptive servers:
a removed or partitioned node campaigns with an ever-rising term and forces a
healthy leader to step down repeatedly. Tests:
`TestVoteIsRefusedWhileALeaderIsReachingUs`, `TestVoteIsGrantedOnceTheLeaderStopsReachingUs`
— `raft/membership_test.go:432`, `:457`.

---

## 4. §5.2 — leader election and timing

| Piece | Where |
|---|---|
| The tick loop | `Server.tick` — `raft/loop.go:71` |
| Election timeout reset | `resetElectionTimerLocked` — `raft/loop.go:104` |
| Becoming a candidate | `startElection` — `raft/loop.go:129` |
| Becoming a leader | `becomeLeader` — `raft/loop.go:227` |
| Stepping down on a higher term | `observeTerm` — `raft/server.go:263` |

`observeTerm` is the single choke point for the paper's *"If RPC request or
response contains term T > currentTerm: set currentTerm = T, convert to
follower"*. Every RPC path funnels through it, which is why term-stepdown cannot
be forgotten in one branch.

The timing constraint the paper states in §5.2:

```
broadcastTime << electionTimeout << MTBF
```

**This project measured that inequality being violated for real.** Under
`-race`, detector overhead inflated `broadcastTime` enough to collide with the
election timeout, producing ~1-in-3 flaky full-suite runs *at HEAD, before any of
this work*. The fix was widening the harness timings — not touching the
algorithm. That is the inequality asserting itself, not a bug.

### `becomeLeader` and the optimistic `nextIndex`

`raft/loop.go:227`. Figure 2: reinitialise `nextIndex[i] = last log index + 1`
and `matchIndex[i] = 0` after election.

`nextIndex` starts **optimistic** (assume the follower is caught up) and
`matchIndex` **pessimistic** (assume nothing is confirmed). The gap between them
is exactly what the decrement-and-retry loop closes — `replicateTo`,
`raft/loop.go:414`.

---

## 5. §5.3 — log replication and commitment

`broadcastAppendEntries` — `raft/loop.go:280`
`replicateTo` — `raft/loop.go:308`
`advanceCommitIndexLocked` — **`raft/loop.go:432`**

The commit rule, quoted in the code at `raft/loop.go:422`:

> "If there exists an N such that N > commitIndex, a majority of matchIndex[i] >=
> N, and **log[N].term == currentTerm**, set commitIndex = N."

**That last clause is §5.4.2, and it is the least intuitive rule in the paper.**
A leader may *not* commit an entry from a previous term just because it is
replicated on a majority. Figure 8 shows the failure: such an entry can still be
overwritten by a future leader. The leader must commit an entry from its **own**
term, which carries the earlier entries with it under the Log Matching Property.

Deleting that one condition leaves every ordinary test green and loses committed
money in exactly one interleaving.

---

## 6. Figure 3 — the five safety properties

| Property | Guaranteed by | Test |
|---|---|---|
| **Election Safety** — at most one leader per term | one vote per term, `raft/server.go:415` | `TestRequestVote_OneVotePerTerm` — `server_test.go:291` |
| **Leader Append-Only** — a leader never overwrites its own log | leader path only ever appends (`Submit`, `raft/loop.go:471`) | `TestLeaderAppendOnly_StaleRPCDoesNotTruncate` — `server_test.go:494` |
| **Log Matching** — same index+term ⟹ identical prefixes | `matchesPrevLog` + `appendFrom` | `TestLogMatching_AfterConflictResolution` — `server_test.go:410`, `assertLogMatching` — `:433` |
| **Leader Completeness** — committed entries appear in all future leaders' logs | `isUpToDate` (§5.4.1) + the §5.4.2 term rule | `TestRequestVote_RejectsCandidateWithStaleLog` — `server_test.go:320` |
| **State Machine Safety** — no two nodes apply different commands at the same index | all of the above, plus in-order apply in `applyCommitted` — `raft/server.go:446` | `TestStateMachineSafety_SameIndexSameCommand` — `server_test.go:462` |

Each property has a test that **fails when the mechanism is removed** — verified
by deliberately breaking each one and confirming the right test went red. A test
that passes both with and without the code it covers is documentation, not a test.

---

## 7. §7 — log compaction and snapshots

| Piece | Where |
|---|---|
| Compaction trigger | `MaybeCompact` — `raft/snapshot.go:120` |
| Discarding the prefix | `discardThrough` — `raft/snapshot.go:197` |
| Index translation | `baseIndex` / `baseTerm` / `slot` — `raft/log.go:23`, `:32`, `:38` |
| Compacted-away probe | `compactedAway` — `raft/log.go:68` |
| Application snapshot | `Machine.Snapshot` / `RestoreSnapshot` — `shard/snapshot.go:43` / `:99` |

Once a prefix is discarded, log index ≠ slice offset. **Every** access goes
through `slot()` rather than indexing `s.log` directly. Skipping that indirection
in one place produced a negative-index panic.

**The bug worth remembering.** `sim.CountingSM` had a method `Snapshot() []string`
that collided with the `raft.Snapshotter` interface signature. The interface was
therefore never satisfied, `InstallSnapshot` was never sent, and **every
compaction test passed while testing nothing.** Fixed by renaming to
`AppliedCopy()` and adding a compile-time assertion:

```go
var _ raft.Snapshotter = (*CountingSM)(nil)
```

Go's implicit interface satisfaction means a silent near-miss is possible. The
assertion turns it into a compile error. Measured result: 275x log reduction.

---

## 8. §6 — cluster membership changes

`raft/membership.go` throughout.

| Paper concept | Where |
|---|---|
| Config as a log entry | `encodeConfig` / `decodeConfig` — `:137` / `:158` |
| Config vs command discrimination | `isConfigEntry` — `:153` |
| **Take effect when appended, not when committed** | `useConfigurationLocked` — `:283`, `adoptConfigFromLogLocked` — `:318` |
| One change at a time | `differsByOne` — `:113`, enforced in `changeConfiguration` — `:227` |
| Leader removing itself | `checkSelfRemovalLocked` — `:355`, `SteppedDownAfterRemoval` — `:340` |

**"A server adopts a configuration as soon as it appears in its log, whether or
not that entry is committed."** Waiting for commit deadlocks: the commit itself
may require the new configuration's majority.

This project uses the **single-server** change of §6, not joint consensus.
`differsByOne` enforces that; overlapping changes are refused rather than queued.

`Configuration.Majority` — `:89` — means quorum is computed from the *current*
configuration, never a cached count. Test: `TestQuorumFollowsConfigurationChanges`,
`raft/quorum_config_test.go`.

> **Known gap, deliberately open:** membership changes are implemented and tested
> but not exposed on the wire — no client-facing API drives `AddServer` /
> `RemoveServer`. Recorded in the README rather than quietly omitted.

---

## 9. Health-weighted election — this project's own extension

`raft/health_priority.go`, applied at `raft/loop.go:116`.

**Not in the paper.** Modelled on etcd's leadership priority and TiKV's leader
weights.

Raft's election has two mechanisms and only one is a guarantee:

- **Who is eligible** — `isUpToDate` (§5.4.1). A candidate missing committed
  entries is refused. **Untouched by this feature.**
- **Which eligible node wins** — arbitrary by design. Randomisation exists to
  break split votes, not to choose well.

Health replaces only the second. It biases the **timer**, never the **vote**:

```go
d += int64(float64(s.rnd.Int63n(span)) * s.NodeHealth().electionBias())
```

**Why it scales the draw instead of assigning each level its own band.** The
first version gave each level a slice of the window (normal drew 35–80%). That
cut the randomised spread from 100ms to 46ms and raised the minimum timeout —
both §5.2 violations. Narrower spread ⟹ more split votes; higher floor ⟹ slower
failover. **It broke five election tests.** Scaling preserves the full range for
a normal node, so a cluster with no health signal behaves exactly as before.

The case to watch in the UI: **a node can report HIGH health precisely because it
was partitioned** — idle and fast *because* it missed writes. Health says "prefer
it"; §5.4.1 says "never". Safety wins.

Guarded by `TestHealthDoesNotOverrideLogCompleteness` — `health_priority_test.go:72`
— confirmed load-bearing (removing `isUpToDate` makes it fail).

Health is a **machine** property but Raft state is per (machine, shard), so one
setting fans out to every group that machine hosts — `applyHealth`,
`demo/health.go:91`. A machine under CPU pressure is slow for all its shards, not
one.

---

## 10. §8 — client interaction

| Paper requirement | Where |
|---|---|
| Redirect to the leader | `LeaderID` — `raft/read.go:40`; `shard.ErrNotLeader` |
| Linearizable reads without writing to the log | `ReadIndex` — `raft/read.go:52` |
| Read under the state machine lock | `LinearizableRead` — `raft/read.go:146` |
| Duplicate request detection | `Machine.recordResult` / `Result` — `shard/machine.go:303` / `:313` |

`ReadIndex` is the §8 optimisation: record `commitIndex`, confirm leadership with
a heartbeat round, wait for `lastApplied` to reach it, then read. A stale leader
cannot serve a stale read because the heartbeat round fails.

**A serving bug worth recording, found from the UI (2026-08-30).** A withdrawal
made while two of three replicas were down returned a plain error — and then the
money left the account anyway once the machines came back. Both halves were
correct: `Submit` **appends** the entry to the surviving leader's log, replication
cannot reach a majority so it does not commit, `Propose` times out and reports an
error, and the entry is still sitting in the log. When quorum returns it
replicates and commits.

So a write during a majority outage is **not refused — it is indeterminate**, and
the entry effectively queues *in the Raft log itself*. The API called it a plain
failure, which invites the caller to reissue under a NEW idempotency key; both
entries then commit and the withdrawal applies twice.

Fixed with `shard.ErrCommitUnknown` — the single-shard analogue of `ErrInDoubt`,
raised in `Coordinator.single` when `Propose` times out **while still leader**
(`isLeader` is what separates "nothing was ever proposed" from "the entry is in
the log"). Consumers match with `errors.Is`, not `==`, because it wraps the
underlying timeout — a bare `==` falls through to `default` and reports exactly
the plain failure the case exists to prevent.

Guarded by `TestWriteDuringMajorityOutageIsIndeterminateNotRefused` and
`TestIndeterminateWriteRetriedWithSameKeyAppliesOnce` (`demo/indeterminate_test.go`),
the second asserting the money lands **once** (9500, never 9000) across outage,
replay, and retry.

**A second serving bug:** the sharded API returned `Indeterminate` when a
node was not the leader. `Indeterminate` means *the entry may or may not have
committed* — the client must not retry blindly. But "I'm not the leader" is a
clean, safe rejection. Reporting it as `Indeterminate` turns a retryable error
into an ambiguous one. Fixed with a typed `shard.ErrNotLeader` → `NotLeader`.

---

## 11. Beyond Raft — the rest of the distributed systems surface

### Consistent hashing — `shard/ring.go`

`NewRing` — `:49` (150 vnodes/shard), `hashKey` — `:72` (CRC32 over 2³²),
`Lookup` — `:125`, `Distribution` — `:147`.

Virtual nodes exist because hashing N shards to N points distributes terribly;
150 points each smooths it. `Lookup` walks clockwise to the first vnode ≥ the
key's hash. **The ring maps key → shard. It says nothing about which machines
hold that shard** — that is placement (`sim/placement.go`), a separate decision.
Conflating the two is the single most common misreading of this design.

### Two-phase commit over Raft — `shard/coordinator.go`, `shard/twopc.go`

`Transfer` — `:111`, `twoPhase` — `:162`, `decide` — `:261`,
`RecoverInDoubt` — `:320`; participant side `applyPrepare` — `shard/machine.go:106`,
`applyDecision` — `:169`.

Spanner's shape: 2PC where each participant is itself a Raft group, so a
participant crash is not a lost vote — the group re-elects and the prepared state
is still in the log. **Durability comes from `Prepare` being a committed log
entry**, not from coordinator memory.

> **Correction recorded:** `DESIGN.md` claimed `txs` was in-memory and lost on
> restart. It was wrong — `txs` is derived from the log and always was. The real
> gap was in the test harness, which never restarted a prepared participant.

**Routing bug worth remembering:** `Transfer` routed on `(From, To)`. For a
single-account op the empty side hashed to an arbitrary shard, turning **every
open/deposit/withdraw into a failing cross-shard 2PC.**

### Hybrid Logical Clocks — `hlc/`, used at `shard/machine.go:250`

HLC gives **causal** ordering across shards — if A happened-before B, HLC orders
them. It does **not** give external consistency; that needs bounded clock error
(Spanner's TrueTime) which this has no way to obtain. Claiming otherwise would be
the single easiest overclaim in the project.

### Observability — `raft/health.go`

`Server.Health` — `:85`.

**Bug worth remembering:** quorum contact was recorded on *any* reply — but a
stopped node still replies `Success=false`. A partitioned leader therefore
reported itself healthy. Unit tests passed; only the end-to-end test caught it.
Contact must mean *a successful exchange*, not *a returned struct*.

---

## 12. What the theory cost in practice

1. **§5.4.2's term restriction** looks like a needless special case until Figure 8.
2. **§5.2's inequality is physical.** The race detector broke it and the tests
   flaked — the algorithm was fine; the timings were a lie.
3. **Go's implicit interfaces let a snapshot test pass while testing nothing.**
   Compile-time assertions (`var _ Iface = (*T)(nil)`) are cheap insurance.
4. **`Indeterminate` and `NotLeader` are not interchangeable.** One is safe to
   retry; the other is not.
5. **A returned RPC struct is not evidence of contact.** Check the outcome.
6. **Extending Raft is safe only where the paper says the choice is arbitrary.**
   Health biasing the timer: fine. Health biasing the vote: loses money.
7. **Narrowing randomness is a correctness bug, not a tuning choice.** The first
   health implementation broke five tests by shrinking the election spread.
8. **Verify the process, not the source.** A stale server (PID 17420) held :8080
   through three verification attempts, serving old responses from correct code.
   It happened again on 2026-08-30 (PID 11796), caught only because a 3-node
   cluster reported five machines.
9. **"It failed" and "I cannot tell you yet" are different answers.** A write
   appended but not committed is indeterminate; calling it a failure invites a
   retry under a new key, which double-applies. The user saw this in the UI
   before any test did — and was right when the model asserted otherwise.
