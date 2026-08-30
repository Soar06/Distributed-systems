# Core Bank — a distributed core-banking ledger

A hand-built Raft consensus implementation with a sharded, double-entry banking
ledger on top. Written from the papers, not from a library: no `hashicorp/raft`,
no ORM, no framework.

**This is a learning project.** The distributed-systems layer is the point; the
bank is the domain that makes the theory concrete and testable. It is not
deployed anywhere and is not production-ready — see [Status](#status) for exactly
what is and is not done.

```
                    ┌──────────────────────────────────────┐
   bank client ───► │  leader (shard-0)   follower  follower│
                    │      │                                │
                    │      ├── AppendEntries ──────────────►│
                    │      │                                │
                    │  ┌───▼────┐  replicated log            │
                    │  │ ledger │  ◄── balances derived      │
                    │  └────────┘                            │
                    └──────────────────────────────────────┘
                         cross-shard transfer → 2PC over Raft
```

## Quick start

Requires **Go 1.27+**. No other dependencies — the module has zero third-party
imports.

```bash
go build ./...
go test -race ./...        # 117 tests
```

### Run a 3-node cluster

Each node is its own OS process with its own data directory. From three terminals:

```bash
PEERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"

go run ./node -id n1 -listen 127.0.0.1:9001 -peers "$PEERS" -data ./data
go run ./node -id n2 -listen 127.0.0.1:9002 -peers "$PEERS" -data ./data
go run ./node -id n3 -listen 127.0.0.1:9003 -peers "$PEERS" -data ./data
```

One node will log `Leader (term 1, ...)` within a few hundred milliseconds.

### Move some money

```bash
go build -o bankcli ./cmd/bankcli

# bankcli <addr> tx <op> <key> <from> <amount> <to>
./bankcli 127.0.0.1:9001 tx open o1 "" 50000 alice     # open alice with 500.00
./bankcli 127.0.0.1:9001 tx open o2 "" 30000 bob
./bankcli 127.0.0.1:9001 tx transfer t1 alice 12000 bob

# bankcli <addr> bal <account> <lin|stale>
./bankcli 127.0.0.1:9001 bal alice lin                 # linearizable read
./bankcli 127.0.0.1:9001 status                        # node role, term, balances
```

Amounts are **integer minor units** (cents). `50000` is 500.00.

Write to a follower and it will redirect you:

```
ok=false err="raft: not leader" notLeader=true leaderAddr=127.0.0.1:9001
```

### Watch a failover

Kill the leader process. Within a second a survivor logs `Leader (term 2, ...)`,
and the cluster keeps serving with balances intact. Restart the dead node and it
rejoins, replays its log from disk, and catches up.

## Node configuration

| Flag | Default | Notes |
|---|---|---|
| `-id` | — | must appear in `-peers` |
| `-listen` | — | `host:port` to bind |
| `-peers` | — | `id=host:port,...`, **including this node** |
| `-data` | `./data` | directory for per-node `<id>.wal` and `<id>.applied` |
| `-rpc-timeout` | `100ms` | must be well below `-election-min` |
| `-election-min` | `150ms` | randomized election timeout, lower bound |
| `-election-max` | `300ms` | upper bound; randomization is what breaks split votes |
| `-heartbeat` | `50ms` | leader heartbeat interval |
| `-allow-single-node` | `false` | permits a 1-node cluster (no fault tolerance) |
| `-seed` | `0` | election-timer seed; 0 derives one from the id |

Configuration is **validated at startup and the node refuses to run if it is
unsafe.** Duplicate node ids, addresses without ports, empty ids, and a
`-rpc-timeout` at or above `-election-min` are all rejected with an explanatory
error. These are not pedantry: a duplicated id silently inflates quorum size, so
a cluster reports healthy while tolerating zero failures.

The timing rule the validation enforces is from Raft §5.2:

```
broadcastTime  <<  electionTimeout  <<  MTBF
```

If an RPC can outlive the election timeout, followers start elections while the
leader is still waiting on a single call, and leadership churns under mild
network degradation.

## Client contract — read this before integrating

The reply to a write has **four** outcomes, not two. Treating it as a boolean is
how a real transfer gets double-sent or wrongly reversed.

| Field | Meaning | What the client must do |
|---|---|---|
| `OK` | Committed and applied. | Done. |
| `NotLeader` + `LeaderAddr` | This node is not the leader. | Retry at `LeaderAddr`, same key. |
| `Conflict` | The idempotency key was already used for a **different** request. | Do **not** retry unchanged. This is a bug in the caller. |
| `Indeterminate` | **Outcome unknown.** The entry may still commit. | Retry with the **same** idempotency key. |

`Indeterminate` is the one that matters. A timed-out write is *not* a failed
write — the entry is in the leader's log and may commit a moment later. A client
that records it as "did not happen" and reissues under a new key will double-send
the money.

**Idempotency keys are mandatory and are bound to their request.** A key is a
retry token for one specific `(op, from, to, amount)`. Reusing it for anything
else returns `Conflict` rather than a stale result — without that binding, a
withdrawal from one account came back `ok=true` carrying another account's
balance.

`bankcli` maps these onto exit codes, so scripts can branch on them:

| Exit | Meaning |
|---|---|
| `0` | committed |
| `1` | failed, or a usage/connection error |
| `2` | account not found (`bal`) |
| `3` | not the leader — retry at the address printed |
| `4` | **indeterminate** — retry with the same key |
| `5` | idempotency key conflict — do not retry unchanged |

Reads come in two flavours:

- **Linearizable** (`lin`) — the leader confirms with a majority before
  answering, per §8. Costs a round trip; never stale.
- **Stale** (`stale`) — served locally by any node, possibly behind. This is the
  path follower reads would build on.

## Repository layout

| Package | Contents |
|---|---|
| `raft/` | Raft from the paper: Figure 2 state and RPCs, the role loop, persistence, linearizable reads |
| `storage/` | Write-ahead log, atomic state file, applied-index marker |
| `ledger/` | Accounts, double-entry, integer money, idempotency, fund reservation |
| `shard/` | Consistent-hash ring, 2PC over Raft, coordinator and recovery |
| `rpc/` | TCP transport, client API, peer-list parsing |
| `node/` | The node binary — one process, one cluster node |
| `cmd/bankcli/` | Minimal client for driving a cluster by hand |
| `sim/` | Deterministic fault-injecting network and the chaos harness |
| `fe/` | Phase 4 UI mockups — **not wired to the backend** |

## Documentation

| Document | Purpose |
|---|---|
| [context/ARCHITECTURE.md](context/ARCHITECTURE.md) | The doc map: scope vs spec |
| [context/NOW.md](context/NOW.md) | What we are building, phase by phase |
| [context/DESIGN.md](context/DESIGN.md) | How it is specified — read this while writing code |
| [context/LATER.md](context/LATER.md) | Production-shaped evolution, deliberately deferred |
| [learn/READING_LIST.md](learn/READING_LIST.md) | Every theory topic with its primary source |
| [Agents/RULES.md](Agents/RULES.md) | Binding project rules |

The canonical Raft citation throughout is the **extended tech report**
(2014-05-20, <https://raft.github.io/raft.pdf>) — section numbers differ from the
shorter conference paper.

## Testing

```bash
go test -race ./...              # everything, 117 tests
go test -race ./sim/             # chaos: crashes, partitions, loss, duplication
go test -race -short ./...       # skips the long chaos and timing runs
```

Per [rule 3](Agents/RULES.md), a feature is not done until it is tested across
**multiple flows** — normal, failure, concurrent, and retry — against both the
paper's rules and real-world behaviour. The chaos suite asserts all five Figure 3
safety properties directly, not merely that balances look right: a balance check
says *something* broke, a Log Matching assertion says *what*.

Measured results the tests produce:

- Adding replicas does **not** add write throughput — 3 nodes 119.9 tx/s vs
  5 nodes 105.9 tx/s. Sharding is what adds capacity.
- Consistent hashing moves ~21.9% of keys when a 5th shard is added, against
  modulo's ~80%.
- Virtual nodes cut shard skew from 579.88x to 1.17x.
- With the §5.2 timing inequality deliberately violated, terms churn while
  Election Safety and Log Matching hold — timing costs liveness, never safety.

## Status

**Phase 1 complete. Phase 2 core complete. A production-hardening pass has been
run.** 117 tests pass under `-race`.

Done: leader election, log replication, durable state, crash recovery,
linearizable reads, double-entry ledger with idempotency, consistent-hash
sharding, cross-shard 2PC modelled on Spanner with coordinator crash recovery and
in-doubt resolution.

**Not production-ready.** The significant remaining gaps, in risk order:

1. **No authentication and no TLS.** Both the client and inter-node ports are
   unauthenticated plaintext. Anyone who can reach them can read every balance
   and inject `AppendEntries`.
2. **No snapshotting or log compaction.** The state file is rewritten on every
   persist — measured O(n²), 481x write amplification at 800 entries. This is a
   hard scalability wall, not a slow degradation.
3. **No multi-process sharded deployment.** `node/` runs a single Raft group;
   sharding works only in-process today.
4. **No metrics, health endpoints, or structured logging.** A cluster committing
   against a degraded quorum looks identical to a healthy one from outside.
5. **No cluster membership changes** — a failed node cannot be replaced without
   full-cluster downtime.

Phase 3 (hybrid logical clocks) and Phase 4 (the UIs) are not started; `fe/` is
still static mockups on fake data.
