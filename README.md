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
go test -race ./...        # 286 tests
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

### Run a sharded cluster

`node` runs a single Raft group. `shardnode` hosts several shard replicas per
process, multiplexed over one listener and one connection per peer:

```bash
go build -o shardnode ./cmd/shardnode

PEERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"
./shardnode -id n1 -listen 127.0.0.1:9001 -peers "$PEERS"             -shards shard-0,shard-1 -data ./data
```

With mutual TLS and client authentication (each node's certificate Common Name
must be its node id — a valid certificate for the wrong node is still an
impostor):

```bash
./shardnode ... -tls-cert n1.crt -tls-key n1.key -tls-ca ca.crt                 -client-token "$CORE_BANK_TOKEN"
```

Without those flags the node starts, but warns that both ports are
unauthenticated plaintext.

### Watch a cluster's health

Pass `-obs-listen` to expose metrics and health on a separate port:

```bash
./shardnode ... -obs-listen 127.0.0.1:9101 -log-format json

curl 127.0.0.1:9101/healthz   # is the PROCESS alive?
curl 127.0.0.1:9101/readyz    # can this node COMMIT?
curl 127.0.0.1:9101/metrics   # Prometheus text format
curl 127.0.0.1:9101/status    # JSON, for the Phase 4 dashboard
```

The two health endpoints answer different questions, and the difference is the
point. Kill enough peers to break quorum and the survivor reports:

```
$ curl -i 127.0.0.1:9101/readyz
HTTP/1.1 503 Service Unavailable
shard-0: NOT READY (leader has heard from 1 of 2 needed for quorum; cannot commit)

$ curl -i 127.0.0.1:9101/healthz
HTTP/1.1 200 OK
```

Alive, but unable to serve. Restarting it would destroy the cluster's remaining
quorum; taking it out of rotation is the correct response.

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

### Watch it work

The fastest way to see everything at once:

```bash
go run ./cmd/demo
# bank app:  http://127.0.0.1:8080/bank-app/
# dashboard: http://127.0.0.1:8080/cluster-dashboard/
```

Stop it with **Ctrl+C** in that terminal.

There is no separate frontend to start: `cmd/demo` is one process that runs the
cluster AND serves the UI files straight from `fe/`. No build step, no npm, no
second port — editing a file in `fe/` and refreshing the page is the whole loop.

#### Options

| Flag | Default | What it does |
|---|---|---|
| `-shards` | 3 | slices of the keyspace (more = more parallel writes) |
| `-nodes` | 3 | machines in the cluster |
| `-replication-factor` | 3 | machines each shard is copied onto |
| `-listen` | `127.0.0.1:8080` | address for the UI |
| `-ui` | `fe` | directory holding the UI files |
| `-seed` | 42 | seed for the simulated network (same seed = same layout) |
| `-seed-accounts` | true | open a few accounts at startup |
| `-data` | *(empty)* | directory for durable state; empty keeps everything in RAM |

A useful shape for seeing placement rather than co-location — more machines than
the replication factor, so some machines hold nothing and can fail without any
account noticing:

```bash
go run ./cmd/demo -shards 2 -nodes 5 -replication-factor 3
```

To keep your accounts across restarts:

```bash
go run ./cmd/demo -data ./demo-data
```

Every replica gets its own write-ahead log under that directory — one per
(machine, shard) pair, because a machine hosting two shards runs two independent
Raft groups and sharing one file between them would interleave two logs into one.
Kill the process, start it again with the same `-data`, and the balances are
replayed from those logs. Without `-data` the cluster is pure RAM and starts
empty every time.

`-nodes` must be at least `-replication-factor`: a shard cannot have more copies
than there are machines to put them on, and the demo refuses rather than quietly
shrinking the replication factor.

#### Two things that will waste your time if you hit them

**"The port is in use" / the UI shows the wrong cluster.** An older run is still
holding `:8080`. It serves perfectly valid *stale* responses, so this looks like
a code bug rather than a stray process — a 3-node cluster reporting five machines
is the giveaway. Find and stop it:

```bash
netstat -ano | findstr ":8080.*LISTENING"   # last column is the PID
taskkill /PID <pid> /F
```

**UI changes not taking effect.** The server now sends `Cache-Control: no-store`,
so a normal refresh is enough. If you are running an older build, hard-refresh
with **Ctrl+Shift+R**.

One process runs the cluster and serves both UIs. Open the dashboard
beside the bank app and:

- **Kill the leader** of a shard from the dashboard. Watch a survivor take over
  in a new term while the shard keeps committing — then revive it and watch it
  catch up.
- **Kill two of three** and the shard reports `NOT READY` with the reason. That
  is quorum, not a bug.
- **Open two bank-app windows on one account** and withdraw from both at once.
  The ledger serializes them: the balance never goes negative, and the loser
  gets `insufficient funds`.
- **Send twice** fires one idempotency key twice, concurrently. The money moves
  once.

State is pushed over Server-Sent Events at 100ms, which is fast enough to see a
candidate mid-election rather than only the outcome.

> The demo's control endpoints are **unauthenticated** — they kill nodes and
> move money. It is a separate binary for that reason; never run it as a
> production surface.

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
| `Busy` | Shed by backpressure or a rate limit. **Nothing was proposed.** | Wait `RetryAfter`, then retry. Unambiguously safe. |

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
| `6` | **busy** — shed by backpressure or a rate limit; nothing was proposed, so retry freely |

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
| `node/` | The node binary — one process, one cluster node (single Raft group) |
| `cmd/shardnode/` | The sharded node binary — one process hosting several shard replicas, multiplexed over one listener, with optional mutual TLS |
| `cmd/bankcli/` | Minimal client for driving a cluster by hand |
| `obs/` | Metrics, health/readiness endpoints, structured logging |
| `hlc/` | Hybrid logical clocks — cross-shard event ordering |
| `demo/` | Live cluster behind the web UI: SSE stream + fault-injection control |
| `sim/` | Deterministic fault-injecting network and the chaos harness |
| `fe/` | The two UIs: a multi-window bank app and the cluster dashboard |
| `cmd/demo/` | The demo binary — runs a cluster and serves both UIs |

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
go test -race ./...              # everything, 286 tests
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
  5 nodes 105.9 tx/s. Sharding is what adds capacity, now measured directly:
  with dedicated nodes per shard, 2 shards reach ~2.0x and 4 shards ~3.3-4.6x of
  a single shard's write throughput.
- Durability costs 25-50x in absolute throughput (fsync per persist), but
  sharding still scales through it at ~2.2-2.8x across 4 shards.
- Consistent hashing moves ~21.9% of keys when a 5th shard is added, against
  modulo's ~80%.
- Virtual nodes cut shard skew from 579.88x to 1.17x.
- With the §5.2 timing inequality deliberately violated, terms churn while
  Election Safety and Log Matching hold — timing costs liveness, never safety.

## Status

**Phase 1 complete. Phase 2 core complete. A production-hardening pass has been
run.** 286 tests pass under `-race`.

Done: leader election, log replication, durable state, crash recovery,
linearizable reads, double-entry ledger with idempotency, consistent-hash
sharding, cross-shard 2PC modelled on Spanner with coordinator crash recovery and
in-doubt resolution. 2PC state is durable: a participant that voted YES keeps
both its promise and the funds it reserved across a full-cluster restart, proven
by four restart flows rather than asserted.

**Not production-ready** — it is a learning project, not a bank. But the gaps
that were listed here are now closed:

- **Membership changes are exposed on the wire.** The `Admin` service carries
  `AddServer`, `RemoveServer` and `Configuration`, so a running cluster can be
  reconfigured from outside. It reports the three outcomes separately —
  committed, rejected (`NotLeader`, nothing appended, safe to retry), and
  *indeterminate* (appended but commit unconfirmed, so read `Configuration`
  rather than retrying). Collapsing those is how a cluster gets grown twice.
- **The demo persists.** `go run ./cmd/demo -data ./demo-data` gives every
  replica its own WAL, so balances and in-flight 2PC promises survive the
  process being killed. Without `-data` it stays in memory, which is what the
  tests and a throwaway demo want.

Everything else on the hardening list is closed too:

- **Auth and TLS** — mutual TLS between nodes with the node id bound to the
  certificate subject, plus client bearer tokens.
- **Multi-process sharded deployment**, and the throughput benchmark it unblocked:
  with dedicated nodes per shard, 4 shards reach ~3.3-4.6x a single shard's write
  throughput.
- **Snapshotting and log compaction** (§7) — persisted state drops 275x after
  compacting 400 entries, and a lagging follower is caught up by `InstallSnapshot`.
- **Metrics, health and readiness endpoints, structured logging** — `/readyz`
  returns 503 with a reason when a node cannot commit, while `/healthz` still
  returns 200.
- **Cluster membership changes** — single-server add/remove, safe by the overlap
  argument, with the cluster serving throughout.
- **Backpressure, rate limiting, and graceful shutdown** — a shed request comes
  back `Busy` rather than `Indeterminate`, so retrying is unambiguously safe.

All four phases and the full hardening sequence are complete. `fe/` now drives
a real cluster — see **Watch it work** above.
