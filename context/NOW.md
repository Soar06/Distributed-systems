# NOW — What We're Actually Building First

Everything here runs on one dev machine. "Nodes" are separate OS processes
(different ports) or separate Docker containers — logically isolated (own memory,
own log/state, communicate only via RPC) but physically on one box. This is
deliberate: it lets us iterate on consensus correctness fast, without the friction
of provisioning real infrastructure. The code we write here does not change when we
later deploy it onto real separate machines (see LATER.md) — same binary, same
protocol, just a different `peers` list and real network latency instead of
localhost.

## Stack decisions

- **Language:** Go — goroutines/channels map closely onto the message-passing model
  used in consensus papers/pseudocode; canonical real systems to read alongside this
  (etcd, CockroachDB, Consul) are also Go.
- **Transport:** gRPC + protobuf for node-to-node and client-to-node RPC.
- **Consensus:** Raft, hand-built from the paper (no hashicorp/raft library) — the
  whole point is to implement leader election / log replication / safety ourselves.
- **Cross-shard transactions:** Two-Phase Commit (2PC) — the mechanism real
  distributed ledgers use for atomic money movement across partitions. Sagas are
  noted as the eventual-consistency alternative but are not the primary path, since
  2PC is "the real deal" for money.

## What each node is

A node = one Go process. It:
- Knows its peers via a static list at startup (host:port), no discovery service yet.
- Runs its own Raft role loop (`Follower` / `Candidate` / `Leader`), independently
  of every other node.
- Keeps its own local log and its own local ledger state, derived by applying that
  log in order.
- Talks to peers only through two RPCs: `RequestVote` (leader election) and
  `AppendEntries` (log replication + heartbeat). No shared memory or disk between
  nodes, even though they're on the same machine — this discipline is what makes
  the code portable to real separate machines later.

Clients send requests to whichever node they think is leader; a non-leader
rejects and redirects. All writes go through the single current leader — this is
a real, important Raft property to internalize now: **adding nodes adds fault
tolerance, not write throughput.** Write scaling comes from sharding (Phase 2),
not from adding replicas to one Raft group.

## Phased scope

### Phase 1 — Single replicated ledger (consensus core)
- One Raft group, 3–5 node-processes, one ledger.
- Leader election, log replication, commit index, linearizable reads.
- Chaos testing: kill the leader mid-transfer, partition the network (simulated),
  verify no lost or duplicated money.
- Explicit demo goal: show that adding a 4th/5th node does NOT increase write
  throughput, only survivability — a common real-world misconception to disprove
  hands-on.

### Phase 2 — Sharding + cross-shard transfers
- Split accounts across multiple Raft groups (hash or range partitioning by account
  ID — same idea as CockroachDB/Spanner/Vitess), still all on the dev machine.
- Cross-shard transfers coordinated via 2PC.
- Hard real-world problems live here: coordinator crash recovery, in-doubt
  transactions, retries that must not double-charge.

### Phase 3 — Things real banks obsess over that tutorials usually skip
- Idempotency keys on every transfer request (networks retry; must not double-process).
- Double-entry bookkeeping enforced at the domain layer (every transfer = matched
  debit + credit, never a bare balance mutation).
- Event-sourced ledger: append-only immutable transaction log, balances derived from
  it (also doubles as the replication log).
- Cross-shard event ordering via Hybrid Logical Clocks (HLC) instead of relying on
  wall-clock time — same idea Spanner/CockroachDB use.

### Phase 4 — Demo UI

Two separate UIs, built last, living under `fe/`. Bank App is scoped now;
Cluster Dashboard to be scoped later once Bank App is settled.

**`fe/bank-app` — simulated bank app**
- Simple end-user bank app: deposit, withdraw, view current balance for an account.
- Must support opening **multiple small independent windows** at once, each acting
  as its own separate app instance/session — same account or different accounts,
  chosen per window.
- Purpose: let multiple windows fire requests concurrently against the cluster to
  make "multiple clients hitting the system at once" a real, visible thing instead
  of a description.
- Specifically supports pointing two-or-more windows at the **same account** and
  triggering operations that affect each other at ~the same time (e.g. concurrent
  withdrawals) — this is the intended way to observe, hands-on, whatever
  consistency guarantee (or violation) the backend actually provides. Whether that
  demonstrates correct CAP-theorem-consistent behavior or a bug to be fixed is a
  backend design/behavior decision, not a UI decision — **deliberately left open**
  until the Raft/consensus core (Phase 1) is understood well enough to decide it
  intentionally rather than by guesswork.
- Backend wiring and concurrency-handling logic for the bank app (how requests are
  routed, serialized, or reconciled) is also deliberately deferred for the same
  reason.

**`fe/cluster-dashboard` — cluster/ops dashboard**
- Visualizes the cluster backing the bank app, live, so the two UIs are meant to
  be watched side by side: actions in the bank app should be visibly reflected in
  the dashboard as they happen.
- **Hash ring view**: which node/shard owns which account, visualized as a ring —
  this becomes meaningful once Phase 2 sharding exists (multiple Raft groups);
  before that, with a single Raft group, this view degenerates to "one ring, one
  group," which is fine and itself illustrates why sharding is what introduces the
  ring in the first place.
- **Per-node state**: for every node, show its current Raft role (Leader /
  Follower / Candidate), and the data/log it currently holds (so a follower that's
  lagging or has just recovered is visibly different from an up-to-date one).
- **Per-node status during in-flight requests**: when a request is being
  processed, show whether each node is pending/waiting/committed for that specific
  request — this is the concrete way to *see* Raft's replication happening rather
  than just trusting it works.
- **Collision visualization**: when two bank-app windows hit the same account at
  ~the same time, the dashboard should show, node by node, how each one is
  handling that collision in real time (e.g. which request the leader ordered
  first, which node(s) are still catching up, what each node's view of the
  account currently is) — this is the direct visual proof of whatever consistency
  behavior the backend provides once that logic is decided (see bank-app note
  above).
- Exact layout/tech (e.g. graph library for the ring) not yet decided — revisit
  once Phase 1/2 backend state is queryable. Live-update transport is decided
  below.

## Frontend stack decision

- **No FE framework** (no React/Vue/etc, no npm, no build step). The user is not
  investing in learning frontend as a separate skill — all learning effort stays
  on Go/distributed systems. Plain HTML + vanilla JS is fully sufficient for the
  scope here (a bank-app window in `fe/bank-app`, a handful of node cards/ring/
  collision view in `fe/cluster-dashboard`) — a framework's main benefit is
  managing state across *many* components, which this project doesn't have.
- **Real-time updates via WebSocket**, not polling. Backend pushes a message the
  instant something changes (e.g. a node commits a log entry, a role flips,
  a balance changes); the browser's `ws.onmessage` updates the relevant DOM
  element directly (e.g. flip a node card's color/text, update a balance number).
  This is what makes the cluster dashboard's "see a node pending/waiting/committed
  live" and the bank app's "see balance update instantly" actually work — no
  framework required for this, it's plain JS reacting to pushed messages.
- Go-side: `nhooyr.io/websocket` or `gorilla/websocket` for the WebSocket server;
  plain `net/http` to serve the static HTML/JS files.
- Served locally by the Go backend itself (no separate frontend dev server) —
  keeps the whole system to one thing to run per node/service.

## Suggested repo layout (subject to change once we start)

```
core-bank/
  node/                 # Go binary — one process = one cluster node
  raft/                 # hand-built Raft implementation
  storage/              # WAL + snapshotting
  ledger/               # bank domain: accounts, double-entry transactions, idempotency
  shard/                # partitioning + 2PC coordinator (phase 2)
  rpc/                  # gRPC node-to-node + client API (protobuf defs)
  sim/                  # deterministic network simulator for chaos testing
  fe/
    bank-app/           # UI: simulated bank app (deposit/withdraw/balance, multi-window)
    cluster-dashboard/  # UI: cluster/ops dashboard (nodes, hash ring, collisions)
```

## Status

**Phase 1 complete. Phase 2 core protocol complete** (2026-08-29). 101 tests under
`-race`; Go 1.27. Production-hardening pass in progress.

- [x] **Phase 1** — consensus core, persistence, ledger, real separate processes over
      TCP, linearizable reads, chaos harness with all five Figure 3 safety properties.
      Demo goal measured: 3 nodes = 119.9 tx/s vs 5 nodes = 105.9 tx/s, so more
      replicas did **not** add write throughput.
- [x] **Phase 2** — consistent-hash sharding (adding a 5th shard moved 21.9% of keys
      vs modulo's ~80%), independent Raft groups, and cross-shard 2PC in Spanner's
      shape: prepare/decision/outcome all replicated log entries, coordinator crash
      recovery, in-doubt resolution, fund reservation. Money conservation asserted
      after every transfer.
- [ ] **Phase 2 remainder** — multi-process sharded deployment; the sharded throughput
      benchmark (not measurable in-process, see [DESIGN.md](DESIGN.md)); persistence
      for 2PC state (a prepared participant currently forgets its promise on restart).
- [ ] **Production hardening** — findings from a multi-agent review of raft/storage,
      ledger/shard, and rpc/node are being worked through. Tracked in DESIGN.md.
- [ ] **Phase 3** — HLC. Idempotency, double-entry, and event sourcing were built
      early, during Phases 1-2.
- [ ] **Phase 4 UIs** — `fe/` remains static mockups on fake data. The backend now
      exposes what they need (`Bank.Status` per node), and the concurrent
      same-account question is settled and proven: the ledger serializes withdrawals
      through the log, so exactly the available funds can be withdrawn and the
      balance never goes negative.
