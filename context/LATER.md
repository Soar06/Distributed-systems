# LATER — Production-Shaped Evolution

This is the same system as [NOW.md](NOW.md) — same Raft implementation, same RPC
contracts, same ledger domain — deployed and extended to look like a real-world
distributed core banking system. Nothing here requires rewriting Phase 1–4; it's
additive infrastructure and topology changes around the same core. Move an item
here into NOW.md when we actually start building it.

## Nodes as real isolated machines

In NOW.md, nodes are processes on one dev box. In a real deployment, each node is a
separate VM/container, often in a separate rack, datacenter, or region — so that a
single hardware failure, kernel panic, or power outage takes out only one node, not
all of them. That's the actual point of "isolated node": independent failure
domains, not just independent processes.

Nothing about the Raft/RPC code changes for this — only the `peers` list (real
hostnames instead of `localhost:PORT`) and the fact that `AppendEntries`/
`RequestVote` now cross real network links with real latency and real partitions
instead of simulated ones.

## Geographic sharding (why "instant response anywhere" and consensus are in tension)

A CDN gets low latency everywhere by caching read-only content at edge locations —
this works because staleness is harmless for static assets. Money is the opposite:
two people withdrawing from the same account in different cities cannot both be
told "yes" independently — there must be one agreed-upon order of operations,
which is exactly what Raft enforces, and enforcing it costs a round trip to other
replicas.

Real payment systems reconcile "fast" and "correct" globally like this:
- **Shard geographically.** A customer's account lives primarily in the Raft group
  closest to them, so their everyday transactions stay low-latency (the leader is
  nearby) — this is the real-world version of Phase 2's sharding.
- **Only cross-region transfers pay the cross-region 2PC/consensus latency cost** —
  and that's rare relative to local activity.
- **Reads that tolerate staleness get served from a nearby follower replica** —
  this is the one place the CDN pattern legitimately applies to a consensus system.
  Real systems call this "follower reads" (CockroachDB, Spanner) — e.g. "show my
  last 10 transactions" doesn't need to hit the leader.

Build target: extend Phase 2's sharding so shards are labeled by region, add a
follower-read path for read-only queries, and demonstrate the latency difference
between a local follower read and a cross-region leader write.

## Autoscaling / overload resilience

Two different real concerns, worth keeping distinct:

- **Autoscaling replica count** is an ops/infrastructure concern (Kubernetes HPA,
  load balancers), not distributed-systems theory. It's also a common
  misconception trap: adding more nodes to a Raft group does not add write
  throughput (writes still funnel through one leader) — it only adds fault
  tolerance and read capacity. Real write scaling comes from adding more shards,
  not more replicas per shard.
- **Overload/DoS resilience** (what "huge request attack" usually actually means)
  is the theory-relevant piece: backpressure (a node queues/rejects instead of
  falling over when saturated), per-client rate limiting, and load shedding at the
  leader specifically, since it's the bottleneck by construction.

Build target: add rate limiting + backpressure at the RPC layer, and a
demonstration of leader saturation under write load that more replicas don't fix
(only sharding does).

## Other production-shape items

### Done — moved out of LATER and into the system

These were on this list and have since been built. Kept here, marked, rather than
deleted: a list that only ever grows tells you nothing about progress, and a list
that quietly drops items loses the record of what was once considered future work.

- **TLS between nodes and between client/node** — mutual TLS with the node id
  bound to the certificate subject, plus client bearer tokens (`rpc/security.go`,
  READING_LIST §13). Verification alone proves only that a peer holds a
  cert signed by our CA, which every node does; binding the connection to the
  id we meant to dial is what stops one member impersonating another.
- **Snapshotting + log compaction** — `raft/snapshot.go`, `rpc/snapshot_rpc.go`,
  §7. Measured 275x log reduction.
- **Observability** — `obs/`, plus `raft.Health` for readiness vs liveness
  (READING_LIST §16). Hand-written Prometheus text format, no dependency.
- **Backpressure and load shedding** — `rpc/admission.go`, `rpc/drain.go`
  (READING_LIST §18). This was the theory-relevant half of the autoscaling item
  below.
- **Membership changes over the wire** — `rpc/admin.go`. Not originally on this
  list; it was the README's top open gap and is now closed, which also makes
  *service discovery* below a smaller job than it was.
- **Durable demo state** — `-data` on `cmd/demo` gives every replica its own WAL,
  so the UI cluster survives a restart rather than being pure RAM.

### Still open

- **Service discovery** instead of a static peer list, so nodes can join or leave
  without a config change. Now that membership is exposed over the wire
  (`rpc/admin.go`), this is the remaining half: something has to *decide* to call
  AddServer/RemoveServer, and to tell a new node who its peers are.
- **Follower reads** — the read path exists (`ReadIndex`, `LinearizableRead`) but
  every read still goes through the leader. READING_LIST §22 records the design
  decision and the condition on adding this: it must be opt-in per request with
  the weaker guarantee stated in the response, never a silent downgrade of every
  read. A stale balance is a wrong balance.
- **Region-labelled shards** — no notion of region exists anywhere in `shard/` or
  `sim/` yet. Prerequisite for the geographic sharding section above.
- ~~**Live resharding**~~ — **done**. `shard/migration.go` + `shard/reshard.go`
  move ring arcs between two running Raft groups while both keep committing:
  prepare → freeze (moving arcs only) → copy → atomic cutover → drain. Measured
  frozen window of 11ms for one account, total money conserved. `Resize` remains
  as the separate teaching control it always was. What is deliberately simplified
  and said so in the code: the copy happens inside the freeze rather than as a
  bulk pass plus a small delta, because the demo's datasets make them the same
  thing. Remaining rough edge: a drained account stays visible on the source with
  a zero balance, since the ledger has no close-account operation.
- **Autoscaling replica count** — deliberately still here and deliberately still
  ops rather than theory. Worth keeping written down because it is the field's
  most common misconception: adding replicas to a Raft group does not add write
  throughput. Writes still funnel through one leader. More replicas buy fault
  tolerance and read capacity; only more *shards* buy write throughput.
