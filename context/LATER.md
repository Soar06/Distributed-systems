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

## Other production-shape items to fold in later

- Service discovery instead of a static peer list (e.g. so nodes can join/leave
  without a config change).
- TLS between nodes and between client/node.
- Snapshotting + log compaction for long-running logs (needed once the log gets
  large enough that replaying it from scratch is impractical).
- Observability: metrics/tracing per node (who's leader, replication lag, commit
  latency) — the operational analog of the demo UI in NOW.md Phase 4.
