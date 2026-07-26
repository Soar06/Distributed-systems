# Core Bank — Distributed Systems Learning Project

**Goal:** Learn how real-life distributed systems actually work, using a core banking
ledger as the concrete domain to apply the theory to. The distributed systems layer is
the point of the project; the "bank" is deliberately just enough domain to make the
theory meaningful and demoable — not a product.

This is a living scope, split into two documents:

- **[NOW.md](NOW.md)** — what we're actually building first: all nodes on one dev
  machine (separate processes/ports, or containers), focused purely on getting Raft
  consensus and the ledger domain correct. Optimizes for learning speed, not realism.
- **[LATER.md](LATER.md)** — the production-shaped evolution of the same system:
  nodes as genuinely separate machines/regions, geographic sharding, follower reads,
  autoscaling, real network latency. Nothing here changes the code's design contracts
  (nodes only ever talk via RPC) — it's the same system, deployed and extended for
  real-world shape.

Update whichever doc reflects the decision that changed. Move an item from LATER to
NOW when we actually start building it.
