# Core Bank — Distributed Systems Learning Project

**Goal:** Learn how real-life distributed systems actually work, using a core banking
ledger as the concrete domain to apply the theory to. The distributed systems layer is
the point of the project; the "bank" is deliberately just enough domain to make the
theory meaningful and demoable — not a product.

This is a living scope, split into three documents. The split is **scope vs spec**:
NOW/LATER say *what* we build and in what order; DESIGN says *how* the thing being
built is actually specified. You read NOW.md to decide what to work on next; you
read DESIGN.md while writing the Go.

- **[NOW.md](NOW.md)** — what we're actually building first: all nodes on one dev
  machine (separate processes/ports, or containers), focused purely on getting Raft
  consensus and the ledger domain correct. Optimizes for learning speed, not realism.
- **[DESIGN.md](DESIGN.md)** — the concrete architecture/design spec for whatever is
  currently being built: state machine states and transitions, RPC message shapes,
  on-disk/in-memory structures, domain model invariants. Tracks the paper (Raft
  extended tech report, Figure 2) closely enough to implement against. Scoped to the
  current phase — when a phase completes, its design either stays as the record of
  what was built or is superseded by the next phase's.
- **[LATER.md](LATER.md)** — the production-shaped evolution of the same system:
  nodes as genuinely separate machines/regions, geographic sharding, follower reads,
  autoscaling, real network latency. Nothing here changes the code's design contracts
  (nodes only ever talk via RPC) — it's the same system, deployed and extended for
  real-world shape.

Update whichever doc reflects the decision that changed. Move an item from LATER to
NOW when we actually start building it. A decision about *what/when* goes in
NOW/LATER; a decision about *how it's built* goes in DESIGN.md.
