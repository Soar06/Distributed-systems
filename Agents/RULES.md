# Project Rules

Super high-level, non-negotiable rules for this project. These apply equally to
the user and to any AI agent (Claude Code or otherwise) working in this repo.

Binding, not advisory: if either party is about to violate a rule here, that must
be flagged explicitly before proceeding — silently going along with a violation
is itself a violation of this file.

Starts empty. Rules are added only when explicitly agreed as a rule going
forward — not retroactively inferred from earlier discussion.

## Rules

1. **Every distributed-systems theory topic must be logged in `learn/`, with its
   source paper/document.** Whenever a theory topic is introduced or discussed
   (consensus, CAP, linearizability, sharding, 2PC, HLC, etc.), it must be added
   as an entry in `learn/READING_LIST.md` (or a topic file under `learn/` it
   points to) with: a quick summary of what the theory actually does/solves, and
   the primary source(s) to read for it. This builds a durable map from
   "knowledge learned" to "where it came from," for later reference — not a
   restatement of the source, a pointer to it plus the why-it-matters summary.

2. **Don't create new context files/folders unless the content is substantially
   different from what already exists — prefer modifying the existing file.**
   Before adding a new `.md` file (or new top-level folder), check whether an
   existing file already covers that context and update it in place instead.
   Splitting into a new file is only justified when the new content is different
   enough that merging it in would make the existing file confusing to read.
   Goal: keep the number of context files small and each one's scope clear, so
   information isn't scattered across near-duplicate files that can drift out of
   sync with each other.

3. **Every new function or feature must be tested across multiple flows before it
   counts as done — proven against both the paper and real-world behavior.**
   A single happy-path test does not satisfy this rule. "Multiple flows" means
   exercising the thing under the conditions it will actually face: the normal
   path, the failure paths (node down, leader killed mid-operation, network
   partition, RPC lost/delayed/duplicated/reordered), the concurrent path (two
   clients hitting the same account at once), and the retry path (same request
   delivered twice must not double-process).

   Two bars must both be met:
   - **Matches the paper** — behavior agrees with the spec it implements
     (for Raft: Figure 2's rules and Figure 3's five safety properties, per
     [context/DESIGN.md](../context/DESIGN.md)). Assert the safety property
     directly, not just the end result — a balance check says *something* broke;
     a Log Matching assertion says *what* broke.
   - **Matches the real world** — behaves the way a real distributed ledger would
     under the same conditions: no lost money, no duplicated money, no
     double-charge on retry, no answer that a real bank could not defend.

   **API-level testing is sufficient.** Tests drive the gRPC/client API directly;
   no UI test is required to satisfy this rule.

   **The verified backend is the source of truth; the UI is corrected to match
   it — never the reverse.** If the UI shows behavior the backend does not
   actually produce, that is a UI bug to fix, not a backend requirement. This
   applies specifically to the concurrent same-account case: whatever consistency
   behavior the backend is proven to provide is what the UI must display,
   including when that is less convenient than the mockups' invented behavior.
   The current `fe/` mockups run on fake balances and fake latency and carry no
   authority over backend design.

   A feature with passing happy-path tests but no failure/concurrency/retry
   coverage is **not done** and must not be described as done.
