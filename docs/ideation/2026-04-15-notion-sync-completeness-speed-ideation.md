---
date: 2026-04-15
topic: notion-sync-completeness-speed
focus: Notion sync completeness and speed
---

# Ideation: Notion Sync Completeness and Speed

## Codebase Context
The repo now has a working Notion writeback path with live status, compact watcher support, duplicate handling, and a reconcile mode that can scan live Notion for rows still missing the four score fields. The remaining pain point is speed: the live Notion source walk is still the bottleneck, and the current sync model can still confuse “scored in `scores_raw.csv`” with “present and complete in live Notion.”

Relevant patterns already in place:

- `build_delta.py` can produce either changed rows only or a full scored set.
- `sync_incremental_delta.py` now supports reconcile-only and full catch-up modes.
- `update_notion.py` already handles live status, no-op skipping, duplicate reporting, and bounded write concurrency.
- `watch_progress.py` already has a compact status mode for a separate terminal window.
- Existing compound notes already capture freshness and watcher-separation lessons.

The remaining opportunity is to reduce repeated live scans and make “is Notion complete?” answerable faster and more directly.

## Ranked Ideas

### 1. Build a local Notion mirror/index for reconciliation
**Description:** Materialize the live Notion source into a local indexed snapshot before reconcile/writeback, then compare against that snapshot instead of repeatedly querying the full source during every run. Use the live API to refresh the mirror and the local index to answer most matching and completeness questions.

**Rationale:** This attacks the dominant cost directly. The current reconcile path spends most of its time walking 178 Notion pages just to rediscover rows that rarely change. A local mirror would make repeated backfills, audits, and no-op checks much cheaper and less visually “stuck.”

**Downsides:** Adds cache invalidation and snapshot freshness complexity. If the mirror is stale or partial, it can produce false confidence. The implementation needs a clear refresh policy and provenance metadata.

**Confidence:** 92%

**Complexity:** High

**Status:** Unexplored

### 2. Replace row-count cursors with a stable fingerprint watermark
**Description:** Track sync progress by stable row identity plus a hash of the four score fields and relevant source metadata, not by `last_synced_rows`. Use that watermark to skip already-complete records and to resume from a true record-level checkpoint.

**Rationale:** The current row-count cursor is fragile if ordering changes or if runs are interrupted. A fingerprint watermark would make incremental sync more truthful and reduce the chance of treating old rows as still pending.

**Downsides:** Does not by itself remove the live Notion scan cost. It also adds bookkeeping around hash generation and watermark storage.

**Confidence:** 88%

**Complexity:** Medium

**Status:** Unexplored

### 3. Add a dedicated completeness audit mode
**Description:** Create a separate command that answers one question only: which live Notion rows are missing any of the four score fields? Emit a concise missing-field CSV and a live count by field, with optional auto-backfill for rows that match cleanly.

**Rationale:** This would make the current user-visible symptom directly measurable. Instead of inferring completeness from writeback logs, the repo would have a first-class audit path for “what is still blank?”

**Downsides:** Still depends on live Notion pagination, so it won’t fully solve throughput. It also introduces another mode to document and support.

**Confidence:** 85%

**Complexity:** Medium

**Status:** Unexplored

### 4. Turn duplicate/unmatched rows into a durable remediation queue
**Description:** Instead of only writing a duplicate report, create a small durable queue for ambiguous, duplicate, and unmatched rows with explicit follow-up actions. Make the main sync finish cleanly while the queue records exactly what still needs human or scripted cleanup.

**Rationale:** Right now the pipeline has a good “skip the bad rows and keep moving” behavior, but the cleanup path is still mostly a report file. A remediation queue makes exceptions feel intentional rather than incidental.

**Downsides:** Adds an operational surface area that someone has to tend. If the queue is not actively reviewed, it can become another dead-end artifact.

**Confidence:** 80%

**Complexity:** Low

**Status:** Unexplored

### 5. Add a nightly Notion coverage sentinel
**Description:** Run a scheduled zero-tolerance audit that checks whether any live Notion row is missing a required score field and alerts immediately if coverage regresses. Treat this as a health check, not a writeback.

**Rationale:** The strongest operational control may be a small recurring guardrail rather than a larger runtime refactor. This keeps completeness from drifting again after a successful catch-up.

**Downsides:** It detects regressions after the fact; it does not make the main sync faster. It also needs scheduling and alerting plumbing.

**Confidence:** 78%

**Complexity:** Low

**Status:** Unexplored

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Make the watcher even more verbose | Already addressed by the compact-only watcher and separate terminal window; more UI noise would not improve completeness or speed. |
| 2 | Add more Notion write workers | The write stage is not the main bottleneck once live scanning dominates; the repo already has bounded worker concurrency. |
| 3 | Optimize OpenRouter scoring for this problem | Scoring is not the current failure mode; the issue is live Notion reconciliation and completeness. |
| 4 | Keep using `last_synced_rows` as the main cursor | We already learned this is too fragile and can drift from real record identity. |
| 5 | Parallelize the live Notion scan aggressively | Notion pagination and rate limits make this a risky, likely low-return optimization compared with caching or mirroring. |

## Session Log
- 2026-04-15: Initial ideation — 9 candidate improvements generated, 5 survived the first pass.
