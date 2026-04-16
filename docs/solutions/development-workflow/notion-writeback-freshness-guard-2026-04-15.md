---
title: Treat writeback artifacts as stale until the live source is revalidated
date: 2026-04-15
category: development-workflow
module: notion writeback
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - a sync job writes summary files that may be mistaken for current state later
  - a live external data source can change after the last run
  - a writeback depends on a uniqueness assumption such as Raw ID being one-to-one
tags: [notion, writeback, freshness, provenance, duplicate-check]
---

# Treat writeback artifacts as stale until the live source is revalidated

## Context
We treated a previous Notion writeback summary as if it described the current database state. That led to the wrong conclusion that the current database still had duplicate Raw IDs, when the evidence we actually had was only a historical run snapshot.

## Guidance
Treat writeback logs, summaries, and status files as run artifacts, not live truth. Before asserting current uniqueness or completeness, requery the live source and record the source ID, run timestamp, and query scope.

For writebacks that depend on key uniqueness:

```python
raw_duplicate_rows = duplicate_lookup_rows(raw_cache, "Raw ID")
if raw_duplicate_rows and not args.dry_run:
    pd.DataFrame(raw_duplicate_rows).to_csv(out_path / "notion_writeback_duplicates.csv", index=False)
    raise SystemExit("Duplicate Raw ID values found; refusing to write.")
```

The run should fail fast before any patch calls if the live source violates the matching assumptions.

## Why This Matters
Without a freshness check, the code can confuse old run output with the current state of the source of truth. That causes bad debugging conclusions, wasted retries, and unsafe writes against a database that may already be in a different state.

## When to Apply
- When a job syncs against Notion, Gmail, Google Drive, or any other external system that can change between runs
- When a matching key is supposed to be unique but the job only discovers duplicates during lookup
- When status files or summaries might be read later as if they were current

## Examples
Before:

- “The last run saw 581 ambiguous Raw IDs, so the current database must still have them.”

After:

- “The last run saw 581 ambiguous Raw IDs at that timestamp. Revalidate the live source before drawing a current-state conclusion.”

Operational guardrails:

- emit a duplicate-key report alongside the writeback artifacts
- label summaries as last-run snapshots unless a live status file is present
- keep the live watcher in a separate compact terminal window for long runs
- stop the write path before patching if the live source breaks uniqueness assumptions

## Related
- [`update_notion.py`](/Users/greg/Code/score4/update_notion.py)
- [`watch_progress.py`](/Users/greg/Code/score4/watch_progress.py)
- [`writeback_status.py`](/Users/greg/Code/score4/writeback_status.py)
- [`notion-watch-progress-in-separate-window-2026-04-15.md`](/Users/greg/Code/score4/docs/solutions/development-workflow/notion-watch-progress-in-separate-window-2026-04-15.md)
