---
title: Keep long-running Notion jobs in a separate compact watcher window
date: 2026-04-15
category: development-workflow
module: notion writeback
problem_type: workflow_issue
component: development_workflow
severity: low
applies_when:
  - starting a long scoring, sync, or Notion writeback run
  - the live status needs to stay readable without scrolling through mixed logs
  - the person watching the run needs a persistent glanceable view of progress
tags: [notion, watcher, compact-only, terminal, workflow]
related_components:
  - watch_progress.py
  - sync_incremental_delta.py
  - update_notion.py
---

# Keep long-running Notion jobs in a separate compact watcher window

## Context
We kept having to re-explain that long Notion syncs need a separate status window, not a mixed stream of updater logs. The user wanted a persistent view that stayed readable without manual scrolling, especially while the writeback and reconciliation jobs were still running.

## Guidance
For any long-running Notion job, keep the updater and the watcher separate:

- run the job in one terminal
- run `watch_progress.py` in a second terminal
- use `--compact-only` for the watcher when the run is long enough that full tails become noisy

Example:

```bash
python watch_progress.py --workdir out --interval 1 --tail 6 --compact-only
python sync_incremental_delta.py --workdir out --reconcile-missing
```

The watcher should stay focused on a small set of live fields:

- current phase
- processed vs remaining rows
- updated vs noop rows
- queued writes
- ETA
- duplicate summary when relevant

If live status is missing, the watcher should say so explicitly instead of pretending the final summary is current.

## Why This Matters
Long runs produce too much output for a single terminal to be useful. If the status is buried in the same stream as the worker logs, the user has to scroll to find the signal, and the UI feels unstable even when the job is healthy.

A dedicated compact watcher makes the run feel persistent:

- it keeps the current status visible at a glance
- it separates progress from noise
- it makes it obvious when the job has actually finished

This also reduces the chance that stale summary output is mistaken for live progress.

## When to Apply
- any writeback or reconciliation that can take more than a few seconds
- jobs that emit retries, batching, or progress updates
- runs where the user explicitly asks to track status continuously

## Examples
Before:

- one terminal prints updater logs and status
- the status line scrolls away
- the user cannot tell whether the job is still working

After:

- terminal 1 runs the updater
- terminal 2 runs `watch_progress.py --compact-only`
- the compact watcher stays readable and persistent

Session history note:

- a prior Notion run explicitly added the watcher as a separate terminal, but the user still had to remind us later that the status needed to stay visible and compact (session history)

## Related
- [`notion-writeback-freshness-guard-2026-04-15.md`](/Users/greg/Code/score4/docs/solutions/development-workflow/notion-writeback-freshness-guard-2026-04-15.md)
- [`README.md`](/Users/greg/Code/score4/README.md)
- [`watch_progress.py`](/Users/greg/Code/score4/watch_progress.py)
- [`sync_incremental_delta.py`](/Users/greg/Code/score4/sync_incremental_delta.py)
- [`update_notion.py`](/Users/greg/Code/score4/update_notion.py)
