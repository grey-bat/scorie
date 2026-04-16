# Notion Scoring Pipeline v4

## What you place locally

* <span data-proof="authored" data-by="human:Greg Bateman">`data/`</span>`full.csv`

* `data/everything.csv`

`everything.csv` can be huge. The prep step reads only `id` and `member_distance`.

## Core behavior

* dedupes `full.csv` before scoring

* primary key: `Raw ID`

* fallback key when `Raw ID` is blank: `Best Email`

* dedupe winner rule:

  1. row with `Stage` beats row without `Stage`
  2. later stage wins: `New < On Deck < Qualified < Outreached < Responded < Booked < Met < Followup`
  3. if equal stage, more recent timestamp wins
  4. if still tied, richer row wins

* `Alumni Signal` is recomputed as `Cal+CBS`, `Cal`, `CBS`, or blank

* distance mapping: `DISTANCE_1 -> 1`, `DISTANCE_2 -> 2`, blank / missing / anything else -> `3`

* score output is keyed by `Match Key`

* `update_notion.py` matches by `Raw ID` first, then `Best Email` if `Raw ID` is blank

## Run scoring

```bash
python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv --workdir out
```

Fast preset:

```bash
python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv --workdir out --fast
```

Aggressive preset:

```bash
python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv --workdir out --aggressive
```

Incremental Notion sync while scoring:

```bash
python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv --workdir out --fast --sync-notion
```

## Dry-run Notion writeback subset

```bash
python test_notion_writeback.py --workdir out --limit 25
```

## Real Notion writeback subset

```bash
python test_notion_writeback.py --workdir out --limit 25 --write
```

## Sync new scored rows to Notion while scoring continues

```bash
python sync_incremental_delta.py --workdir out
```

Reconcile only rows that are still incomplete in live Notion:

```bash
python sync_incremental_delta.py --workdir out --reconcile-missing
```

One-time full catch-up across every scored row:

```bash
python sync_incremental_delta.py --workdir out --catch-up
```

## Watch live progress

```bash
python watch_progress.py --workdir out
```

For long runs, keep the watcher in a separate terminal window and use `--compact-only` so the status stays readable without scrolling through mixed logs.

Live Notion writeback progress is written to `04_notion/notion_writeback_status.json` for full pipeline runs and `04_notion_sync/notion_writeback_status.json` for incremental sync runs. The watcher shows the current phase, source-scan progress, total rows needing update, remaining rows, retries, queued writes, and ETA when enough rows have completed.
When the writeback sees duplicate lookup keys, it writes `notion_writeback_duplicates.csv`, skips only the duplicate rows, and finishes the rest of the run.
Large writebacks can overlap page updates with bounded worker concurrency. Set `NOTION_WRITE_WORKERS` to tune it, or pass `--write-workers` directly to `update_notion.py`.

## Identify and optionally archive duplicate pages in Notion

Live scan dry run:

```bash
python notion_dedupe_cleanup.py --out notion_dupe_plan.csv
```

Apply archive:

```bash
python notion_dedupe_cleanup.py --out notion_dupe_plan.csv --apply
```

Export fallback against `full.csv`:

```bash
python notion_dedupe_cleanup.py --source export --full data/full.csv --out notion_dupe_plan.csv
```

## Notes

* Notion writes are paced and can use bounded worker concurrency on large runs.

* Writeback now isolates duplicate Raw ID / Best Email rows into a report and skips only those rows instead of stopping the whole run.

* Notion dedupe cleanup now scans live Notion by default and only uses `full.csv` when you explicitly ask for export mode.

* `sync_incremental_delta.py` now has a live reconciliation mode for incomplete Notion rows and a full catch-up mode that can backfill every scored row.

* OpenRouter scoring retries each failed batch twice before stepping down concurrency, then batch size, and will step back up after a cooldown without further failures.