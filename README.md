# Notion Scoring Pipeline v4

## What you place locally
- `full.csv`
- `data/everything.csv`

`everything.csv` can be huge. The prep step reads only `id` and `member_distance`.

## Core behavior
- dedupes `full.csv` before scoring
- primary key: `Raw ID`
- fallback key when `Raw ID` is blank: `Best Email`
- dedupe winner rule:
  1. row with `Stage` beats row without `Stage`
  2. later stage wins: `New < On Deck < Qualified < Outreached < Responded < Booked < Met < Followup`
  3. if equal stage, more recent timestamp wins
  4. if still tied, richer row wins
- `Alumni Signal` is recomputed as `Cal+CBS`, `Cal`, `CBS`, or blank
- distance mapping: `DISTANCE_1 -> 1`, `DISTANCE_2 -> 2`, blank / missing / anything else -> `3`
- score output is keyed by `Match Key`
- `update_notion.py` matches by `Raw ID` first, then `Best Email` if `Raw ID` is blank

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

## Watch live progress
```bash
python watch_progress.py --workdir out
```

## Identify and optionally archive duplicate pages in Notion
Dry run:
```bash
python notion_dedupe_cleanup.py --out notion_dupe_plan.csv
```
Apply archive:
```bash
python notion_dedupe_cleanup.py --out notion_dupe_plan.csv --apply
```

## Notes
- Notion writes are serialized and paced to reduce rate-limit pain.
- OpenRouter scoring retries each failed batch twice before stepping down concurrency, then batch size, and will step back up after a cooldown without further failures.
