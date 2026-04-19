# scorie

## What It Is
- LLM-based lead scoring pipeline for B2B outreach
- Scores LinkedIn profiles across 5 dimensions and syncs results back to Notion
- Supports selective company-data backfill for close leads using Linked Helper exports

## Status
- Active — production pipeline, tested against real Notion database
- Scoring works end-to-end; incremental sync stable
- Known gap: no `.env.example` — must set env vars manually

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # edit with your keys
python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv
```

## Commands
- `run_pipeline.py` — full pipeline orchestrator
- `prepare_input.py` — clean and enrich input CSV
- `score_openrouter.py` — send batches to LLM via OpenRouter
- `build_delta.py` — map scores back to full CSV by Raw ID
- `update_notion.py` — write scores to Notion pages
- `watch_progress.py` — live progress monitor
- `sync_incremental_delta.py` — incremental sync mode
- `run_pipeline.py` now emits `05_backfill/company_backfill_candidates.csv` for rows with `weighted_score >= 50` that still need company data

## Env Vars
- `OPENROUTER_API_KEY` — OpenRouter API key for LLM scoring
- `NOTION_API_KEY` — Notion integration token
- `NOTION_DATABASE_ID` — target Notion database ID
- `NOTION_DATA_SOURCE_ID` — (optional) data source for incremental sync
- `OPENROUTER_MODEL` — model slug (default: `minimax/minimax-m2.7`)

## Key Docs
- `PRD.md` — product direction
- `SPEC.md` — implementation approach
- `PLAN.md` — current work
- `scoring_rubric.md` — LLM scoring rubric (fo_persona, ft_persona, allocator, access)
