# SPEC

## Build Goal
- Python CLI pipeline: CSV in → LLM scores → Notion writeback

## Stack
- language: Python 3.11+
- LLM gateway: OpenRouter (aiohttp async)
- data: pandas for CSV processing
- notion: requests (REST API v2026-03-11)
- config: python-dotenv
- hosting: local / run manually

## Main Parts
- `prepare_input.py` — dedupes full.csv, recomputes alumni signal, maps distance, outputs prepared_scoring_input.csv
- `score_openrouter.py` — async batch scorer; reads rubric, sends rows to LLM, writes scores_raw.csv
- `build_delta.py` — joins scores_raw.csv back to full.csv by Raw ID / email fallback
- `update_notion.py` — reads delta, writes 4 score properties to each Notion page via PATCH
- `sync_incremental_delta.py` — incremental mode; only processes rows changed since last run
- `watch_progress.py` — tails run output, shows live scoring rate
- `run_pipeline.py` — orchestrates all steps in order

## Data / API
- Input: `data/full.csv` (Notion export), `data/everything.csv` (distance sidecar)
- Key columns: `Raw ID`, `Best Email`, `Stage`, `Alumni Signal`, `member_distance`
- Score output: `fo_persona`, `ft_persona`, `allocator`, `access` (int 0–5)
- Notion API: PATCH `/v1/pages/{id}` with score properties

## Constraints
- `data/` is gitignored — never commit contact data
- No background jobs; run manually per scoring session
- Model must support JSON output mode

## Technical Decisions
- decision: async aiohttp for LLM calls
  - why: parallel scoring of large batches (100+ contacts) would be too slow synchronously
- decision: Match Key as join key (normalised Raw ID or email)
  - why: Notion export IDs can have formatting inconsistencies
- decision: atomic JSON writes for incremental state
  - why: prevents corrupt state file if process is killed mid-run

## Risks / Open Questions
- risk: OpenRouter model availability / pricing changes
- open question: should scoring rubric be versioned in the repo?

## Verify
- `python run_pipeline.py --full data/full.csv --distance-csv data/everything.csv` completes without error
- Notion pages show updated score properties after `update_notion.py`
- Re-running is idempotent (no duplicate writes)
