# scorie

## What It Is
- LLM-based lead scoring pipeline for B2B outreach
- Scores LinkedIn profiles across either the legacy 5-dimension rubric or the current 2-axis fintech rubric and syncs results back to Notion
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
- `OPENROUTER_SCORING_MODEL` — scoring-model slug for autopilot (default: `z-ai/glm-5.1`)
- `OPENROUTER_RUBRIC_MODEL` — rubric-rewrite model slug for autopilot (default: `google/gemini-3.1-pro-preview`)

## Autopilot Calibration
`autopilot_calibrate.py` iteratively rewrites `scoring_rubric.md` against the
manual Sent/Skip labels + `Reason` column. Each candidate must pass a
**structural gate** before being scored:

- `>= 6` material rule changes (threshold anchors modified, reason-category
  rules added/rewritten, etc.) — not prose edits.
- `>= 1` point-map cap changed within `+/-` `--weight-step` (default `6`),
  and the six caps must still sum to exactly `100`.
- `>= 1` of the top-3 FP or FN reason categories from the error dossier must
  be newly addressed or rewritten in the candidate.

If the rubric model fails the gate, it is re-prompted up to
`--rubric-max-retries` times (default `2`) with explicit feedback and a
higher temperature. After that, a deterministic heuristic mutation shifts
weights and injects reason-category templates.

Per-iteration artifacts under `<workdir>/autopilot_iter_NN/`:
- `rubric_gate.json` — gate outcome, attempts, retry temperatures, fallback flag.
- `rubric_semantic_diff.md` — structural diff (added / removed / modified rules, weight changes, reason categories newly addressed).
- `rubric_diff_from_parent.md` — unified text diff.

### Compound-learning resume

Re-launching `autopilot_calibrate.py` against an existing `--workdir` is
resumable. The cached baseline scores in `autopilot_iter_00/` are reused,
every prior `autopilot_iter_NN/autopilot_metrics.csv` is scanned, the best
iteration is promoted into `best_version`, and the loop continues from
`max_existing_iter + 1`. The rubric-rewrite prompt sees every prior
candidate's metrics and semantic diff, so it does not re-propose moves
the loop already tried. Pass `--target-fp` and `--target-fn` as
share-of-total acceptance caps; `should_stop` ends the run early as soon
as an iteration meets both.

### 2-axis experiment

`scoring_rubric_2axis.md` collapses FT scoring into two dimensions
(`company_fit` cap `70`, `role_fit` cap `30`) with every non-FO rule
from the 5-dim rubric preserved but reorganized by bucket. The
autopilot, gate, and scoring code paths are data-driven by the active
rubric's point maps, so a 2-axis rubric just works:

```bash
python autopilot_calibrate.py \
  --workdir out/autopilot_2axis \
  --manual-labels-csv data/manual_labels_128.csv \
  --iterations 7 --max-iterations 7 \
  --rubric-path scoring_rubric_2axis.md \
  --scoring-model google/gemini-3-flash-preview \
  --rubric-model google/gemini-3.1-pro-preview \
  --target-fp 0.08 --target-fn 0.08 \
  --batch-size 10 --concurrency 12
```

For production one-off rescoring runs, use the promoted rubric artifact under
`out/autopilot_2axis/rubrics/` rather than assuming the checked-in
`scoring_rubric_2axis.md` is still the best version:

```bash
.venv/bin/python score_openrouter.py \
  --input out/65_and_higher_v006/01_prepare/prepared_scoring_input.csv \
  --out out/65_and_higher_v006/02_score \
  --model google/gemini-3-flash-preview \
  --scoring-mode autopilot_direct_100 \
  --rubric-path out/autopilot_2axis/rubrics/rubric_v006_2026-04-20T17-42-46.md \
  --batch-size 10 \
  --concurrency 12
```

This produces a compact scorer output in `02_score/scores_raw.csv`; for a
human-readable merged CSV, join it back onto the deduped full input. The
canonical merged artifact from the corrected April 20 rerun is:

- `out/65_and_higher_v006/scored_65_and_higher_v006.csv`

### Live dashboard

`live_dashboard.py` serves a single-page HTML dashboard with
auto-refreshing KPIs. On autopilot runs it shows iteration metrics and
recent events; on single-pass scoring runs it falls back to `x of y`
progress from `01_prepare` / `02_score` plus the last 10 scored rows.
No external deps:

```bash
python live_dashboard.py --workdir out/autopilot_2axis --port 8765
# then open http://127.0.0.1:8765/
```

Single-pass example:

```bash
python live_dashboard.py --workdir out/65_and_higher_v006 --port 8767
# then open http://127.0.0.1:8767/
```

### One-off Notion v2 writeback

`write_2axis_v2_to_notion.py` is the scoped writer for the April 20
2-axis rerun. It creates the numeric properties if needed and writes:

- `Fintech Score v2` ← `ft_total`
- `Role Fit v2` ← `role_fit`
- `Company Fit v2` ← `company_fit`

Matching precedence is:

1. `LinkedIn Member URN`
2. `Raw ID`
3. `Best Email`

Chunked example:

```bash
.venv/bin/python write_2axis_v2_to_notion.py --offset 0 --limit 100
```

## Key Docs
- `PRD.md` — product direction
- `SPEC.md` — implementation approach
- `PLAN.md` — current work
- `scoring_rubric.md` — LLM scoring rubric (fo_persona, ft_persona, allocator, access)
