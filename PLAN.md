# PLAN

## Current Goal
- Address code review findings; prioritise correctness issues before usability

## Code Review Findings (per CODE.md)

### 🔴 Bugs / Correctness

- [ ] **`build_delta.py` line 59 — wrong change detection for Degree**
  - `row.get("Distance")` should be `row.get("Degree")` — `Distance` column never exists in `merged`; this means Degree changes are never detected and Notion is never updated when only Degree changes
  - Fix: change `row.get("Distance")` → `row.get(RAW_SCORE_COLUMNS.get("Degree", "Degree"), row["Degree"])` or compare `row.get("Degree")` to the prepared value directly

- [ ] **`build_delta.py` line 60 — Alumni Signal change detection always no-op**
  - `row.get("Alumni Signal")` vs `row.get("Alumni Signal_prepared")` — the `_prepared` suffix column only exists if pandas suffixed it during merge; should use explicit `merged["Alumni Signal_prepared"]` without `.get()` to surface KeyError if schema changes

- [ ] **`update_notion.py` line 91 — rate-limited error re-raises instead of retrying**
  - `_handle()` raises `RuntimeError("rate_limited")` on 429 but `_request()` only catches `requests.Timeout` and `requests.RequestException` — `RuntimeError` propagates out of the retry loop uncaught, meaning a 429 on any non-`query` call terminates the write job
  - Fix: catch `RuntimeError` in `_request()` and retry on `retryable_notion_error()`

### 🟡 Code Quality (CODE.md violations)

- [ ] **`score_openrouter.py` — `main()` is 260+ lines (CODE.md: max ~50 lines per function)**
  - The async orchestration loop (lines 331–519) should be extracted into `run_scoring_session(records, args, session)` — separate concerns: setup, wave loop, flush, recovery
  - `flush_batch` and `process_batch` are nested inside `main()` — extract to module-level functions with explicit parameters instead of closures over `nonlocal`

- [ ] **`update_notion.py` — `main()` is 280+ lines (CODE.md: max ~50 lines)**
  - Extract: `load_delta()`, `build_write_jobs(delta, all_pages, ...)`, `execute_write_jobs(write_jobs, client, ...)` — each is a clear stage with explicit inputs/outputs

- [ ] **`prepare_input.py` — `main()` is 130 lines (CODE.md: max ~50 lines)**
  - Deduplication logic (lines 69–93) should be `deduplicate_by_match_key(keyed: pd.DataFrame) -> pd.DataFrame`
  - Already partially extracted in `utils.py` (`STAGE_RANK`, `RICHNESS_FIELDS`) but the vectorized logic in `prepare_input.py` duplicates constants already in `utils.py`

- [ ] **`prepare_input.py` lines 69–73 — duplicates `utils.py` constants**
  - `stage_map` dict and `richness_fields` list are re-defined inline; `utils.py` already has `STAGE_RANK` and `RICHNESS_FIELDS` — use those directly

- [ ] **`score_openrouter.py` `deterministic_mock()` — 35-line function with nested `if/any()` chains and magic strings (CODE.md: no magic strings, max ~50 lines)**
  - Keyword lists (`family office`, `fintech`, etc.) should be named constants at module level, not inline anonymous lists

- [ ] **`utils.py` `notion_plain_text()` — 54-line if/elif chain (CODE.md: max ~50 lines)**
  - Extract to a `_NOTION_PLAIN_TEXT_EXTRACTORS` dispatch dict mapping `ptype → callable`

### 🟢 Usability / Ops (PLAN items)

- [ ] Add `.env.example` with all required keys documented
- [ ] Verify `scoring_rubric.md` is current — confirm no v4 exists
- [ ] Add `data/README.md` documenting expected columns in `full.csv` and `everything.csv`

## Next
- [ ] Score drift report: compare current `scores_raw.csv` against previous run
- [ ] Version the scoring rubric (`scoring_rubric_v3.md` + changelog)
- [ ] Slack/webhook notification on pipeline completion

## Later
- [ ] Web UI dashboard for monitoring runs
- [ ] Auto-export from Notion before pipeline (avoid manual re-export step)

## Blockers
- none

## Notes / Execution Decisions
- `data/` is gitignored — must re-export from Notion before each new scoring session
- `everything.csv` can be very large; `prepare_input.py` reads only `id` and `member_distance` columns
- Model default is `minimax/minimax-m2.7` via OpenRouter; override with `OPENROUTER_MODEL` env var
- `--dry-run` flag already exists in `update_notion.py` (line 307) — remove the stale Next item
