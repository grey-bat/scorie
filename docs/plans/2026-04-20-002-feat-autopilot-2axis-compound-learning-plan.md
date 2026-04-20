---
title: "feat: 2-axis rubric experiment, compound learning, live dashboard, API resilience"
type: feat
status: done
date: 2026-04-20
---

# feat: 2-axis rubric experiment, compound learning, live dashboard, API resilience

## Overview

Extends the autopilot calibration loop so a single command can iterate rubrics under two very different FT-score shapes (5-dimension and 2-axis), share baseline scoring across re-runs, compound learning across iterations, survive transient OpenRouter `402/429/5xx` responses, and stream live status to a browser dashboard.

## Problem Frame

Before this change, each autopilot re-launch re-scored the baseline (expensive), forgot prior candidate attempts on the very next launch (no compounding), crashed on any non-2xx OpenRouter response (including the common 402 "max_tokens too high for your credit balance"), and could only be watched via a terminal-only status file.

The calibration target on the 127-row eval set had also stalled: the 5-dimension rubric kept converging on configurations that lowered FP rate but left FN rate above 20% regardless of how the rewriter redistributed caps across the five FT dimensions.

## Requirements Trace

- R1. Baseline scoring runs at most once per workdir; subsequent launches resume from the best prior iteration.
- R2. The rubric rewriter sees the full list of prior candidate attempts with their share-of-total FP/FN metrics and their semantic-diff summaries, so it never repeats a rewrite the loop already proved non-improving.
- R3. Acceptance targets (`--target-fp`, `--target-fn`) use share-of-total, not class-conditional rates, so targets compose with the observable match/FP/FN = 100% identity.
- R4. Support a 2-dimension FT rubric (`company_fit` + `role_fit`, caps summing to 100) without forking the scoring or gating code paths.
- R5. Rubric rewrite LLM calls cap `max_tokens` to a level that any topped-up OpenRouter account can fund, and transient 402/429/5xx responses retry with backoff before falling back to deterministic heuristic mutation.
- R6. A live browser dashboard on `localhost` surfaces per-iteration metrics, current status, recent scoring rows (with `Manual` GOOD/SKIP), and recent autopilot events, refreshed every ~2.5 s.

## Design Choices

### Data-driven FT dimension set

The active FT dimensions are now read from the rubric's `Direct Point Maps` block at load time into `DIRECT_POINT_HELP`. `score_openrouter.py` derives:

- LLM `required_keys` from `DIRECT_POINT_HELP.keys()`,
- scoring-CSV schema from `DIRECT_POINT_HELP.keys()`,
- display rows from `DIRECT_POINT_HELP.keys()`,

so a 2-axis rubric that defines only `company_fit` and `role_fit` point maps works without code forks. `composite_formula.direct_score` already multiplies by per-dimension caps and sums to 100 regardless of dimension count, so the 2-axis and 5-dimension cases share arithmetic.

### Compound-learning resume

At startup in `autopilot_calibrate.autopilot_calibrate`, if `autopilot_iter_00/autopilot_metrics.csv` exists the baseline scoring subprocess is skipped. The loop then scans every `autopilot_iter_NN/autopilot_metrics.csv`, reconstructs per-iteration metrics into a `prior_attempts` list (including the iteration's `rubric_semantic_diff.md`), promotes the iteration with the best `combined_error` into `best_version`, and continues from `max_existing_iter + 1`.

`prior_attempts` is threaded into `generate_rubric_candidate` as a new kwarg, included in the JSON `user_prompt` sent to the rubric-rewrite model along with `acceptance_targets` and an explicit `learning_instructions` block. Each newly-evaluated iteration appends its own metrics + semantic-diff back into `prior_attempts` so later iterations in the same run also benefit.

### Share-of-total FP / FN targets

`should_stop` now reads `fp_share` and `fn_share` from the metrics dict (falling back to class-conditional `fp_rate`/`fn_rate` for older dicts) so `--target-fp 0.20 --target-fn 0.15` means "false-positive share of all labeled rows ≤ 20%, false-negative share ≤ 15%" instead of the prior ambiguous per-class rates.

### API resilience

`generate_rubric_candidate` sets `"max_tokens": 16000` in every OpenRouter request (pro-preview defaults to 65536, which OpenRouter refuses with a 402 when account credit cannot fund that ceiling) and wraps the `requests.post` in a four-attempt retry loop for status `402`, `429`, and `5xx`. After four failures it returns the deterministic `default_direct_rubric` fallback, letting `propose_rubric_with_gate` still gate + apply `heuristic_mutate` instead of killing the run.

### Live dashboard

A new `live_dashboard.py` runs a pure-stdlib `ThreadingHTTPServer` serving `/` (single-page HTML with inline JS) and `/api/all` (JSON snapshot). The browser polls `/api/all` every 2.5 s and renders KPI chips, status key/value list, per-iteration metrics table with best-row highlighting, the last 30 scoring rows tailed from `run.log`, and the last 10 "event" lines (session starts, resumes, HTTP errors, failed batches).

## Implementation Summary

- `autopilot_calibrate.py`
  - `autopilot_calibrate()` baseline-cached resume, `prior_attempts` scan and promotion, per-iter append.
  - `generate_rubric_candidate()` compound-learning fields, `acceptance_targets`, `max_tokens`=16000, 402/429/5xx retry, `default_direct_rubric` fallback.
  - `propose_rubric_with_gate()` threads `prior_attempts` + targets through to the candidate function.
  - `should_stop()` uses `fp_share` / `fn_share`.
- `score_openrouter.py`
  - `build_system_prompt`, `score_fieldnames`, output writer, display row, and `required_keys` all driven by `DIRECT_POINT_HELP` keys.
  - Live table gains `Manual` GOOD/SKIP column via `utils.AUTOPILOT_DISPLAY_COLUMNS` addition.
  - Run-config log line prints the scoring `model=` explicitly so there is no ambiguity about which model is scoring vs. rewriting.
- `scoring_rubric_2axis.md`
  - New rubric: FT caps `company_fit` + `role_fit` = 100, every non-FO rule from the 5-dim rubric preserved but regrouped by bucket.
- `live_dashboard.py`
  - New server: `/` HTML + `/api/all` JSON, auto-refresh 2.5 s.
- `rubric_structure.py`, `reason_catalog.py`
  - `customize_not_skip` reason category removed (it was a catch-all that prevented the rewriter from targeting specific miss-modes).
- `test_autopilot_calibrate.py`
  - Fakes updated to accept the new `prior_attempts` / `target_fp_share` / `target_fn_share` kwargs.

## Result

Run A — 5-dim rubric, 10 iterations (`out/autopilot_gated_3iter`):

- Best: match 71.7%, FP share 6.7%, FN share 21.7%.
- FN never crossed below the 15% target across 10 iterations despite the compound-learning prompt seeing every prior attempt.

Run B — 2-axis rubric, 2 iterations (`out/autopilot_2axis`):

| Iter | Match | FP share | FN share | Separation |
|---|---:|---:|---:|---:|
| 00 (baseline) | 68.3% | 13.3% | 18.3% | 19.98 |
| 01 | 70.0% | 13.3% | 16.7% | 20.69 |
| **02** | **75.0%** | **14.2%** | **10.8%** | **25.19** |

Iter 02 was the first configuration in this session that satisfied both caps (`fp_share ≤ 0.20`, `fn_share ≤ 0.15`). `should_stop` ended the run early because the acceptance bar was met. A follow-up run with tighter `--target-fp 0.08 --target-fn 0.08` is continuing from iter 02 to see how far the rewriter can push match above 75%.

## How to Run

Live dashboard:

```bash
python live_dashboard.py --workdir out/autopilot_2axis --port 8765
# open http://127.0.0.1:8765/
```

2-axis autopilot, 5 fresh iterations with tight targets:

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

Relaunching against the same workdir is safe: baseline scoring is cached and the loop resumes from the best prior iteration.

## Out of Scope

- Changing the score band boundaries (still 75 / 50 / 25).
- FO score — intentionally dropped from the 2-axis experiment, not from the 5-dim rubric.
- Pushing the dashboard beyond localhost (no TLS, no auth).
