---
title: Compound-learning resume and 2-axis rubric unlock the FN target
date: 2026-04-20
last_updated: 2026-04-20
category: autopilot-calibration
module: autopilot_calibrate
problem_type: optimization_strategy
component: rubric_rewrite_loop
severity: high
applies_when:
  - an iterative LLM-rewrite loop stalls inside a local optimum
  - each re-launch re-scores a cached baseline and wastes credits
  - the rewriter keeps proposing rewrites the loop already rejected
  - OpenRouter returns 402 because the request's max_tokens exceeds funded ceiling
tags: [autopilot, rubric, compound-learning, 2-axis, openrouter, 402, max-tokens]
---

# Compound-learning resume and 2-axis rubric unlock the FN target

## Context

The 5-dimension rubric autopilot had stalled: match rate plateaued near 71.7% and false-negative share stuck at 21.7% across 10 iterations, even though false-positive share was already well under the 20% cap. Each re-launch also wasted a full baseline scoring pass (120 rows × scoring-model credits) and the rewriter had no memory of prior candidates, so it kept proposing variations of moves the loop had already proven non-improving.

The same calibration also created a downstream workflow trap: a later 680-row production rerun of `65 and higher.csv` was first scored against the checked-in `scoring_rubric_2axis.md` instead of the promoted artifact `out/autopilot_2axis/rubrics/rubric_v006_2026-04-20T17-42-46.md`. That rerun had to be rescored with `google/gemini-3-flash-preview`, then rewritten into Notion v2 fields through a chunked URN-first writer. The session history shows the team explicitly treated the checked-in file as the experiment baseline and the promoted rubric as the operational source of truth (session history).

## Guidance

Three independent changes were needed before the loop could escape the plateau:

1. **Cache the baseline and resume from best.** If `autopilot_iter_00/autopilot_metrics.csv` exists, skip the baseline scoring subprocess. Scan every `autopilot_iter_NN/autopilot_metrics.csv`, promote the best `combined_error` iteration into `best_version`, and continue from `max_existing_iter + 1`. This turns "re-launch" into a resumable checkpoint, not a restart.

2. **Feed the rewriter a history of prior attempts.** Every candidate's metrics (`match_rate`, `fp_share`, `fn_share`, `combined_error`, `separation`) and its `rubric_semantic_diff.md` are passed into the rewrite prompt as `prior_attempts`, alongside an explicit `learning_instructions` block telling the LLM to treat the best prior attempt as a floor and avoid repeating any move another attempt already tried without improvement. Compound learning is real only when the prompt sees what has been tried and what happened.

3. **Change the optimization surface when iteration within a surface stops helping.** The 5-dimension rubric forced the rewriter to balance five caps summing to 100 with fixed per-dimension anchors. The 2-axis rubric collapses all non-FO signal into `company_fit` and `role_fit` caps that also sum to 100. Every constraint the 5-dim rubric expressed is preserved, just bucketed differently. The 2-axis rubric hit both the FP and FN caps at iteration 2 and raised match from 68.3% baseline to 75.0%, a configuration the 5-dim loop could not reach in 10 iterations.

4. **Treat promoted rubric artifacts as production inputs.** Once the autopilot promotes a winner into `out/autopilot_2axis/rubrics/`, one-off reruns should point `score_openrouter.py` at that promoted file, not at `scoring_rubric_2axis.md`. The calibrated artifact is the thing that hit the target; the checked-in file is only the baseline experiment.

5. **Use chunked, URN-first Notion writeback for reruns.** The corrected rerun wrote `ft_total`, `role_fit`, and `company_fit` into `Fintech Score v2`, `Role Fit v2`, and `Company Fit v2` through `write_2axis_v2_to_notion.py`, with matching precedence `LinkedIn Member URN` → `Raw ID` → `Best Email`. In the April 20 rerun, URN resolved all 680 rows cleanly, so `Raw ID` and `Best Email` remained true fallbacks instead of primary join keys.

6. **Make the live viewer degrade cleanly for single-pass runs.** `live_dashboard.py` now falls back to `x of y` progress from `01_prepare` and `02_score`, plus the last 10 scored rows refreshed every 2 seconds. That avoids the previous blank browser state when the workdir does not contain `autopilot_status.json`.

## Why This Matters

A rewrite loop can only learn if it sees what it already tried. A loop stuck in a local optimum of a constrained search surface is usually answered by changing the surface, not by running more iterations of the same rewrite on the same surface.

Re-scoring a cached baseline on every launch also silently caps how many iterations a session can afford. Skipping it frees the credit budget for the iterations that actually move the needle.

For downstream operational scoring, the promoted rubric artifact matters just as much as the calibration logic. A one-off production rerun should use the promoted file under `out/autopilot_2axis/rubrics/` (for example `rubric_v006_2026-04-20T17-42-46.md`), not blindly fall back to the checked-in `scoring_rubric_2axis.md`. The checked-in file is the experiment baseline; the promoted artifact is the calibrated output that actually hit the target.

The Notion side has the same lesson. A broad or opaque sync makes it too easy to lose track of what key actually matched or whether the rerun is rewriting stale values. The session-history trail shows that the successful path was the narrow one: create versioned v2 properties, match by `LinkedIn Member URN` first, and push in visible chunks (session history).

## When to Apply

- Match rate stops improving for several iterations in a row.
- One error class (FP or FN) is already comfortably under target and the other is not, and the rewriter keeps trading points between dimensions without crossing the target for the stuck class.
- Re-running the loop feels wasteful because you are rescoring data you already scored under the same baseline rubric.
- A one-off production rerun needs to use the calibrated rubric winner, not the repository baseline.
- A corrected rerun needs to overwrite live Notion fields without relying on a full-source scan.
- A long-running single-pass scoring job needs browser-visible progress rather than an autopilot-only status page.

## When Not to Apply

- The rubric surface actually has room the rewriter has not yet explored (e.g., it is still making large material rule changes and metrics are still moving). Restructuring the surface prematurely throws away learned calibration.
- The eval set has changed between launches (different labels or different rows). Cached baselines stop being comparable and must be invalidated.

## Related Lessons

- Cap LLM `max_tokens` to what your credit balance funds. OpenRouter returns 402 with a message like `This request requires more credits, or fewer max_tokens. You requested up to 65536 tokens, but can only afford 48606.` Pro-preview defaults to 65536 which many top-ups cannot fund; a 16000 cap is ample for a rubric rewrite and survives any reasonable balance.
- Wrap LLM calls in a `402/429/5xx` retry with short backoff and a deterministic fallback so transient credit blips never take down a 10-iteration autopilot run.
- Emit the scoring model name explicitly in the run-config log line so "is it calling the right model?" never becomes ambiguous during a session with multiple models in play.
- When syncing one-off reruns back to Notion, add new versioned properties (`Fintech Score v2`, `Role Fit v2`, `Company Fit v2`) and match rows by `LinkedIn Member URN` first. In the April 20 rerun, URN resolved all 680 rows cleanly, making `Raw ID` and `Best Email` true fallbacks rather than the primary join.
- What did not work: using the checked-in `scoring_rubric_2axis.md` for a production rerun, and treating the Notion sync as one opaque bulk step rather than a chunked URN-first overwrite. Those two mistakes were exactly what forced the rerun correction (session history).
- GitHub issue search for this incident was skipped because `gh issue list` could not reach `api.github.com`; do not infer that “no issue exists” from that result.
- Related docs with lower-signal overlap: `docs/solutions/development-workflow/notion-writeback-freshness-guard-2026-04-15.md` for stale/live-source cautions, and `docs/solutions/development-workflow/notion-watch-progress-in-separate-window-2026-04-15.md` for watcher ergonomics.
