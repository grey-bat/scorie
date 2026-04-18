# PLAN

## Current Goal
- Pipeline is working; make it easier to re-run and onboard a new session

## Now
- [ ] Add `.env.example` with all required keys documented
- [ ] Verify `scoring_rubric.md` is current (v3 — check if v4 exists)
- [ ] Test full run against current Notion database export

## Next
- [ ] Add `--dry-run` flag to `update_notion.py` (preview writes without committing)
- [ ] Add run summary output (total scored, errors, time elapsed)
- [ ] Version the scoring rubric (rename to `scoring_rubric_v3.md`, add changelog)

## Later
- [ ] Web UI dashboard for monitoring pipeline runs
- [ ] Slack webhook notification on completion
- [ ] Score drift report (compare current run vs previous)

## Blockers
- none

## Notes / Execution Decisions
- `data/` is gitignored — must re-export from Notion before each new scoring session
- `everything.csv` can be very large; `prepare_input.py` reads only `id` and `member_distance` columns
- Model default is `minimax/minimax-m2.7` via OpenRouter; override with `OPENROUTER_MODEL` env var
