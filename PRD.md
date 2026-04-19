# PRD

## Product
- Lead scoring pipeline that rates LinkedIn profiles for B2B outreach fit
- Eliminates manual review of large contact lists by automating qualification

## User
- Primary user: Greg (solo operator)
- Pain point: Notion database of hundreds of contacts with no systematic scoring
- Context: contacts exported from LinkedIn/hiring sources, need scoring before outreach

## MVP Goal
- Score every contact in Notion across 4 dimensions using an LLM
- Write scores back to Notion so filtering and prioritisation is possible in-database

## Core Flow
1. Export contacts CSV from Notion (`full.csv`) + distance sidecar (`everything.csv`)
2. `prepare_input.py` dedupes, enriches, and normalises the CSV
3. `score_openrouter.py` sends each row to LLM with scoring rubric, gets 4 integer scores
4. `build_delta.py` maps scores back by Raw ID / Best Email fallback
5. `update_notion.py` writes scores to each Notion page via API
6. `watch_progress.py` shows live progress during scoring run

## Must Have
- 4 scores per contact: `fo_persona`, `ft_persona`, `allocator`, `access` (0–5 integers)
- Deduplication before scoring (Raw ID primary key, email fallback)
- Resume-on-crash (incremental sync state)
- Notion writeback with idempotent updates

## Nice to Have
- Web UI for monitoring runs
- Slack notification on completion
- Score drift detection across runs

## Not Now
- Automated Notion export trigger
- Multi-user support

## Product Decisions
- decision: LLM-only scoring (no rule-based fallback)
  - why: rubric-based scoring requires nuanced reading of profiles
- decision: OpenRouter as LLM gateway
  - why: model flexibility, easy key rotation
- decision: Raw ID as primary key, email as fallback
  - why: Notion rows sometimes lack Raw ID; email is reliable secondary

## Success
- All contacts in Notion have scores; top decile identified and ready for outreach
