# Store Siting App — Agent Handoff Document

## What This Is

You are picking up a **QSR store siting application** built on Databricks. The design and implementation plan are complete. Your job is to execute the plan task by task.

## Critical Files To Read First

1. **Design doc:** `docs/plans/2026-03-05-store-siting-design.md` — the approved architecture
2. **Implementation plan:** `docs/plans/2026-03-05-store-siting-implementation.md` — 22 tasks with exact code, file paths, test commands, and commit messages

## Required Skill

Use `superpowers:executing-plans` to implement the plan. This is non-negotiable — it ensures task-by-task execution with verification between steps.

## Project State

- **Repo:** `/Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/store`
- **Git:** Initialized, 2 commits (design doc + implementation plan)
- **Code written so far:** None — the repo only contains `docs/plans/`
- **No existing dependencies installed** — you'll need to set up everything from Task 1

## What You're Building

A store siting app that helps QSR chains (fast food/coffee) find optimal new store locations by analyzing **leading indicators of population growth** — not just census data (which lags by 10 years).

### Architecture

```
10 Open Data Sources → Bronze (raw) → Silver (standardized to county FIPS) → Gold (scored) → React + FastAPI App
```

### Key Decisions Already Made

| Decision | Choice |
|----------|--------|
| Data grain | US county level (5-digit FIPS code) |
| Scoring model | Weighted composite (configurable weights, not ML yet) |
| Frontend | React 18 + TypeScript + deck.gl choropleth map |
| Backend | FastAPI + databricks-sql-connector |
| Deployment | Databricks Asset Bundles (DABs) |
| Target workspace | FE demo workspace |
| Catalog | `store_siting` (Unity Catalog) with `bronze`, `silver`, `gold` schemas |

### Data Sources (10 total)

1. Census Building Permits (monthly) — **strongest leading indicator**
2. HUD Residential Construction Permits (quarterly)
3. USPS Change-of-Address migration via HUD (quarterly)
4. USPS Vacancy Data via HUD (quarterly)
5. BLS QCEW Employment (quarterly)
6. NCES School Enrollment (annual)
7. ACS 1-Year Estimates (annual)
8. Census County Business Patterns (annual)
9. County-Level SSP Projections (periodic)
10. Census National Population Projections (periodic)

### Scoring Formula

7 indicators, min-max normalized to 0-1, then weighted:
- Building permits per 1K pop: 25%
- Net migration rate: 20%
- Vacancy rate YoY change (inverted): 15%
- Employment growth rate: 15%
- School enrollment growth: 10%
- SSP projected growth: 10%
- QSR density (inverted — less competition = better): 5%

Score = weighted sum * 100. Tiers: A (80-100), B (60-79), C (40-59), D (20-39), F (0-19).

## Execution Order

Execute tasks **strictly in order** (1 through 22). Each task has dependencies on prior tasks.

| Phase | Tasks | What | Dependencies |
|-------|-------|------|-------------|
| 1 | 1-2 | Scaffolding + common utilities | None |
| 2 | 3-5 | Bronze ingestion (10 sources) | Task 2 (FIPS, config, logger) |
| 3 | 6-9 | Silver transforms | Tasks 3-5 (Bronze modules exist) |
| 4 | 10-11 | Gold scoring | Tasks 6-9 (Silver modules exist) |
| 5 | 12 | FastAPI backend | Tasks 10-11 (Gold tables defined) |
| 6 | 13-17 | React frontend | Task 12 (API endpoints exist) |
| 7 | 18-19 | GeoJSON + DABs config | Tasks 12-17 (app code exists) |
| 8 | 20-22 | Tests + README | All prior tasks |

## Per-Task Execution Protocol

For **every single task** in the implementation plan:

1. **Read the task** from `docs/plans/2026-03-05-store-siting-implementation.md`
2. **Write the failing test first** (TDD — the plan provides exact test code)
3. **Run the test** to confirm it fails for the right reason
4. **Write the implementation** (the plan provides exact code)
5. **Run the test** to confirm it passes
6. **Commit** using the exact commit message from the plan
7. **Move to the next task**

Do NOT skip tests. Do NOT batch multiple tasks into one commit. Do NOT deviate from the plan unless something is broken.

## Environment Setup Notes

- **Python 3.11+** required
- **Node.js 18+** required for React frontend
- **Databricks CLI** must be configured for DABs deployment
- PySpark runs locally for tests (`master("local[1]")`)
- The plan uses `python -m pytest` for Python tests and `npx vitest run` for frontend tests

## Databricks-Specific Instructions

- Before deploying, authenticate with Databricks: use the `fe-databricks-tools:databricks-authentication` skill
- Use `databricks bundle validate -t dev` to validate the DABs bundle before deploying
- Use `databricks bundle deploy -t dev` to deploy
- The app connects to Unity Catalog via SQL Warehouse — `warehouse_id` must be set in DABs variables

## What Success Looks Like

When you're done:
- All 22 tasks committed individually
- `python -m pytest tests/ -v` passes
- `cd src/app/frontend && npx vitest run` passes
- `databricks bundle validate -t dev` succeeds
- The React app renders a US county choropleth colored by growth score
- Clicking a county shows a detail panel with radar chart and trend sparklines
- Weight sliders allow reconfiguring the scoring formula

## Common Pitfalls

- **FIPS codes must be zero-padded to 5 digits.** Alabama counties start with "01", not "1". The `normalize_fips()` function in `src/common/fips.py` handles this.
- **GeoJSON is ~15MB.** Don't commit it to git — use the download script in `scripts/download_geojson.py` and `.gitignore` the output.
- **Silver transforms need population data for rate calculations.** ACS demographics should be ingested and transformed before other Silver transforms that compute per-capita rates.
- **The inverted indicators** (vacancy change, QSR density) need `1.0 - normalized_value` in Gold scoring. Lower vacancy change and fewer QSR competitors = higher score.
- **SQL injection in FastAPI routes.** The plan has placeholder SQL string formatting — when implementing, use parameterized queries for any user-supplied values (state filter, FIPS codes).

## Start Command

```
Read docs/plans/2026-03-05-store-siting-implementation.md, then execute Task 1 using superpowers:executing-plans.
```
