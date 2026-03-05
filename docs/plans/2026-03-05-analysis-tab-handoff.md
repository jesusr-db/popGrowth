# Analysis Tab — Agent Handoff Document

## What This Is

You are picking up a **fully working** QSR store siting application deployed on Databricks. Your job is to implement 11 tasks that add an Analysis tab and fix two bugs. The app is live and working — you are adding features to it.

## Critical Files To Read First

1. **Implementation plan:** `docs/plans/2026-03-05-analysis-tab-implementation.md` — 11 tasks with exact code, file paths, and commit messages
2. **Design doc:** `docs/plans/2026-03-05-analysis-tab-design.md` — the approved design

## Required Skill

Use `superpowers:executing-plans` to implement the plan. Read the plan from `docs/plans/2026-03-05-analysis-tab-implementation.md` and execute tasks in order.

## Current Project State

- **Repo:** `/Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/store`
- **App URL:** `https://store-siting-1351565862180944.aws.databricksapps.com/`
- **Databricks profile:** `DEFAULT` (host: `fe-vm-vdm-classic-rikfy0.cloud.databricks.com`)
- **Catalog:** `vdm_classic_rikfy0_catalog` (schemas: `bronze`, `silver`, `gold`)
- **SQL Warehouse ID:** `5067b513037fbf07`
- **Gold scoring job ID:** `196681278666944`
- **App name:** `store-siting`
- **Git:** On `main` branch, all prior work committed

### What Is Already Working

- Full Medallion pipeline (Bronze → Silver → Gold) with 8 data sources ingested
- 3,209 US counties scored with composite growth scores
- Percentile-based tier assignment (A=top 10%, B=top 30%, C=top 60%, D=top 85%, F=bottom 15%)
- Adaptive weighting (missing data redistributes weight instead of penalizing)
- SSP2 population projections at state level, joined to counties via state FIPS prefix
- React + FastAPI Databricks App deployed and running:
  - Choropleth map (deck.gl MapboxOverlay + MapLibre GL)
  - County detail panel with metrics, radar chart, bar chart, projection card
  - State/tier/score filters on the map
  - Weight tuner panel
- DABs bundle with 12 job definitions + app resource, validates cleanly

### What Is Broken (Bugs to Fix — Tasks 1-3)

1. **QSR White Space indicator always scores 0.** The `silver_business_patterns` table has no usable `qsr_establishments` data. Fix: remove QSR from scoring weights and redistribute its 5% to SSP projections (25/20/15/15/10/15 instead of 25/20/15/15/10/10/5).

2. **Component scores show 0 for missing indicators.** The `component_scores` struct uses `coalesce(normalized, lit(0.0))` which writes 0 for NULL. The radar chart and bar chart show a misleading 0 bar. Fix: pass NULL through in the struct, show "N/A" in the UI with a hatched bar style.

### What You Are Building (Tasks 4-11)

An **Analysis tab** with:
- **Tab navigation** in the header ("Map" | "Analysis")
- **Left panel (280px):** Filter controls — 8 range sliders (score, population, income, permits, migration, occupancy, employment, SSP growth), tier checkboxes, state multi-select. Count badge: "X of 3,209 counties."
- **Center:** Same map component with highlight/dim — matching counties keep tier colors, non-matching counties go gray
- **Right panel (400px):** Sortable results table (rank, county, state, score, tier, pop, income). Clicking a row opens CountyDetail. When no county selected, shows the ranked list.
- All filtering is **client-side** — the GeoJSON (~3,209 features) is already loaded in memory. No new API calls needed after initial fetch.

## Key File Paths

| File | Purpose |
|------|---------|
| `src/common/config.py` | Scoring weights (Task 1 modifies) |
| `src/gold/compute_scores.py` | Gold scoring pipeline (Tasks 1-2 modify) |
| `src/app/backend/routes/geojson.py` | GeoJSON endpoint (Task 4 enriches with metrics) |
| `src/app/backend/models/county.py` | Pydantic models (Task 1 modifies) |
| `src/app/backend/routes/scoring.py` | Scoring weights endpoint (Task 1 modifies) |
| `src/app/frontend/src/App.tsx` | App shell (Task 5 adds tab nav) |
| `src/app/frontend/src/App.css` | All styles (Tasks 2, 5, 10 modify) |
| `src/app/frontend/src/api/client.ts` | API client types (Task 1 modifies) |
| `src/app/frontend/src/components/NationalMap.tsx` | Map component (Task 5 changes props) |
| `src/app/frontend/src/components/CountyDetail.tsx` | Detail panel (Tasks 1-2 modify) |
| `src/app/frontend/src/components/WeightTuner.tsx` | Weight tuner (Task 1 modifies) |
| `src/app/frontend/src/components/analysisFilters.ts` | **NEW** filter types + logic (Task 6) |
| `src/app/frontend/src/components/FilterPanel.tsx` | **NEW** filter panel (Task 7) |
| `src/app/frontend/src/components/ResultsList.tsx` | **NEW** results table (Task 8) |
| `src/app/frontend/src/components/AnalysisView.tsx` | **NEW** analysis view (Task 9) |

## Execution Order

Execute tasks **strictly in order** (1 through 11). Phase 1 (bug fixes) must complete before Phase 2+ because the gold scoring job needs to re-run.

| Phase | Tasks | What | Notes |
|-------|-------|------|-------|
| 1 | 1-3 | Bug fixes + re-run gold scoring | Must deploy bundle + re-run job + rebuild frontend |
| 2 | 4 | Enrich GeoJSON endpoint | Backend change only |
| 3 | 5 | Tab navigation | Modifies App.tsx + NationalMap.tsx |
| 4 | 6-10 | Analysis view components + CSS | All new frontend files |
| 5 | 11 | Build, deploy, verify | End-to-end verification |

## Build & Deploy Commands

```bash
# Build frontend (from repo root)
cd src/app/frontend && npx vite build && cd ../../..

# Deploy bundle (uploads all code to workspace)
databricks bundle deploy --profile=DEFAULT

# Re-run gold scoring job (after scoring code changes)
databricks jobs run-now 196681278666944 --profile=DEFAULT

# Deploy the app
databricks apps deploy store-siting \
  --source-code-path /Workspace/Users/jesus.rodriguez@databricks.com/.bundle/store-siting/dev/files/src/app \
  --profile=DEFAULT
```

## Verification Checklist

After all 11 tasks are complete:

- [ ] QSR White Space no longer appears in component scores
- [ ] Missing indicators show "N/A" with hatched bar (not 0)
- [ ] SSP Projections weight is now 15% (up from 10%)
- [ ] "Map" and "Analysis" tabs appear in the header
- [ ] Map tab works exactly as before
- [ ] Analysis tab shows filter panel on left
- [ ] Range sliders auto-derive min/max from data
- [ ] Tier checkboxes filter counties on the map (dim non-matching)
- [ ] State multi-select works
- [ ] Population/income/score sliders filter map and results list
- [ ] Results list shows matching counties, sortable by clicking column headers
- [ ] Clicking a result row opens the county detail panel
- [ ] Close button on detail returns to results list
- [ ] Count badge updates live: "X of 3,209 counties"
- [ ] Reset button restores all filters to defaults

## Common Pitfalls

- **Frontend build uses `npx vite build`, not `npm run build`** — the latter runs `tsc` first which fails on test files with a missing type.
- **The GeoJSON is cached server-side** (`_geojson_cache` in `geojson.py`). After changing the query, the app must be redeployed (cache resets on restart).
- **NationalMap.tsx currently fetches its own GeoJSON** — Task 5 lifts this to App.tsx and passes it as a prop. Make sure to remove the internal `useState`/`useEffect` for geojson in NationalMap.
- **deck.gl clicks don't work via synthetic DOM events** — only real mouse interactions trigger picking. Use `evaluate_script` to dispatch React state changes for testing via Chrome DevTools.
- **After modifying `compute_scores.py`, you must:** (1) `databricks bundle deploy`, (2) `databricks jobs run-now 196681278666944`, (3) verify the job succeeds before deploying the app.
- **The `component_scores` struct is a Spark struct type stored in Delta.** When you change it from `coalesce(..., lit(0.0))` to just the nullable column, add `.option("overwriteSchema", "true")` if not already present on the gold table write (it is already there).

## Start Command

```
Read docs/plans/2026-03-05-analysis-tab-implementation.md, then execute Task 1 using superpowers:executing-plans.
```
