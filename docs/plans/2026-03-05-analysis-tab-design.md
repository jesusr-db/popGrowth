# Analysis Tab Design

**Date:** 2026-03-05
**Goal:** Add a second tab with multi-filter search panel for deeper analysis of county data, plus fix component score bugs.

## Bug Fixes (Pre-requisite)

### QSR White Space Always 0
The `qsr_establishments` column is NULL for all counties after the silver join — the `silver_business_patterns` table likely has no data. Fix: check if the table has data and run ingestion if needed. If data genuinely doesn't exist, remove QSR from scoring weights and redistribute its 5%.

### Net Migration Shows 0 for Missing Data
The component_scores struct uses `coalesce(normalized, lit(0.0))` which writes 0 for NULL indicators. The scoring is correct (adaptive weighting handles NULLs), but the radar chart and bar chart show a misleading 0. Fix: pass NULL through to the struct and show "No data" in the UI for indicators where the raw value is NULL.

## Tab Navigation

- Header gets "Map" and "Analysis" tabs
- Map tab: exactly as today, no changes
- Both tabs share the same GeoJSON data (fetched once on app load)

## Analysis Tab Layout

Three-column layout:

### Left Panel (280px) — Filters
- Count badge: "X of 3,209 counties"
- "Reset Filters" button
- Filter controls (all AND together):

| Filter | Type | Range |
|--------|------|-------|
| Composite Score | Range slider | 0-100 |
| Tier | Checkboxes | A, B, C, D, F |
| State | Multi-select dropdown | All 50 + DC |
| Population | Range slider | 0-max |
| Median Income | Range slider | 0-max |
| Permits / 1K Pop | Range slider | 0-max |
| Net Migration Rate | Range slider | min-max |
| Occupancy Rate | Range slider | 0%-100% |
| Employment per Capita | Range slider | 0-max |
| SSP Growth Rate | Range slider | min-max |

Slider ranges auto-derive from data min/max.

### Center — Map
Same map component, driven by filter state. Matching counties keep tier colors, non-matching counties dim to light gray (highlight + dim pattern).

### Right Panel (400px) — Results
- Default: Sortable ranked list of matching counties
  - Columns: Rank, County, State, Score, Tier, Population, Median Income
  - Default sort: composite score descending
  - Clickable column headers to re-sort
  - Clicking a row opens CountyDetail
  - Virtual scroll for performance (up to 3,209 rows)
- When county selected: CountyDetail panel with back button to return to list

## Data Flow

### GeoJSON Enrichment
Add 7 properties to each GeoJSON feature (from gold tables):
- population, median_income, permits_per_1k_pop, net_migration_rate, occupancy_rate, employment_per_capita, ssp_growth_rate

Increases payload from ~3.3MB to ~4MB. Single fetch on app load.

### Client-Side Filtering
Filter state object:
```
filters = {
  score: [0, 100],
  tiers: ['A', 'B', 'C', 'D', 'F'],
  states: [],
  population: [0, max],
  median_income: [0, max],
  permits_per_1k_pop: [0, max],
  net_migration_rate: [min, max],
  occupancy_rate: [0, 1],
  employment_per_capita: [0, max],
  ssp_growth_rate: [min, max],
}
```

Pure function `filterFeatures(geojson, filters)` returns matching FIPS set. Drives both map highlight/dim and results list.

## Approach
Client-side filtering (Approach 1). All 3,209 counties in memory, instant filtering, no API calls after initial load.

## Tech
React 18, TypeScript, deck.gl MapboxOverlay (reused), CSS virtual scroll for results list. No new dependencies.
