# Store Siting App — Design Document

**Date:** 2026-03-05
**Status:** Approved
**Customer Use Case:** QSR store siting using leading indicators of population growth
**Deployment:** Databricks (DABs) — FE demo workspace
**Stack:** Medallion architecture + React/FastAPI Databricks App

---

## Problem Statement

Census data arrives every 10 years. QSR chains need first-mover advantage on store locations — they need to know where population will grow 1-3 years before it shows up in official counts. This app ingests open-source leading indicators of population growth, computes a composite growth score per US county, and presents results in an interactive choropleth map.

## Architecture: Medallion + Databricks App (Approach B)

Bronze/Silver/Gold medallion architecture with separate ingestion jobs per data source. React + FastAPI Databricks App reads from Gold. DABs bundles everything for portable deployment.

---

## Section 1: Data Sources & Ingestion (Bronze Layer)

All tables registered in Unity Catalog under `store_siting.bronze`.

| # | Source | Format | Frequency | Grain | Signal |
|---|--------|--------|-----------|-------|--------|
| 1 | Census Building Permits | CSV/API (census.gov) | Monthly | County/Metro | New residential construction |
| 2 | HUD Residential Construction Permits | CSV (huduser.gov) | Quarterly | County | Permitted units by structure type |
| 3 | USPS Change-of-Address (via HUD) | CSV (huduser.gov) | Quarterly | County/ZIP | Net migration |
| 4 | USPS Vacancy Data (via HUD) | CSV (huduser.gov) | Quarterly | County/ZIP/Tract | Declining vacancy = growth |
| 5 | BLS QCEW Employment | CSV/API (bls.gov) | Quarterly | County | Job growth attracts population |
| 6 | NCES School Enrollment | CSV (nces.ed.gov) | Annual | County/District | Growing schools = growing families |
| 7 | ACS 1-Year Estimates | API (census.gov) | Annual | County | Current population, income, demographics |
| 8 | Census County Business Patterns | CSV/API (census.gov) | Annual | County | Business density, QSR competition |
| 9 | County-Level SSP Projections | CSV (data.gov) | Static/Periodic | County | Long-range population scenarios |
| 10 | Census National Population Projections | CSV (census.gov) | Periodic | National/State | Macro growth context |

**Ingestion pattern:** Python jobs using `requests`/`urllib` to download, load via Spark into Bronze Delta tables. Each table keeps `source_date` and `ingested_at` columns for lineage.

---

## Section 2: Transformation (Silver Layer)

All Silver tables in `store_siting.silver`, standardized to **5-digit FIPS county code** as common grain.

**Standardization rules:**
- Geography: All sources mapped to FIPS county code. ZIP-level data aggregated using HUD ZIP-County crosswalk.
- Time: Normalized to `report_year` + `report_quarter` (quarterly) or `report_year` (annual). Monthly permits rolled up to quarterly.
- Deduplication: Remove overlapping download duplicates.
- Nulls: Flag missing counties per source rather than dropping.
- Rates: Convert raw counts to per-capita rates and YoY changes.

**Silver tables:**

| Table | Key Columns |
|-------|-------------|
| `silver_building_permits` | fips, report_year, report_quarter, total_units_permitted, single_family_units, multi_family_units, permits_per_1k_pop |
| `silver_hud_construction` | fips, report_year, report_quarter, permitted_units_by_type, construction_growth_rate |
| `silver_migration` | fips, report_year, report_quarter, inflow, outflow, net_migration, net_migration_rate |
| `silver_vacancy` | fips, report_year, report_quarter, vacancy_rate, vacancy_rate_yoy_change |
| `silver_employment` | fips, report_year, report_quarter, total_employment, employment_growth_rate, avg_weekly_wage |
| `silver_school_enrollment` | fips, report_year, total_enrollment, enrollment_growth_rate |
| `silver_acs_demographics` | fips, report_year, population, median_income, median_age, households |
| `silver_business_patterns` | fips, report_year, total_establishments, qsr_establishments, retail_density |
| `silver_ssp_projections` | fips, projection_year, projected_population, scenario (SSP1-5) |
| `silver_national_projections` | state_fips, projection_year, projected_population |

---

## Section 3: Scoring (Gold Layer)

Primary table: `store_siting.gold.gold_county_growth_score`

| Column | Description |
|--------|-------------|
| fips | 5-digit FIPS county code |
| county_name | Human-readable name |
| state | State abbreviation |
| report_year | Score period year |
| report_quarter | Score period quarter |
| population | Current ACS population |
| median_income | Current ACS median income |
| composite_score | Weighted 0-100 score |
| score_tier | A/B/C/D/F |
| rank_national | Rank out of ~3,100 US counties |
| component_scores | Struct with individual indicator scores |

**Weighted Composite Scoring Formula:**

Each indicator min-max normalized to 0-1 across all counties, then weighted:

| Indicator | Weight | Rationale |
|-----------|--------|-----------|
| Building permits per 1K pop (Census + HUD combined) | 25% | Strongest leading indicator |
| Net migration rate | 20% | People voting with their feet |
| Vacancy rate YoY change (inverted) | 15% | Filling up = demand |
| Employment growth rate | 15% | Jobs attract residents and traffic |
| School enrollment growth rate | 10% | Families = core QSR demographic |
| SSP projected population growth | 10% | Long-range trajectory confirmation |
| Existing QSR density (inverted) | 5% | White space opportunity |

`composite_score = SUM(normalized_indicator * weight) * 100`

**Score tiers:** A (80-100), B (60-79), C (40-59), D (20-39), F (0-19)

**Secondary table:** `gold_county_details` — wide denormalized table with all raw and derived metrics for the drill-down view.

**Configurability:** Weights stored in `gold_scoring_config` table. FastAPI endpoint allows updating weights and re-triggering scoring.

---

## Section 4: Application Architecture (React + FastAPI)

Databricks App deployed via DABs using APX pattern.

### Backend (FastAPI)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/counties` | GET | All counties with score, tier, rank. `?state=` filter |
| `/api/counties/{fips}` | GET | Full county detail — all scores, metrics, time series |
| `/api/counties/top` | GET | Top N counties. `?n=25&state=` |
| `/api/scores/weights` | GET | Current scoring weights |
| `/api/scores/weights` | PUT | Update weights, triggers async recomputation |
| `/api/geojson` | GET | County boundaries GeoJSON with scores joined |
| `/api/trends/{fips}` | GET | Historical time series for a county |

Connects to Unity Catalog via `databricks-sql-connector`.

### Frontend (React)

| View | Description |
|------|-------------|
| **National Map** | County choropleth (deck.gl/react-map-gl), colored by score (green=A to red=F). Hover tooltip, click to drill down. State/tier/score filters. |
| **County Detail** | Radar chart of component scores, metric cards, trend sparklines, tier badge, national rank. |
| **Weight Tuner** | Sliders for 7 indicator weights. Recalculate button refreshes map. Shows ranking shift feedback. |

**GeoJSON:** Census TIGER/Line shapefiles (~15MB simplified), pre-joined with scores on backend, cached aggressively.

---

## Section 5: DABs Deployment & Pipeline Orchestration

### Bundle Structure

```
store-siting/
├── databricks.yml
├── resources/
│   ├── jobs/                          # 12 job definitions (10 ingest + silver + gold)
│   └── app.yml                        # Databricks App resource
├── src/
│   ├── ingestion/                     # One module per source (10 files)
│   ├── silver/transforms.py
│   ├── gold/scoring.py
│   └── app/
│       ├── backend/                   # FastAPI (main.py, routes/, models/)
│       └── frontend/                  # React (App.tsx, components/, api/)
├── data/county_geojson/               # Simplified TIGER/Line boundaries
└── tests/
```

### Job Orchestration

```
Ingestion Jobs (10 independent, parallel)
        |
        v
Silver Transform Job (depends on all ingestion)
        |
        v
Gold Scoring Job (depends on Silver)
        |
        v
App serves latest scores
```

- Ingestion scheduled per source frequency (monthly/quarterly/annual).
- Silver + Gold triggered downstream via job dependencies.
- Shared single-node job cluster for demo cost efficiency.
- All tables in Unity Catalog: `store_siting.{bronze,silver,gold}`.

---

## Section 6: Error Handling & Testing

### Error Handling

| Layer | Strategy |
|-------|----------|
| Ingestion | Retry with exponential backoff (3 attempts). Log to `bronze._ingestion_log`. Don't block other sources. |
| Silver | Skip counties with missing data, flag in `data_completeness` column. Log to `silver._transform_log`. |
| Gold | Score with available data if >= 5 sources present. Add `data_coverage` field. Mark "Insufficient Data" if < 5 sources. |
| App | Proper HTTP error codes, user-friendly messages, stale data indicator. |

### Testing

| Type | Scope |
|------|-------|
| Unit | Scoring formula, normalization, FIPS mapping, rate calculations |
| Integration | Each ingestion module against sample fixtures. Silver schema validation. |
| Data quality | No null FIPS in Silver, scores 0-100, all ~3,100 counties present in Gold |
| App | FastAPI endpoint tests (pytest), React component tests (Vitest) |
