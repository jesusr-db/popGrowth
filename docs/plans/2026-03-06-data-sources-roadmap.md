# Data Sources Roadmap — County-Level Store Siting

**Date:** 2026-03-06
**Goal:** Expand the set of free public data sources used for county-level QSR composite growth scoring.

## Current Sources

| Source | Granularity | Frequency |
|--------|-------------|-----------|
| Census Building Permits | County | Monthly |
| Census ACS Demographics | County | Annual |
| IRS SOI Migration (net) | County | Annual |
| Census ACS Vacancy | County | Annual |
| BLS QCEW Employment | County | Quarterly |
| Census ACS School Enrollment | County | Annual |
| Census County Business Patterns | County | Annual |
| SSP Population Projections | State (stub, not implemented) | — |

---

## Tier 1 — High Impact, Easy to Add (APIs)

### 1. Census Population Estimates Program (PEP)
- **URL:** `api.census.gov/data/{year}/pep/population`
- **Method:** REST API, same Census API key as existing sources
- **Granularity:** County | **Frequency:** Annual
- **Measures:** Intercensal population estimates with components of change (births, deaths, domestic migration, international migration).
- **Value:** Provides actual historical county growth rates and migration decomposition, replacing reliance on state-level SSP proxies.

### 2. Census ACS Commuting / Journey to Work (B08301)
- **URL:** `api.census.gov/data/{year}/acs/acs5` — table B08301
- **Method:** REST API (ACS 5-year)
- **Granularity:** County | **Frequency:** Annual
- **Measures:** Mode of transportation to work, commuting volumes.
- **Value:** Reveals economic connectivity between counties; high inbound commuting signals daytime population (and lunch traffic) exceeding resident counts.

### 3. Census ACS Age Distribution (B01001)
- **URL:** `api.census.gov/data/{year}/acs/acs5` — table B01001
- **Method:** REST API (ACS 5-year)
- **Granularity:** County | **Frequency:** Annual
- **Measures:** Population by 5-year age cohorts and sex.
- **Value:** Age structure predicts family formation (25-34 cohort), workforce growth, and QSR-heavy demographics (18-34).

---

## Tier 2 — High Impact, Moderate Effort (File Downloads)

### 4. IRS SOI County-to-County Migration (full matrix)
- **URL:** `irs.gov/statistics/soi-tax-stats-migration-data`
- **Method:** CSV download (annual zip files)
- **Granularity:** County-pair flows | **Frequency:** Annual
- **Measures:** Full origin-destination migration matrix (inflows and outflows by county pair, with AGI).
- **Value:** Current ingestion uses net totals only. The full matrix shows WHERE people move from/to, enabling feeder-county and spillover analysis.

### 5. USPS Change of Address via HUD Crosswalk
- **URL:** `huduser.gov/portal/datasets/usps_crosswalk.html`
- **Method:** CSV download (requires free HUD account)
- **Granularity:** ZIP-to-county | **Frequency:** Quarterly
- **Measures:** Address vacancy, change-of-address counts, no-stat addresses.
- **Value:** Most timely migration/mobility signal available — leads Census estimates by 12-18 months.

### 6. FHFA House Price Index
- **URL:** `fhfa.gov/DataTools/Downloads`
- **Method:** CSV/Excel download
- **Granularity:** County (3-digit FIPS) | **Frequency:** Quarterly
- **Measures:** Repeat-sale house price appreciation index.
- **Value:** Rising prices signal demand exceeding supply — a leading indicator of population pressure and rooftop growth.

### 7. SEDAC County-Level SSP Projections
- **URL:** `sedac.ciesin.columbia.edu/data/set/popdynamics-us-county-level-pop-projections-v1`
- **Method:** Manual download (NetCDF/CSV)
- **Granularity:** County | **Frequency:** Decadal projections (SSP2)
- **Measures:** Population projections under SSP2 scenario out to 2100.
- **Value:** Directly replaces the broken state-level SSP stub with actual county-level projections.

---

## Tier 3 — Nice to Have (Enhance Scoring)

### 8. FDIC Summary of Deposits
- **URL:** `fdic.gov/resources/data-tools/summary-of-deposits`
- **Method:** CSV download
- **Granularity:** Branch/County | **Frequency:** Annual (June 30)
- **Measures:** Bank branch counts and deposit totals per county.
- **Value:** Branch density is a proxy for commercial activity and retail infrastructure maturity.

### 9. Census LEHD Origin-Destination Employment
- **URL:** `lehd.ces.census.gov/data`
- **Method:** CSV download (LODES files)
- **Granularity:** Block-to-block (aggregable to county) | **Frequency:** Annual
- **Measures:** Where workers live vs. where they work.
- **Value:** Quantifies daytime population shifts; counties with large net worker inflows have higher lunch-hour QSR demand.

### 10. NCES School District Finance
- **URL:** `nces.ed.gov/ccd/f33agency.asp`
- **Method:** CSV download
- **Granularity:** School district (mappable to county) | **Frequency:** Annual
- **Measures:** Per-pupil expenditure, revenue sources.
- **Value:** School quality is a top driver of family in-migration; high per-pupil spending correlates with desirable suburban growth counties.

### 11. EPA Smart Location Database
- **URL:** `epa.gov/smartgrowth/smart-location-mapping`
- **Method:** Geodatabase download
- **Granularity:** Census block group | **Frequency:** Periodic (updated ~every 3 years)
- **Measures:** Walkability index, transit access, land-use mix, employment density.
- **Value:** Adds a built-environment dimension to scoring; walkable, mixed-use areas support higher QSR foot traffic.

---

## Priority Order

1. **PEP** (Tier 1) — biggest single improvement; replaces SSP stub with real data.
2. **SEDAC SSP** (Tier 2) — complements PEP with forward-looking projections.
3. **Age Distribution** (Tier 1) — quick API add, directly relevant to QSR demographics.
4. **FHFA HPI** (Tier 2) — leading demand indicator, quarterly cadence.
5. **IRS county-to-county flows** (Tier 2) — extends existing IRS pipeline.
6. **Commuting** (Tier 1) — daytime population adjustment.
7. **HUD/USPS** (Tier 2) — timeliest migration signal.
8. Tier 3 sources as capacity allows.
