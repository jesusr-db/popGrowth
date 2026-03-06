# popGrowth

A QSR (Quick Service Restaurant) growth scoring application that scores all 3,209 US counties by leading indicators of population growth. Built on Databricks with a Medallion architecture (Bronze/Silver/Gold) and deployed as a Databricks App with a React + FastAPI frontend.

![Architecture: Bronze → Silver → Gold → App](https://img.shields.io/badge/Architecture-Medallion-blue)
![Counties: 3,209](https://img.shields.io/badge/Counties-3%2C209-green)
![Data Sources: 8](https://img.shields.io/badge/Data%20Sources-8-orange)

## Quick Start

```bash
# 1. Authenticate with Databricks
databricks auth login <workspace-url> --profile=<profile>

# 2. Deploy the bundle
databricks bundle deploy -t dev

# 3. Run the pipeline (Bronze → Silver → Gold)
databricks bundle run ingest_building_permits -t dev
databricks bundle run ingest_acs_demographics -t dev
databricks bundle run ingest_migration -t dev
databricks bundle run ingest_vacancy -t dev
databricks bundle run ingest_employment -t dev
databricks bundle run ingest_school_enrollment -t dev
databricks bundle run ingest_business_patterns -t dev
databricks bundle run transform_silver -t dev
databricks bundle run compute_gold_scores -t dev

# 4. Deploy the app
databricks apps deploy popGrowth --source-code-path <workspace-files-path>/src/app

# 5. Open the app URL in your browser
```

## Architecture

```
                  ┌─────────────────────────────────────────────────────┐
                  │                   Databricks                        │
                  │                                                     │
  Census APIs ──► │  Bronze (raw)  ──►  Silver (clean)  ──►  Gold      │
  BLS APIs    ──► │  8 ingestion       7 transforms         scored     │
  HUD APIs    ──► │  tables            county-FIPS          3,209      │
                  │                    standardized          counties   │
                  │                                                     │
                  │  ┌───────────────────────────────────────────────┐  │
                  │  │  Databricks App (FastAPI + React)             │  │
                  │  │                                               │  │
                  │  │  ┌──────────────┐  ┌───────────────────────┐ │  │
                  │  │  │ FastAPI      │  │ React + deck.gl       │ │  │
                  │  │  │ /api/counties│  │ Choropleth map        │ │  │
                  │  │  │ /api/geojson │  │ County detail panel   │ │  │
                  │  │  │ /api/scores  │  │ Weight tuner          │ │  │
                  │  │  └──────┬───────┘  └───────────────────────┘ │  │
                  │  │         │                                     │  │
                  │  │    SQL Warehouse (reads Gold tables)          │  │
                  │  └───────────────────────────────────────────────┘  │
                  └─────────────────────────────────────────────────────┘
```

**Tech Stack:** Python 3.11, PySpark, Delta Lake, Unity Catalog, FastAPI, React 18, TypeScript, deck.gl v9, MapLibre GL, Recharts, Vite, Databricks Asset Bundles (DABs)

## Data Sources

The app ingests 8 open government data sources to build a composite picture of county-level growth potential:

| # | Source | API | Frequency | What It Measures |
|---|--------|-----|-----------|-----------------|
| 1 | **Census Building Permits** | `api.census.gov/data/timeseries/eits/bps` | Monthly | New residential construction activity — single-family, multi-family, and total units permitted per county. A leading indicator: permits today mean rooftops (and customers) in 12-18 months. |
| 2 | **ACS Demographics** | `api.census.gov/data/{year}/acs/acs1` | Annual | Population counts and median household income from the American Community Survey 1-Year Estimates. Provides the denominator for per-capita metrics. |
| 3 | **USPS Migration (via HUD)** | `huduser.gov/hudapi/public/usps` | Quarterly | Net migration (move-ins minus move-outs) tracked via USPS address changes. Captures real-time population flow — are people arriving or leaving? |
| 4 | **USPS Vacancy (via HUD)** | `huduser.gov/hudapi/public/usps` | Quarterly | Residential vacancy rates from USPS delivery data. Lower vacancy = higher occupancy = more potential customers per square mile. |
| 5 | **BLS QCEW Employment** | `data.bls.gov/cew/data/api` | Quarterly | Total employment and average weekly wages from the Quarterly Census of Employment and Wages. Measures local economic vitality and spending power. |
| 6 | **NCES School Enrollment** | `educationdata.urban.org/api/v1` | Annual | Public school enrollment from the National Center for Education Statistics. School enrollment growth is a strong leading indicator of family formation and residential demand. |
| 7 | **Census Business Patterns** | `api.census.gov/data/{year}/cbp` | Annual | QSR (NAICS 7222) establishment counts and retail density. Used inversely — fewer existing QSR establishments means more white space opportunity. |
| 8 | **SSP Population Projections** | State-level Census estimates | Periodic | Shared Socioeconomic Pathway (SSP2 "middle-of-the-road") population projections through 2035. Projects state-level population growth and applies it to counties within each state. |

### How Population Projections Work

Population projections use the **SSP2 (middle-of-the-road)** scenario from the IIASA Shared Socioeconomic Pathways framework. This is how they flow through the pipeline:

1. **State-level base populations** are computed by summing county populations from the ACS within each state.
2. **Annual growth rates** are derived from Census Bureau vintage population estimates (2020-2023 trends). For example, Texas grows at ~1.4%/year, Idaho at ~1.8%/year, while Illinois declines at ~0.4%/year.
3. **Projections are computed** for 2025, 2030, and 2035 by compounding the annual growth rate forward from the base population.
4. **State projections are joined to counties** by matching the first two digits of the 5-digit county FIPS code (the state FIPS prefix). Every county in Texas inherits Texas's projected growth rate.
5. **The growth rate feeds into the composite score** as the `ssp_projected_growth` indicator (10% weight). Counties in high-growth states score higher.

The projection card in the county detail panel shows:
- Current population (from ACS)
- Projected state-level population at 2035
- Percentage growth with directional indicator (green ▲ / red ▼)

To improve projection accuracy, you could:
- Replace state-level projections with county-level projections from the University of Virginia's Weldon Cooper Center or similar sources
- Use multiple SSP scenarios (SSP1 through SSP5) to show optimistic/pessimistic ranges
- Incorporate local zoning and master plan data for sub-county projections
- Add historical population growth trends as a time series chart

## Scoring Logic

### Composite Growth Score (0-100)

Each county receives a composite growth score computed from 7 normalized indicators:

```
Score = Σ(normalized_indicator × weight) / available_weight × 100
```

| Indicator | Source Column | Weight | Direction | What It Captures |
|-----------|-------------|--------|-----------|-----------------|
| **Building Permits** | `permits_per_1k_pop` | 25% | Higher = better | Construction activity relative to population |
| **Net Migration** | `net_migration_rate` | 20% | Higher = better | Are people moving in or out? |
| **Occupancy** | `occupancy_rate` (1 - vacancy) | 15% | Higher = better | Housing demand signal |
| **Employment** | `employment_per_capita` | 15% | Higher = better | Local job market strength |
| **School Enrollment** | `enrollment_per_capita` | 10% | Higher = better | Family formation proxy |
| **Pop. Projections** | `ssp_growth_rate` | 10% | Higher = better | Long-term demographic trajectory |
| **QSR White Space** | `qsr_establishments` | 5% | Lower = better (inverted) | Fewer competitors = more opportunity |

### Normalization

Each indicator is min-max normalized across all 3,209 counties to a 0-1 range:

```
normalized = (value - min) / (max - min)
```

For inverted indicators (QSR White Space), the formula is:

```
normalized = 1 - (value - min) / (max - min)
```

### Adaptive Weighting

If an indicator is NULL for a county (e.g., no migration data available), its weight is **redistributed proportionally** among the available indicators rather than treating NULL as zero. This prevents missing data from unfairly penalizing a county's score.

```
effective_score = Σ(norm_i × weight_i) / Σ(available_weights) × 100
```

### Tier Assignment (Percentile-Based)

Tiers are assigned using percentile ranks across all scored counties, ensuring a meaningful distribution regardless of the absolute score range:

| Tier | Percentile | Counties | Meaning |
|------|-----------|----------|---------|
| **A** | Top 10% | ~321 | Highest growth potential |
| **B** | Top 30% | ~642 | Strong growth signals |
| **C** | Top 60% | ~963 | Moderate growth |
| **D** | Top 85% | ~802 | Below average |
| **F** | Bottom 15% | ~481 | Lowest growth indicators |

### Configurable Weights

The app includes a weight tuner panel (click "Adjust Weights" in the header) that lets users adjust the relative importance of each indicator and recalculate scores in real time. Weight changes are persisted to the `gold_scoring_config` table and trigger a re-scoring job.

## Project Structure

```
store/
├── databricks.yml              # DABs bundle configuration
├── resources/
│   ├── app.yml                 # Databricks App resource definition
│   └── jobs/                   # Job definitions for each pipeline stage
│       ├── ingest_*.yml        # Bronze ingestion jobs (8 sources)
│       ├── transform_silver.yml # Silver transformation job
│       └── compute_gold_scores.yml # Gold scoring job
├── src/
│   ├── common/
│   │   ├── config.py           # Weights, tiers, catalog config
│   │   ├── fips.py             # FIPS code utilities
│   │   ├── ingestion_logger.py # Ingestion audit logging
│   │   └── schemas.py          # Shared schemas
│   ├── ingestion/              # Bronze layer — one module per data source
│   │   ├── building_permits.py
│   │   ├── acs_demographics.py
│   │   ├── migration.py
│   │   ├── vacancy.py
│   │   ├── employment.py
│   │   ├── school_enrollment.py
│   │   ├── business_patterns.py
│   │   └── national_projections.py
│   ├── silver/                 # Silver transforms — standardize to county FIPS
│   │   ├── building_permits.py
│   │   ├── acs_demographics.py
│   │   ├── migration.py
│   │   ├── vacancy.py
│   │   ├── employment.py
│   │   ├── school_enrollment.py
│   │   └── ssp_projections.py
│   ├── gold/
│   │   ├── compute_scores.py   # Join indicators, normalize, score, rank
│   │   └── scoring.py          # Scoring engine (composite score, tier assignment)
│   ├── jobs/                   # Spark job entry points
│   │   ├── ingest_*.py
│   │   ├── transform_silver.py
│   │   └── compute_gold_scores.py
│   └── app/
│       ├── app.py              # Uvicorn entry point
│       ├── app.yaml            # Databricks App config (env vars)
│       ├── requirements.txt    # Python dependencies
│       ├── backend/
│       │   ├── main.py         # FastAPI app with CORS + static files
│       │   ├── db.py           # SQL Warehouse connector (SDK OAuth)
│       │   ├── models/
│       │   │   └── county.py   # Pydantic models
│       │   └── routes/
│       │       ├── counties.py # /api/counties, /api/counties/{fips}
│       │       ├── geojson.py  # /api/geojson (choropleth data)
│       │       └── scoring.py  # /api/scores/weights (GET/PUT)
│       └── frontend/
│           ├── src/
│           │   ├── App.tsx
│           │   ├── App.css
│           │   ├── api/client.ts
│           │   └── components/
│           │       ├── NationalMap.tsx   # deck.gl choropleth + MapLibre
│           │       ├── CountyDetail.tsx  # Side panel with metrics + radar
│           │       └── WeightTuner.tsx   # Scoring weight sliders
│           └── dist/           # Built frontend (served by FastAPI)
└── tests/
    ├── unit/
    └── integration/
```

## Unity Catalog Tables

All tables live under `{catalog}.{schema}`:

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `bronze.building_permits` | Raw Census building permit records |
| Bronze | `bronze.acs_demographics` | Raw ACS population and income |
| Bronze | `bronze.migration` | Raw USPS migration data |
| Bronze | `bronze.vacancy` | Raw USPS vacancy data |
| Bronze | `bronze.employment` | Raw BLS QCEW employment |
| Bronze | `bronze.school_enrollment` | Raw NCES enrollment |
| Bronze | `bronze.business_patterns` | Raw Census CBP |
| Bronze | `bronze.national_projections` | Raw population projections |
| Silver | `silver.silver_*` | Cleaned, county-FIPS standardized versions of each Bronze table |
| Gold | `gold.gold_county_growth_score` | Final scored + ranked counties with composite scores, tiers, SSP projections |
| Gold | `gold.gold_county_details` | Full indicator values per county (permits, migration, vacancy, employment, etc.) |
| Gold | `gold.gold_scoring_config` | Current scoring weights (indicator → weight mapping) |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/counties` | List all scored counties (optional `?state=TX` filter) |
| GET | `/api/counties/top?n=25` | Top N counties by score (optional `?state=TX`) |
| GET | `/api/counties/{fips}` | Full county detail (scores + all indicators + projections) |
| GET | `/api/geojson` | County polygons with scores for map rendering |
| GET | `/api/trends/{fips}` | Historical trend data (placeholder for multi-year) |
| GET | `/api/scores/weights` | Current scoring weights |
| PUT | `/api/scores/weights` | Update scoring weights and trigger re-score |
