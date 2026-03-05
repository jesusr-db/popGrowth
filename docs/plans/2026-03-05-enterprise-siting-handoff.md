# Enterprise Store Siting — Agent Handoff Document

## What This Is

You are picking up a **fully working** QSR store siting application deployed on Databricks. Your job is to implement 4 phases that add street-level site selection, site comparison, and cannibalization analysis. The app is live and working — you are adding enterprise features to it.

## Critical Files To Read First

1. **Design doc:** `docs/plans/2026-03-05-enterprise-siting-design.md` — the approved design with all 5 features, architecture decisions, data model, and component specs
2. **Previous implementation plan (for patterns):** `docs/plans/2026-03-05-analysis-tab-implementation.md` — shows the task structure and commit style used in this repo

## Required Skill

Use `superpowers:writing-plans` to create the implementation plan from the design doc, then `superpowers:executing-plans` to implement it. Phases 1-4 only — phases 5-6 are roadmap (do not implement).

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
- 3,221 US counties scored with composite growth scores (6 indicators, adaptive weighting)
- React + FastAPI Databricks App deployed and running:
  - **Map tab:** Choropleth map (deck.gl MapboxOverlay + MapLibre GL), county detail panel with radar chart, bar chart, SSP projection card. State/tier/score filters.
  - **Analysis tab:** Collapsible filter panel (8 range sliders, tier checkboxes, state multi-select), highlight/dim map, sortable results table, county detail on row click
  - Weight tuner panel for re-scoring
- DABs bundle with 12 job definitions + app resource

## What You Are Building (Phases 1-4)

### Phase 1: Overture Maps Ingestion Pipeline

Ingest real POI and road segment data from Overture Maps into the existing medallion pipeline.

**Data source:** Overture Maps Foundation (open data, Parquet on cloud storage)
- **Places theme:** ~60M US POIs including restaurants. Filter to QSR-relevant categories.
- **Transportation/segment theme:** Road network with functional class (motorway, trunk, primary, etc.)

**IMPORTANT — Overture Maps S3 path discovery:**
The exact S3 paths need to be verified. The previous agent tried `s3://overturemaps-us-west-2/release/2025-03-02.0/` which returned PATH_DOES_NOT_EXIST. You must:
1. Check https://docs.overturemaps.org/getting-data/ for current release paths
2. The data may be on Azure Blob (`wasbs://`) or AWS S3 — Overture publishes to both
3. Alternative: use the `overturemaps` Python CLI package to download, or query via the Overture Maps Parquet files directly from their documented cloud paths
4. Databricks can read Parquet directly from S3/Azure with `spark.read.parquet("s3://...")`

**Bronze tables to create:**
- `bronze.overture_places` — Raw places data, filtered to US restaurants/retail
- `bronze.overture_segments` — Raw road segments, filtered to US

**Silver tables to create:**
- `silver.silver_poi_restaurants` — QSR-relevant POIs with columns: `id`, `lat`, `lng`, `name`, `brand`, `category`, `fips` (derived from lat/lng → county FIPS lookup)
- `silver.silver_road_segments` — Road segments with `traffic_class` score (1-5: motorway=5, trunk=4, primary=3, secondary=2, residential=1). Columns: `id`, `geometry`, `functional_class`, `traffic_class`, `fips`

**New files:**
- `src/ingestion/overture_maps.py` — Ingestion module (follows pattern in `src/ingestion/building_permits.py`)
- `src/silver/overture_poi.py` — Silver transform for POI restaurants
- `src/silver/overture_segments.py` — Silver transform for road segments
- `src/jobs/ingest_overture_maps.py` — Job entry point (follows pattern in `src/jobs/ingest_building_permits.py`)
- `resources/jobs/ingest_overture_maps.yml` — DABs job definition

**CONSTRAINT: All data must be real. No synthetic, mock, or placeholder data.**

### Phase 2: SiteExplorer View + Competitor/Traffic Layers

New street-level drill-down view. Entry point: "Explore Sites ▶" button added to `CountyDetail.tsx`.

**New API endpoints (in `src/app/backend/routes/sites.py`):**
- `GET /api/sites/poi?fips={fips}` — QSR POIs within county bounds (query `silver.silver_poi_restaurants`)
- `GET /api/sites/traffic?fips={fips}` — Road segments within county (query `silver.silver_road_segments`)
- `GET /api/sites/demographics?lat={lat}&lng={lng}&radius={miles}` — Aggregated demographics

**New frontend components:**
- `SiteExplorer.tsx` — Full-screen street-level map + stats sidebar
- `CompetitorLayer.ts` — deck.gl ScatterplotLayer for QSR POI pins
- `TrafficLayer.ts` — deck.gl PathLayer for road segments colored by functional class
- `SiteStats.tsx` — Sidebar showing competitor counts, traffic score, demographics

**Navigation changes:**
- `CountyDetail.tsx` — Add "Explore Sites ▶" button, needs `onExplore` callback prop
- `App.tsx` — Add `siteExplorer` view state, render `SiteExplorer` when active
- `AnalysisView.tsx` — Pass `onExplore` through to CountyDetail

### Phase 3: Isochrone Integration (Drive-Time Polygons)

When user drops a pin on the SiteExplorer map, generate 5/10/15-minute drive-time isochrone polygons.

**Routing engine options (pick one that works):**
1. **OpenRouteService API** (free tier: 2,000 req/day) — Simplest. `POST https://api.openrouteservice.org/v2/isochrones/driving-car` with API key. Sufficient for demo.
2. **Valhalla** — Self-hosted, no API limits, but requires OSM data (~8GB PBF for US). Can run as subprocess.
3. **OSRM** — Similar to Valhalla, lighter weight.

**Recommendation: Start with OpenRouteService API** for fastest implementation. Sign up at openrouteservice.org for free API key.

**New API endpoint:**
- `POST /api/sites/isochrone` — Body: `{lat, lng, times: [5,10,15]}` → Returns GeoJSON FeatureCollection with 3 isochrone polygons

**New frontend:**
- `IsochroneLayer.ts` — deck.gl GeoJsonLayer for drive-time polygons (semi-transparent fill, colored by time: green=5min, yellow=10min, red=15min)
- Pin drop interaction on SiteExplorer map (onClick when in "drop pin" mode)
- Stats sidebar updates with: competitors within each ring, population within 10-min ring

### Phase 4: Site Comparison + Cannibalization

User can save up to 3 candidate pins and compare them side-by-side.

**New npm dependencies:**
- `@turf/intersect` (~8KB gzip) — polygon overlap calculation
- `@turf/area` (~3KB gzip) — area measurement for overlap percentage

**New frontend components:**
- `ComparisonTray.tsx` — Bottom bar showing saved candidate thumbnails (name, score, competitor count)
- `ComparisonView.tsx` — Full side-by-side comparison layout (see wireframe in design doc)
- `SiteMiniMap.tsx` — Small static map with isochrone for each comparison column
- `CannibalizationOverlay.ts` — Turf.js polygon intersection + overlap % calculation

**Data model (React state, no backend persistence):**
```typescript
interface CandidateSite {
  id: string;
  lat: number;
  lng: number;
  address?: string;
  countyFips: string;
  isochrones: GeoJSON[];    // 5, 10, 15 min polygons
  metrics: {
    competitorsIn5min: number;
    competitorsIn10min: number;
    competitorsIn15min: number;
    populationIn10min: number;
    medianIncomeIn10min: number;
    trafficScore: number;
  };
}
```

**Cannibalization logic:**
- For each pair of candidate sites, compute `turf.intersect(siteA.isochrones[1], siteB.isochrones[1])` (10-min rings)
- Calculate overlap area as % of each site's trade area using `turf.area()`
- Display warning if overlap > 20%: "40% trade area overlap with Site A"

## Key Codebase Patterns

### Ingestion Pattern (`src/ingestion/building_permits.py`)
- Function: `ingest(spark, year, month, catalog=None)`
- Fetch data → parse into list of dicts → `spark.createDataFrame(rows)` → add metadata → `write.mode("append").saveAsTable()`
- Uses `log_ingestion()` from `src.common.ingestion_logger`

### Silver Transform Pattern (`src/silver/building_permits.py`)
- Function: `transform_building_permits(bronze_df) -> DataFrame`
- Read bronze → filter/aggregate → add calculated columns → return DataFrame
- Orchestrated by `src/silver/transforms.py` → `run_all_silver_transforms(spark)`
- Each writes to `{catalog}.silver.silver_{name}` with mode="overwrite"

### Job Entry Point Pattern (`src/jobs/ingest_building_permits.py`)
- Boilerplate: sys.path manipulation to find `src/` root
- Import ingestion function, create SparkSession, call ingest

### DABs Job YAML Pattern (`resources/jobs/ingest_building_permits.yml`)
```yaml
resources:
  jobs:
    ingest_overture_maps:
      name: "store-siting-ingest-overture-maps"
      tasks:
        - task_key: ingest
          environment_key: default
          spark_python_task:
            python_file: ../../src/jobs/ingest_overture_maps.py
            source: WORKSPACE
      environments:
        - environment_key: default
          spec:
            client: "1"
```

### Backend Route Pattern (`src/app/backend/routes/counties.py`)
- `router = APIRouter(prefix="/api")`
- Query via `execute_query(sql_string)` from `backend.db`
- Register in `src/app/backend/main.py` with `app.include_router(router)`
- Input validation with regex, HTTPException for bad input

### Frontend Component Pattern
- deck.gl layers via `MapboxOverlay` + `useControl` hook
- MapLibre GL basemap: `https://basemaps.cartocdn.com/gl/positron-gl-style/style.json`
- API calls via `src/app/frontend/src/api/client.ts` — add new methods there

## Key File Paths

| File | Purpose |
|------|---------|
| `src/common/config.py` | Catalog, schema names, weights |
| `src/common/fips.py` | FIPS normalization, state lookup |
| `src/common/ingestion_logger.py` | `log_ingestion()` utility |
| `src/ingestion/building_permits.py` | Example ingestion module (follow this pattern) |
| `src/silver/transforms.py` | Silver orchestrator (add new transforms here) |
| `src/jobs/ingest_building_permits.py` | Example job entry point |
| `resources/jobs/ingest_building_permits.yml` | Example DABs job YAML |
| `src/app/backend/main.py` | FastAPI app — register new routers here |
| `src/app/backend/routes/sites.py` | **NEW** — site exploration API endpoints |
| `src/app/backend/db.py` | `execute_query()` function |
| `src/app/frontend/src/App.tsx` | App shell — add SiteExplorer view routing |
| `src/app/frontend/src/App.css` | All styles |
| `src/app/frontend/src/api/client.ts` | API client — add new methods |
| `src/app/frontend/src/components/CountyDetail.tsx` | Add "Explore Sites" button |
| `src/app/frontend/src/components/AnalysisView.tsx` | Pass onExplore callback |
| `src/app/frontend/src/components/SiteExplorer.tsx` | **NEW** — street-level view |
| `src/app/frontend/src/components/ComparisonView.tsx` | **NEW** — side-by-side comparison |
| `src/app/frontend/src/components/ComparisonTray.tsx` | **NEW** — candidate tray |
| `src/app/frontend/package.json` | Add @turf/intersect, @turf/area |
| `databricks.yml` | DABs bundle config |

## Build & Deploy Commands

```bash
# Build frontend (from repo root)
cd src/app/frontend && npx vite build && cd ../../..

# Deploy bundle (uploads all code to workspace)
databricks bundle deploy --profile=DEFAULT

# Run a job (e.g., overture ingestion)
databricks jobs run-now <JOB_ID> --profile=DEFAULT

# Deploy the app
databricks apps deploy store-siting \
  --source-code-path /Workspace/Users/jesus.rodriguez@databricks.com/.bundle/store-siting/dev/files/src/app \
  --profile=DEFAULT
```

## Implementation Order

| Phase | What | Key Blocker |
|-------|------|-------------|
| **1** | Overture Maps ingestion (places + segments → bronze → silver) | Must verify Overture S3/Azure paths first |
| **2** | SiteExplorer view + competitor/traffic layers + API endpoints | Needs Phase 1 data in silver tables |
| **3** | Isochrone integration (routing engine + drive-time polygons) | Needs Phase 2 view to add layers to |
| **4** | Site comparison + cannibalization (Turf.js) | Needs Phase 2-3 (pins + isochrones) |

**Do NOT implement:** Phase 5 (AI chat) or Phase 6 (PDF reports) — these are roadmap items.

## Common Pitfalls

- **Frontend build uses `npx vite build`, not `npm run build`** — the latter runs `tsc` first which fails.
- **The GeoJSON is cached server-side** (`_geojson_cache` in `geojson.py`). App must be redeployed to refresh.
- **Overture Maps S3 paths change per release.** Check https://docs.overturemaps.org/getting-data/ for the current release. The path `s3://overturemaps-us-west-2/release/2025-03-02.0/` was NOT valid as of this writing — you must discover the correct path.
- **Overture `places` schema uses nested structs** — `names.primary` for the name, `categories.primary` for category, `geometry` is a Point. Read the Overture docs for exact schema.
- **FIPS from lat/lng:** To assign a county FIPS to each POI, you'll need a spatial join. Options: (a) use H3 hexagons, (b) use Databricks built-in `ST_Contains` with county polygons, (c) use a simple bounding box lookup table.
- **All data must be real.** No synthetic data, no mocks, no placeholder values anywhere.
- **`@turf/intersect` needs `@turf/helpers`** as a peer dependency — install both.

## Start Command

```
Read docs/plans/2026-03-05-enterprise-siting-design.md, then create an implementation plan using superpowers:writing-plans for Phases 1-4. Execute using superpowers:executing-plans.
```
