# Enterprise Store Siting Features — Design Document

**Date:** 2026-03-05
**Status:** Approved
**Constraint:** All data must be real — no synthetic, mock, or placeholder data.

---

## Goal

Transform the county-level growth score explorer into a competitive enterprise store siting tool by adding street-level drill-down, site comparison, cannibalization analysis, AI-powered recommendations, and PDF report generation.

## Top 5 Features

| # | Feature | Phase | Status |
|---|---------|-------|--------|
| 1 | Street-level site selection (competitors, drive-times, traffic) | Build (Phases 1-3) | To implement |
| 2 | Site comparison mode (side-by-side, up to 3 sites) | Build (Phase 4) | To implement |
| 3 | Cannibalization analysis (drive-time overlap detection) | Build (Phase 4) | To implement |
| 4 | County details tab — building permit trends & breakdown | Build (Phase 5) | To implement |
| 5 | AI-powered site recommendation (chat-style, Foundation Model API) | Roadmap | Future |
| 6 | PDF report generation (single site + comparison) | Roadmap | Future |

---

## Architecture Decisions

- **Lazy drill-down:** County map remains the entry point. Street-level data loads on demand when user drills into a county.
- **Data sources:** Overture Maps (free, open) for POIs and road segments. Valhalla or OSRM (open-source) for drive-time isochrones. Zero API costs.
- **No fake data:** All POIs come from Overture Maps (~60M real US places). All road segments are real OpenStreetMap-derived data. All isochrones computed from real road networks.
- **Client-side cannibalization:** Polygon intersection via Turf.js — no backend needed for overlap calculation.

---

## Feature 1: Street-Level Site Selection

### User Flow

1. User views county-level map (existing Analysis or Map tab)
2. Clicks a county → CountyDetail panel opens (existing)
3. New **"Explore Sites ▶"** button at the top of CountyDetail
4. Transitions to **SiteExplorer** — full-screen street-level map of that county:
   - Map zoomed to county bounds with street-level basemap
   - **Competitor pins:** QSR restaurants from Overture Maps as colored markers
   - **Traffic heatmap:** Road segments colored by functional class (highway > arterial > residential)
   - **Pin drop:** User clicks map to place a candidate site pin
   - **Isochrone generation:** Dropping a pin calls routing engine → draws 5/10/15-min drive-time polygons
   - **Stats sidebar:** Competitor count per ring, traffic score, county demographics
5. User can drop up to 3 pins → feeds into comparison mode
6. **"← Back to County"** returns to county view

### Data Architecture

| Layer | Source | Storage | Query Pattern |
|-------|--------|---------|---------------|
| QSR Competitors | Overture Maps `places` | `bronze.overture_places` → `silver.silver_poi_restaurants` | Bounding box query for county |
| Road Traffic | Overture Maps `segments` | `bronze.overture_segments` → `silver.silver_road_segments` | Bounding box query for county |
| Drive-time isochrones | Valhalla (self-hosted) | Computed on demand | `POST /isochrone?lat=X&lng=Y&times=5,10,15` |
| County boundary | Existing GeoJSON | Already loaded in memory | Extract polygon for selected FIPS |

### New API Endpoints

- `GET /api/sites/poi?fips={fips}` — QSR POIs within county bounds
- `GET /api/sites/traffic?fips={fips}` — Road segments with functional class within county
- `POST /api/sites/isochrone` — Body: `{lat, lng, times: [5,10,15]}` → isochrone GeoJSON
- `GET /api/sites/demographics?lat={lat}&lng={lng}&radius={miles}` — Aggregated demographics within radius

### Frontend Components

- `SiteExplorer.tsx` — Main street-level view (map + sidebar)
- `CompetitorLayer.ts` — deck.gl ScatterplotLayer for POI pins
- `TrafficLayer.ts` — deck.gl PathLayer for road segments colored by class
- `IsochroneLayer.ts` — deck.gl GeoJsonLayer for drive-time polygons
- `SiteStats.tsx` — Sidebar showing stats for dropped pin location

### Routing Engine

Valhalla or OSRM, self-hosted. Options:
- Valhalla as a subprocess/sidecar in the Databricks App container (uses OSM PBF data, ~8GB for US)
- Lightweight OSRM instance
- Fallback: OpenRouteService free API (2,000 req/day — sufficient for demo)

---

## Feature 2: Site Comparison Mode

### User Flow

1. From SiteExplorer, user drops a pin → **"+ Add to Comparison"** button appears
2. Saves candidate to comparison tray (up to 3 candidates, React state)
3. **Comparison tray** at bottom of SiteExplorer shows thumbnail cards
4. **"Compare Sites"** opens **ComparisonView** — side-by-side layout:

```
┌──────────────┬──────────────┬──────────────┐
│   Site A     │   Site B     │   Site C     │
│  [mini map]  │  [mini map]  │  [mini map]  │
│  isochrone   │  isochrone   │  isochrone   │
├──────────────┼──────────────┼──────────────┤
│ Score: 82    │ Score: 74    │ Score: 68    │
│ Competitors  │ Competitors  │ Competitors  │
│  5-min: 3    │  5-min: 1    │  5-min: 5    │
│ 10-min: 8    │ 10-min: 4    │ 10-min: 12   │
│ Traffic: High│ Traffic: Med │ Traffic: High│
│ Pop (10min): │ Pop (10min): │ Pop (10min): │
│   45,200     │   28,100     │   61,300     │
│ Med Income:  │ Med Income:  │ Med Income:  │
│   $62,400    │   $71,200    │   $48,900    │
├──────────────┼──────────────┼──────────────┤
│ Cannibalize: │ Cannibalize: │ Cannibalize: │
│   None       │   None       │  Site A (40%)│
└──────────────┴──────────────┴──────────────┘
```

### Data Model

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
    trafficScore: number;   // 0-100 based on road functional class
  };
}
```

### Components

- `ComparisonTray.tsx` — Bottom bar with saved candidate thumbnails
- `ComparisonView.tsx` — Side-by-side comparison layout
- `SiteMiniMap.tsx` — Small static map per comparison column

---

## Feature 3: Cannibalization Analysis

Integrated into the comparison view. When comparing multiple candidate sites (or candidates vs existing stores):

- Compute drive-time polygon intersection using **Turf.js** (`@turf/intersect` + `@turf/area`)
- Calculate overlap area as percentage of each site's trade area
- Display warning: "40% trade area overlap with Site A"
- Visual: Overlapping isochrone regions highlighted in red on the comparison mini-maps

All computed client-side — no backend needed.

### Component

- `CannibalizationOverlay.ts` — Turf.js polygon intersection + overlap % calculation

---

## Feature 4: County Details Tab — Building Permit Data

Add a dedicated **Details tab** to the county detail panel that shows granular building permit data for the selected county.

### Data Available

From `silver.silver_building_permits` (quarterly, per county):
- `total_units_permitted` — total residential units permitted
- `single_family_units` — single-family units permitted
- `multi_family_units` — multi-family units permitted
- `report_year`, `report_quarter` — time dimensions

### User Flow

1. User clicks a county (Map or Analysis tab) → CountyDetail panel opens
2. Panel gets a new **"Details"** sub-tab (alongside existing summary view)
3. Details tab shows:
   - **Quarterly permits trend chart** — bar/line chart of total units permitted over time
   - **Unit type breakdown** — stacked bar or donut: single-family vs multi-family split
   - **YoY change** — current quarter vs same quarter prior year (% change)
   - **Raw data table** — scrollable table of quarterly permit records

### New API Endpoint

- `GET /api/counties/{fips}/permits` — Returns all quarterly building permit records for a county (from `silver_building_permits` via Lakebase synced table)

### Frontend Components

- `CountyDetailsTab.tsx` — Tab container with permit charts and table
- `PermitTrendChart.tsx` — Time series chart (quarterly bars)
- `PermitBreakdown.tsx` — Single-family vs multi-family breakdown visual

### Implementation Notes

- Query the Lakebase `synced_silver_building_permits` table (or add a new synced table if not already synced)
- Chart library: reuse whatever is already in the frontend (likely inline SVG or a lightweight lib)
- No new data ingestion needed — data already exists in silver

---

## Roadmap Features (Not Implementing Now)

### Feature 4: AI-Powered Site Recommendation (Roadmap)

Chat-style interface where users type natural language queries. Powered by Databricks Foundation Model API with tool calls against existing data. Returns ranked recommendations pinned on the map.

**Key components (future):**
- `POST /api/chat` endpoint with tool definitions mapping to existing queries
- `ChatDrawer.tsx` — slide-out chat panel
- Tools: `search_counties`, `find_competitors`, `calculate_isochrone`, `get_demographics`, `rank_sites`
- System prompt with scoring methodology and geographic coverage
- Max 5 tool calls per turn, 30s timeout, read-only tools only

### Feature 5: PDF Report Generation (Roadmap)

Server-side PDF generation using `reportlab`. Single site and comparison report formats. Map screenshots via `staticmap` or frontend `html2canvas`. AI summary via Foundation Model API.

**Key components (future):**
- `POST /api/reports/generate` endpoint
- `src/app/backend/reports/pdf_builder.py`
- `ReportButton.tsx` trigger component

---

## Data Pipeline: Overture Maps Ingestion

### New Ingestion Job

One new DABs job: `ingest-overture-maps`

**Source:** Overture Maps Foundation data on S3 (public, Parquet format)
- `s3://overturemaps-us-west-2/release/...` (latest release)
- `places` theme — ~60M POIs in the US
- `transportation/segment` theme — road network with functional class

**Bronze tables:**
- `bronze.overture_places` — Raw Overture places data, filtered to US
- `bronze.overture_segments` — Raw road segments, filtered to US

**Silver transforms:**
- `silver.silver_poi_restaurants` — Filtered to QSR-relevant categories (NAICS 7222xx, Overture category `restaurant`, `fast_food`). Columns: `id`, `lat`, `lng`, `name`, `brand`, `category`, `fips` (derived from lat/lng → county lookup)
- `silver.silver_road_segments` — With `traffic_class` score (1-5 based on functional class: motorway=5, trunk=4, primary=3, secondary=2, residential=1). Columns: `id`, `geometry` (LineString), `functional_class`, `traffic_class`, `fips`

### Size Estimates

- Overture Places (US restaurants/retail): ~2-3 GB Parquet → ~500MB in Delta after filtering
- Overture Segments (US roads): ~8-10 GB Parquet → ~2GB in Delta after filtering
- Total new storage: ~2.5 GB in Unity Catalog

---

## New Dependencies

### Frontend (npm)

| Package | Size (gzipped) | Purpose |
|---------|---------------|---------|
| `@turf/intersect` | ~8KB | Polygon overlap for cannibalization |
| `@turf/area` | ~3KB | Area calculation for overlap percentage |

### Backend (Python)

No new Python dependencies for Phases 1-4. Routing engine is external.

### Infrastructure

- Valhalla or OSRM instance for isochrone computation
- US OSM PBF data (~8GB) for the routing engine

---

## Implementation Phases

| Phase | Scope | Builds On |
|-------|-------|-----------|
| **1** | Overture Maps ingestion pipeline (places + segments → bronze → silver) | Existing medallion pipeline |
| **2** | SiteExplorer view + competitor pins + traffic layer | Phase 1 data |
| **3** | Isochrone integration (routing engine + drive-time polygons) | Phase 2 view |
| **4** | Site comparison + cannibalization analysis | Phase 2-3 (pins + isochrones) |

Roadmap (future):
| **5** | AI chat with tool calls (Foundation Model API) | All data layers as tools |
| **6** | PDF report generation | All views and data |

---

## What We're NOT Building

- No user accounts or authentication (demo app)
- No server-side session persistence (chat history in React state)
- No real-time traffic data (Overture functional class is sufficient proxy)
- No tract-level scoring pipeline (county scores = entry, street-level = drill-down)
- No mobile responsive layout (desktop demo)
- No fake/synthetic/mock data anywhere — all real data from Overture Maps, OSM, Census
