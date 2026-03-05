# Analysis Tab Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix component score bugs (QSR always 0, missing data shows as 0), then add an Analysis tab with multi-filter search panel, highlight/dim map, and sortable results list.

**Architecture:** Client-side filtering on the already-loaded GeoJSON (~3,209 counties). Enrich GeoJSON with additional metrics from gold tables. New `AnalysisView` component with filter panel, reused map component, and results table. Tab navigation in the header switches between Map and Analysis views.

**Tech Stack:** React 18, TypeScript, deck.gl MapboxOverlay, MapLibre GL, Vite. No new dependencies.

---

## Phase 1: Bug Fixes

### Task 1: Remove QSR from Scoring Weights

The `silver_business_patterns` table exists but `qsr_establishments` is NULL for all counties. Rather than fix ingestion for a 5% weight indicator, remove it and redistribute.

**Files:**
- Modify: `src/common/config.py`
- Modify: `src/gold/compute_scores.py`
- Modify: `src/app/frontend/src/components/CountyDetail.tsx`
- Modify: `src/app/frontend/src/components/WeightTuner.tsx`
- Modify: `src/app/frontend/src/api/client.ts`
- Modify: `src/app/backend/models/county.py`
- Modify: `src/app/backend/routes/scoring.py`

**Step 1: Update scoring weights in config**

In `src/common/config.py`, change `DEFAULT_WEIGHTS` to remove `qsr_density_inv` and redistribute its 5%:

```python
DEFAULT_WEIGHTS = {
    "building_permits": 0.25,
    "net_migration": 0.20,
    "vacancy_change": 0.15,
    "employment_growth": 0.15,
    "school_enrollment_growth": 0.10,
    "ssp_projected_growth": 0.15,
}
```

**Step 2: Remove QSR from gold scoring**

In `src/gold/compute_scores.py`, in `score_counties()`, remove the `"qsr_density_inv": "qsr_establishments"` entry from `indicator_cols` dict and remove `"qsr_density_inv"` from the `inverted` set.

**Step 3: Remove QSR from frontend**

In `src/app/frontend/src/components/CountyDetail.tsx`, remove the `qsr_density_inv: "QSR White Space"` entry from `INDICATOR_LABELS`.

In `src/app/frontend/src/components/WeightTuner.tsx`, remove the `qsr_density_inv: "QSR White Space"` entry from the labels map.

In `src/app/frontend/src/api/client.ts`, remove `qsr_density_inv` from the `ComponentScores` interface.

In `src/app/backend/models/county.py`, remove `qsr_density_inv` from the `ComponentScores` model.

In `src/app/backend/routes/scoring.py`, remove `"qsr_density_inv"` from the indicators list.

**Step 4: Commit**

```bash
git add src/common/config.py src/gold/compute_scores.py src/app/frontend/src/components/CountyDetail.tsx src/app/frontend/src/components/WeightTuner.tsx src/app/frontend/src/api/client.ts src/app/backend/models/county.py src/app/backend/routes/scoring.py
git commit -m "fix: remove QSR indicator (no data available), redistribute weight to SSP projections"
```

---

### Task 2: Show "No data" Instead of 0 for Missing Component Scores

Currently the component_scores struct writes 0 for NULL indicators, making the radar chart and bar chart misleading.

**Files:**
- Modify: `src/gold/compute_scores.py`
- Modify: `src/app/frontend/src/components/CountyDetail.tsx`

**Step 1: Pass NULL through in component_scores struct**

In `src/gold/compute_scores.py`, find the line that builds the `component_scores` struct (around line 248):

```python
    df = df.withColumn("component_scores", struct(
        *[coalesce(col(f"_norm_{name}"), lit(0.0)).alias(name) for name in w]
    ))
```

Change to:

```python
    df = df.withColumn("component_scores", struct(
        *[col(f"_norm_{name}").alias(name) for name in w]
    ))
```

This preserves NULL instead of coercing to 0.

**Step 2: Handle null scores in the frontend**

In `src/app/frontend/src/components/CountyDetail.tsx`, update the `radarData` mapping to distinguish null from 0:

```typescript
  const radarData = Object.entries(INDICATOR_LABELS).map(([key, label]) => {
    const raw = componentScores[key];
    const hasData = raw != null && raw !== undefined;
    return {
      indicator: label,
      value: hasData ? Math.round(raw * 100) : 0,
      hasData,
    };
  });
```

Then in the component bars section, show "N/A" for missing data:

```tsx
      <div className="component-bars">
        {radarData.map((d) => (
          <div key={d.indicator} className="bar-row">
            <span className="bar-label">{d.indicator}</span>
            <div className="bar-track">
              {d.hasData ? (
                <div
                  className="bar-fill"
                  style={{ width: `${d.value}%`, backgroundColor: tierColor }}
                />
              ) : (
                <div className="bar-fill bar-fill-na" style={{ width: "100%" }} />
              )}
            </div>
            <span className="bar-value">{d.hasData ? d.value : "N/A"}</span>
          </div>
        ))}
      </div>
```

**Step 3: Add CSS for N/A bar style**

In `src/app/frontend/src/App.css`, add:

```css
.bar-fill-na {
  background: repeating-linear-gradient(
    45deg, #e5e7eb, #e5e7eb 4px, #d1d5db 4px, #d1d5db 8px
  ) !important;
  opacity: 0.5;
}
```

**Step 4: Commit**

```bash
git add src/gold/compute_scores.py src/app/frontend/src/components/CountyDetail.tsx src/app/frontend/src/App.css
git commit -m "fix: show N/A for missing component scores instead of misleading 0"
```

---

### Task 3: Re-run Gold Scoring and Redeploy

**Step 1: Deploy bundle with updated gold scoring code**

```bash
databricks bundle deploy --profile=DEFAULT
```

**Step 2: Re-run the gold scoring job**

```bash
databricks jobs run-now <gold_job_id> --profile=DEFAULT
```

Job ID: `196681278666944`

**Step 3: Rebuild frontend and deploy app**

```bash
cd src/app/frontend && npx vite build
databricks bundle deploy --profile=DEFAULT
databricks apps deploy store-siting --source-code-path /Workspace/Users/jesus.rodriguez@databricks.com/.bundle/store-siting/dev/files/src/app --profile=DEFAULT
```

**Step 4: Verify via Chrome DevTools**

- Reload the app
- Click a county — component scores should show N/A for missing indicators
- QSR White Space should no longer appear
- SSP Projections weight should be 15% (up from 10%)

**Step 5: Commit any remaining changes**

```bash
git add -A && git commit -m "chore: rebuild frontend with bug fixes"
```

---

## Phase 2: GeoJSON Enrichment

### Task 4: Add Metric Properties to GeoJSON Endpoint

The `/api/geojson` endpoint currently only includes `composite_score`, `score_tier`, `rank_national`, `population`. The Analysis tab needs 7 additional metrics for client-side filtering.

**Files:**
- Modify: `src/app/backend/routes/geojson.py`

**Step 1: Update the GeoJSON query to join gold_county_details**

Replace the entire `get_geojson` function in `src/app/backend/routes/geojson.py`:

```python
@router.get("/geojson")
def get_geojson():
    global _geojson_cache

    catalog = os.environ.get("CATALOG", "store_siting")
    scores = execute_query(
        f"""SELECT s.fips, s.composite_score, s.score_tier, s.rank_national,
                   s.population, s.median_income, s.ssp_growth_rate,
                   d.permits_per_1k_pop, d.net_migration_rate,
                   d.occupancy_rate, d.employment_per_capita
            FROM {catalog}.gold.gold_county_growth_score s
            LEFT JOIN {catalog}.gold.gold_county_details d ON s.fips = d.fips"""
    )
    score_lookup = {r["fips"]: r for r in scores}

    if _geojson_cache is None:
        geojson_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "us-counties.json"
        )
        if not os.path.exists(geojson_path):
            geojson_path = os.path.join(
                os.path.dirname(__file__), "..", "..", "..", "..", "data", "county_geojson", "us-counties.json"
            )
        with open(geojson_path) as f:
            _geojson_cache = json.load(f)

    for feature in _geojson_cache.get("features", []):
        fips = feature.get("properties", {}).get("GEOID", "")
        if fips in score_lookup:
            feature["properties"].update(score_lookup[fips])

    return _geojson_cache
```

**Step 2: Commit**

```bash
git add src/app/backend/routes/geojson.py
git commit -m "feat: enrich GeoJSON with additional metrics for analysis filtering"
```

---

## Phase 3: Tab Navigation

### Task 5: Add Tab Navigation to App Shell

**Files:**
- Modify: `src/app/frontend/src/App.tsx`
- Modify: `src/app/frontend/src/App.css`

**Step 1: Add tab state and navigation to App.tsx**

Replace the entire `src/app/frontend/src/App.tsx`:

```tsx
import { useState, useEffect } from "react";
import { NationalMap } from "./components/NationalMap";
import { CountyDetail } from "./components/CountyDetail";
import { WeightTuner } from "./components/WeightTuner";
import { AnalysisView } from "./components/AnalysisView";
import { api } from "./api/client";
import "./App.css";

type Tab = "map" | "analysis";

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("map");
  const [selectedFips, setSelectedFips] = useState<string | null>(null);
  const [showWeightTuner, setShowWeightTuner] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [geojson, setGeojson] = useState<any | null>(null);

  useEffect(() => {
    api.getGeoJson().then(setGeojson);
  }, [refreshKey]);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Store Siting — Growth Score Explorer</h1>
        <nav className="tab-nav">
          <button
            className={`tab-btn ${activeTab === "map" ? "active" : ""}`}
            onClick={() => setActiveTab("map")}
          >
            Map
          </button>
          <button
            className={`tab-btn ${activeTab === "analysis" ? "active" : ""}`}
            onClick={() => setActiveTab("analysis")}
          >
            Analysis
          </button>
        </nav>
        <button onClick={() => setShowWeightTuner(!showWeightTuner)}>
          {showWeightTuner ? "Hide Weights" : "Adjust Weights"}
        </button>
      </header>

      <div className="app-body">
        {activeTab === "map" && (
          <>
            <div className="map-container">
              <NationalMap
                geojson={geojson}
                onSelectCounty={setSelectedFips}
              />
            </div>
            {selectedFips && (
              <div className="detail-panel">
                <CountyDetail
                  fips={selectedFips}
                  onClose={() => setSelectedFips(null)}
                />
              </div>
            )}
          </>
        )}

        {activeTab === "analysis" && (
          <AnalysisView
            geojson={geojson}
            onSelectCounty={setSelectedFips}
            selectedFips={selectedFips}
            onCloseDetail={() => setSelectedFips(null)}
          />
        )}

        {showWeightTuner && (
          <div className="weight-panel">
            <WeightTuner onRecalculate={() => setRefreshKey((k) => k + 1)} />
          </div>
        )}
      </div>
    </div>
  );
}
```

**Step 2: Update NationalMap to accept geojson as prop**

In `src/app/frontend/src/components/NationalMap.tsx`, change the Props interface and remove the internal fetch:

```typescript
interface Props {
  geojson: any | null;
  onSelectCounty: (fips: string) => void;
}
```

Remove the `refreshKey` prop, remove the `const [geojson, setGeojson] = useState` and the `useEffect` that fetches geojson. Use `props.geojson` directly instead.

**Step 3: Add tab navigation CSS**

In `src/app/frontend/src/App.css`, add after the `.app-header button` block:

```css
.tab-nav {
  display: flex;
  gap: 4px;
  background: rgba(255,255,255,0.1);
  padding: 3px;
  border-radius: 8px;
}

.tab-btn {
  padding: 6px 20px !important;
  border: none !important;
  background: transparent !important;
  color: rgba(255,255,255,0.6) !important;
  border-radius: 6px !important;
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
  transition: all 0.15s;
}

.tab-btn.active {
  background: rgba(255,255,255,0.2) !important;
  color: white !important;
}
```

**Step 4: Commit**

```bash
git add src/app/frontend/src/App.tsx src/app/frontend/src/App.css src/app/frontend/src/components/NationalMap.tsx
git commit -m "feat: add tab navigation (Map / Analysis) to app header"
```

---

## Phase 4: Analysis View

### Task 6: Create Filter Types and Utility

**Files:**
- Create: `src/app/frontend/src/components/analysisFilters.ts`

**Step 1: Create the filter types and filtering function**

```typescript
// src/app/frontend/src/components/analysisFilters.ts

export interface Filters {
  score: [number, number];
  tiers: string[];
  states: string[];
  population: [number, number];
  median_income: [number, number];
  permits_per_1k_pop: [number, number];
  net_migration_rate: [number, number];
  occupancy_rate: [number, number];
  employment_per_capita: [number, number];
  ssp_growth_rate: [number, number];
}

export interface DataBounds {
  population: [number, number];
  median_income: [number, number];
  permits_per_1k_pop: [number, number];
  net_migration_rate: [number, number];
  occupancy_rate: [number, number];
  employment_per_capita: [number, number];
  ssp_growth_rate: [number, number];
}

export function computeBounds(features: any[]): DataBounds {
  const vals = (key: string) => features
    .map(f => f.properties?.[key])
    .filter(v => v != null && isFinite(v));

  const range = (arr: number[]): [number, number] =>
    arr.length ? [Math.min(...arr), Math.max(...arr)] : [0, 0];

  return {
    population: range(vals("population")),
    median_income: range(vals("median_income")),
    permits_per_1k_pop: range(vals("permits_per_1k_pop")),
    net_migration_rate: range(vals("net_migration_rate")),
    occupancy_rate: range(vals("occupancy_rate")),
    employment_per_capita: range(vals("employment_per_capita")),
    ssp_growth_rate: range(vals("ssp_growth_rate")),
  };
}

export function defaultFilters(bounds: DataBounds): Filters {
  return {
    score: [0, 100],
    tiers: ["A", "B", "C", "D", "F"],
    states: [],
    population: bounds.population,
    median_income: bounds.median_income,
    permits_per_1k_pop: bounds.permits_per_1k_pop,
    net_migration_rate: bounds.net_migration_rate,
    occupancy_rate: bounds.occupancy_rate,
    employment_per_capita: bounds.employment_per_capita,
    ssp_growth_rate: bounds.ssp_growth_rate,
  };
}

function inRange(val: any, range: [number, number]): boolean {
  if (val == null || !isFinite(val)) return true; // don't exclude missing data
  return val >= range[0] && val <= range[1];
}

export function filterFeatures(features: any[], filters: Filters): Set<string> {
  const matching = new Set<string>();
  for (const f of features) {
    const p = f.properties || {};
    const fips = p.GEOID;
    if (!fips) continue;

    if (!inRange(p.composite_score, filters.score)) continue;
    if (filters.tiers.length < 5 && !filters.tiers.includes(p.score_tier)) continue;
    if (filters.states.length > 0 && !filters.states.includes(p.STATE)) continue;
    if (!inRange(p.population, filters.population)) continue;
    if (!inRange(p.median_income, filters.median_income)) continue;
    if (!inRange(p.permits_per_1k_pop, filters.permits_per_1k_pop)) continue;
    if (!inRange(p.net_migration_rate, filters.net_migration_rate)) continue;
    if (!inRange(p.occupancy_rate, filters.occupancy_rate)) continue;
    if (!inRange(p.employment_per_capita, filters.employment_per_capita)) continue;
    if (!inRange(p.ssp_growth_rate, filters.ssp_growth_rate)) continue;

    matching.add(fips);
  }
  return matching;
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/analysisFilters.ts
git commit -m "feat: add analysis filter types and client-side filtering logic"
```

---

### Task 7: Create FilterPanel Component

**Files:**
- Create: `src/app/frontend/src/components/FilterPanel.tsx`

**Step 1: Create the filter panel with range sliders, tier checkboxes, state dropdown**

```tsx
// src/app/frontend/src/components/FilterPanel.tsx
import { useMemo } from "react";
import { Filters, DataBounds, defaultFilters } from "./analysisFilters";

const STATE_FIPS: Record<string, string> = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
  "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
  "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
  "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
  "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
  "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
  "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
  "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
  "56": "WY",
};

interface Props {
  filters: Filters;
  bounds: DataBounds;
  matchCount: number;
  totalCount: number;
  states: string[];
  onChange: (f: Filters) => void;
}

function RangeSlider({ label, value, min, max, step, fmt, onChange }: {
  label: string;
  value: [number, number];
  min: number;
  max: number;
  step?: number;
  fmt?: (v: number) => string;
  onChange: (v: [number, number]) => void;
}) {
  const format = fmt || ((v: number) => v.toLocaleString());
  const s = step || (max - min > 100 ? Math.round((max - min) / 100) : 0.01);
  return (
    <div className="filter-group">
      <label className="filter-label">
        {label}
        <span className="filter-range-text">{format(value[0])} — {format(value[1])}</span>
      </label>
      <div className="dual-range">
        <input type="range" min={min} max={max} step={s} value={value[0]}
          onChange={e => onChange([Math.min(Number(e.target.value), value[1]), value[1]])} />
        <input type="range" min={min} max={max} step={s} value={value[1]}
          onChange={e => onChange([value[0], Math.max(Number(e.target.value), value[0])])} />
      </div>
    </div>
  );
}

export function FilterPanel({ filters, bounds, matchCount, totalCount, states, onChange }: Props) {
  const stateEntries = useMemo(() =>
    states.map(s => ({ fips: s, abbr: STATE_FIPS[s] || s }))
      .sort((a, b) => a.abbr.localeCompare(b.abbr)),
    [states]
  );

  const setFilter = <K extends keyof Filters>(key: K, val: Filters[K]) =>
    onChange({ ...filters, [key]: val });

  const reset = () => onChange(defaultFilters(bounds));

  const fmtDollar = (v: number) => `$${Math.round(v).toLocaleString()}`;
  const fmtPct = (v: number) => `${(v * 100).toFixed(1)}%`;
  const fmtDec = (v: number) => v.toFixed(2);

  return (
    <div className="filter-panel">
      <div className="filter-header">
        <span className="match-count">
          <strong>{matchCount.toLocaleString()}</strong> of {totalCount.toLocaleString()} counties
        </span>
        <button className="reset-btn" onClick={reset}>Reset</button>
      </div>

      <RangeSlider label="Composite Score" value={filters.score}
        min={0} max={100} step={1} fmt={v => v.toFixed(0)} onChange={v => setFilter("score", v)} />

      <div className="filter-group">
        <label className="filter-label">Tier</label>
        <div className="tier-checkboxes">
          {["A", "B", "C", "D", "F"].map(t => (
            <label key={t} className="tier-check">
              <input type="checkbox" checked={filters.tiers.includes(t)}
                onChange={e => {
                  const next = e.target.checked
                    ? [...filters.tiers, t]
                    : filters.tiers.filter(x => x !== t);
                  setFilter("tiers", next);
                }} />
              {t}
            </label>
          ))}
        </div>
      </div>

      <div className="filter-group">
        <label className="filter-label">State</label>
        <select multiple className="state-multi-select" value={filters.states}
          onChange={e => setFilter("states",
            Array.from(e.target.selectedOptions, o => o.value)
          )}>
          {stateEntries.map(s => (
            <option key={s.fips} value={s.fips}>{s.abbr}</option>
          ))}
        </select>
        {filters.states.length > 0 && (
          <button className="clear-states" onClick={() => setFilter("states", [])}>
            Clear ({filters.states.length})
          </button>
        )}
      </div>

      <RangeSlider label="Population" value={filters.population}
        min={bounds.population[0]} max={bounds.population[1]}
        fmt={v => v.toLocaleString()} onChange={v => setFilter("population", v)} />

      <RangeSlider label="Median Income" value={filters.median_income}
        min={bounds.median_income[0]} max={bounds.median_income[1]}
        fmt={fmtDollar} onChange={v => setFilter("median_income", v)} />

      <RangeSlider label="Permits / 1K Pop" value={filters.permits_per_1k_pop}
        min={bounds.permits_per_1k_pop[0]} max={bounds.permits_per_1k_pop[1]}
        fmt={fmtDec} onChange={v => setFilter("permits_per_1k_pop", v)} />

      <RangeSlider label="Net Migration Rate" value={filters.net_migration_rate}
        min={bounds.net_migration_rate[0]} max={bounds.net_migration_rate[1]}
        fmt={fmtDec} onChange={v => setFilter("net_migration_rate", v)} />

      <RangeSlider label="Occupancy Rate" value={filters.occupancy_rate}
        min={bounds.occupancy_rate[0]} max={bounds.occupancy_rate[1]}
        fmt={fmtPct} onChange={v => setFilter("occupancy_rate", v)} />

      <RangeSlider label="Employment / Capita" value={filters.employment_per_capita}
        min={bounds.employment_per_capita[0]} max={bounds.employment_per_capita[1]}
        fmt={fmtDec} onChange={v => setFilter("employment_per_capita", v)} />

      <RangeSlider label="SSP Growth Rate" value={filters.ssp_growth_rate}
        min={bounds.ssp_growth_rate[0]} max={bounds.ssp_growth_rate[1]}
        fmt={fmtPct} onChange={v => setFilter("ssp_growth_rate", v)} />
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/FilterPanel.tsx
git commit -m "feat: add FilterPanel component with range sliders, tier checkboxes, state select"
```

---

### Task 8: Create ResultsList Component

**Files:**
- Create: `src/app/frontend/src/components/ResultsList.tsx`

**Step 1: Create the sortable results list**

```tsx
// src/app/frontend/src/components/ResultsList.tsx
import { useState, useMemo } from "react";

const STATE_FIPS: Record<string, string> = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
  "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
  "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
  "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
  "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
  "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
  "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
  "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
  "56": "WY",
};

const TIER_DOT: Record<string, string> = {
  A: "#15803d", B: "#4aaf64", C: "#e6a817", D: "#e07020", F: "#c41e3a",
};

type SortKey = "rank" | "name" | "state" | "score" | "tier" | "population" | "income";
type SortDir = "asc" | "desc";

interface Props {
  features: any[];
  matchingFips: Set<string>;
  onSelect: (fips: string) => void;
}

export function ResultsList({ features, matchingFips, onSelect }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("score");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const rows = useMemo(() => {
    const matched = features
      .filter(f => matchingFips.has(f.properties?.GEOID))
      .map(f => f.properties);

    matched.sort((a, b) => {
      let va: any, vb: any;
      switch (sortKey) {
        case "rank": va = a.rank_national; vb = b.rank_national; break;
        case "name": va = a.NAME || ""; vb = b.NAME || ""; break;
        case "state": va = STATE_FIPS[a.STATE] || ""; vb = STATE_FIPS[b.STATE] || ""; break;
        case "score": va = a.composite_score || 0; vb = b.composite_score || 0; break;
        case "tier": va = a.score_tier || "Z"; vb = b.score_tier || "Z"; break;
        case "population": va = a.population || 0; vb = b.population || 0; break;
        case "income": va = a.median_income || 0; vb = b.median_income || 0; break;
      }
      if (typeof va === "string") {
        const cmp = va.localeCompare(vb);
        return sortDir === "asc" ? cmp : -cmp;
      }
      return sortDir === "asc" ? va - vb : vb - va;
    });

    return matched;
  }, [features, matchingFips, sortKey, sortDir]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir(key === "name" || key === "state" ? "asc" : "desc");
    }
  };

  const arrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  return (
    <div className="results-list">
      <table className="results-table">
        <thead>
          <tr>
            <th onClick={() => toggleSort("rank")}>#{ arrow("rank")}</th>
            <th onClick={() => toggleSort("name")}>County{arrow("name")}</th>
            <th onClick={() => toggleSort("state")}>ST{arrow("state")}</th>
            <th onClick={() => toggleSort("score")}>Score{arrow("score")}</th>
            <th onClick={() => toggleSort("tier")}>Tier{arrow("tier")}</th>
            <th onClick={() => toggleSort("population")}>Pop.{arrow("population")}</th>
            <th onClick={() => toggleSort("income")}>Income{arrow("income")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p, i) => (
            <tr key={p.GEOID} onClick={() => onSelect(p.GEOID)} className="results-row">
              <td className="col-rank">{p.rank_national}</td>
              <td className="col-name">{p.NAME || p.GEOID}</td>
              <td className="col-state">{STATE_FIPS[p.STATE] || p.STATE}</td>
              <td className="col-score">{p.composite_score?.toFixed(1)}</td>
              <td className="col-tier">
                <span className="tier-dot" style={{ background: TIER_DOT[p.score_tier] || "#999" }} />
                {p.score_tier}
              </td>
              <td className="col-pop">{p.population?.toLocaleString()}</td>
              <td className="col-income">{p.median_income ? `$${Math.round(p.median_income).toLocaleString()}` : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/ResultsList.tsx
git commit -m "feat: add sortable ResultsList component for analysis tab"
```

---

### Task 9: Create AnalysisView Component

**Files:**
- Create: `src/app/frontend/src/components/AnalysisView.tsx`

**Step 1: Create the main analysis view that combines filter panel, map, and results**

```tsx
// src/app/frontend/src/components/AnalysisView.tsx
import { useState, useMemo, useCallback } from "react";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoJsonLayer } from "@deck.gl/layers";
import MapGL, { useControl, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { FilterPanel } from "./FilterPanel";
import { ResultsList } from "./ResultsList";
import { CountyDetail } from "./CountyDetail";
import { Filters, DataBounds, computeBounds, defaultFilters, filterFeatures } from "./analysisFilters";

const INITIAL_VIEW = {
  longitude: -98.5, latitude: 39.8, zoom: 4, pitch: 0, bearing: 0,
};

const TIER_COLORS: Record<string, [number, number, number, number]> = {
  A: [21, 128, 61, 220],
  B: [74, 175, 100, 200],
  C: [230, 168, 23, 200],
  D: [224, 112, 32, 200],
  F: [196, 30, 58, 180],
};

const DIM_COLOR: [number, number, number, number] = [200, 200, 200, 50];

function DeckOverlay(props: { layers: any[] }) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  overlay.setProps({ layers: props.layers });
  return null;
}

interface Props {
  geojson: any | null;
  onSelectCounty: (fips: string) => void;
  selectedFips: string | null;
  onCloseDetail: () => void;
}

export function AnalysisView({ geojson, onSelectCounty, selectedFips, onCloseDetail }: Props) {
  const features = geojson?.features || [];

  const bounds = useMemo<DataBounds>(() => computeBounds(features), [features]);
  const [filters, setFilters] = useState<Filters>(() => defaultFilters(bounds));

  const states = useMemo(() => {
    const seen = new Set<string>();
    for (const f of features) {
      const st = f.properties?.STATE;
      if (st) seen.add(st);
    }
    return Array.from(seen).sort();
  }, [features]);

  const matchingFips = useMemo(() => filterFeatures(features, filters), [features, filters]);

  const [hovered, setHovered] = useState<any>(null);

  const layers = useMemo(() => {
    if (!geojson) return [];
    return [
      new GeoJsonLayer({
        id: "analysis-counties",
        data: geojson,
        filled: true,
        stroked: true,
        getLineColor: [100, 100, 100, 80],
        lineWidthMinPixels: 0.5,
        getFillColor: (f: any) => {
          const fips = f.properties?.GEOID;
          if (!fips || !matchingFips.has(fips)) return DIM_COLOR;
          const tier = f.properties?.score_tier || "F";
          return TIER_COLORS[tier] || TIER_COLORS.F;
        },
        pickable: true,
        onHover: (info: any) => setHovered(info.object ? info : null),
        onClick: (info: any) => {
          const fips = info.object?.properties?.GEOID;
          if (fips) onSelectCounty(fips);
        },
        updateTriggers: {
          getFillColor: [matchingFips],
        },
      }),
    ];
  }, [geojson, matchingFips, onSelectCounty]);

  if (!geojson) return <div className="loading">Loading data...</div>;

  return (
    <div className="analysis-view">
      <FilterPanel
        filters={filters}
        bounds={bounds}
        matchCount={matchingFips.size}
        totalCount={features.length}
        states={states}
        onChange={setFilters}
      />

      <div className="analysis-map">
        <MapGL
          initialViewState={INITIAL_VIEW}
          style={{ width: "100%", height: "100%" }}
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        >
          <DeckOverlay layers={layers} />
          <NavigationControl position="bottom-right" />
        </MapGL>

        {hovered?.object && (
          <div className="tooltip" style={{ left: hovered.x + 10, top: hovered.y + 10 }}>
            <strong>{hovered.object.properties.NAME}</strong><br />
            Score: {hovered.object.properties.composite_score?.toFixed(1) ?? "N/A"}<br />
            Tier: {hovered.object.properties.score_tier ?? "N/A"}
          </div>
        )}
      </div>

      <div className="analysis-right">
        {selectedFips ? (
          <CountyDetail fips={selectedFips} onClose={onCloseDetail} />
        ) : (
          <ResultsList
            features={features}
            matchingFips={matchingFips}
            onSelect={onSelectCounty}
          />
        )}
      </div>
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/AnalysisView.tsx
git commit -m "feat: add AnalysisView combining filter panel, map, and results list"
```

---

### Task 10: Add Analysis CSS Styles

**Files:**
- Modify: `src/app/frontend/src/App.css`

**Step 1: Add all analysis-related styles**

Append to `src/app/frontend/src/App.css`:

```css
/* === Analysis View === */

.analysis-view {
  display: flex;
  flex: 1;
  overflow: hidden;
}

.filter-panel {
  width: 280px;
  background: #f8f9fa;
  border-right: 1px solid #eee;
  overflow-y: auto;
  padding: 16px;
  flex-shrink: 0;
}

.filter-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.match-count { font-size: 14px; color: #333; }

.reset-btn {
  padding: 4px 12px;
  border: 1px solid #ccc;
  background: white;
  border-radius: 4px;
  font-size: 12px;
  cursor: pointer;
}

.filter-group {
  margin-bottom: 14px;
}

.filter-label {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  font-weight: 600;
  color: #555;
  margin-bottom: 4px;
}

.filter-range-text {
  font-weight: 400;
  color: #888;
  font-size: 11px;
}

.dual-range {
  position: relative;
  height: 20px;
}

.dual-range input[type="range"] {
  position: absolute;
  width: 100%;
  top: 0;
  pointer-events: none;
  -webkit-appearance: none;
  appearance: none;
  background: transparent;
  height: 20px;
}

.dual-range input[type="range"]::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: #4A90D9;
  cursor: pointer;
  pointer-events: all;
  border: 2px solid white;
  box-shadow: 0 1px 3px rgba(0,0,0,0.3);
}

.tier-checkboxes {
  display: flex;
  gap: 10px;
}

.tier-check {
  font-size: 13px;
  display: flex;
  align-items: center;
  gap: 3px;
  cursor: pointer;
}

.state-multi-select {
  width: 100%;
  height: 80px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 12px;
  padding: 2px;
}

.clear-states {
  margin-top: 4px;
  padding: 2px 8px;
  border: none;
  background: #e5e7eb;
  border-radius: 3px;
  font-size: 11px;
  cursor: pointer;
}

.analysis-map {
  flex: 1;
  position: relative;
}

.analysis-right {
  width: 400px;
  background: white;
  border-left: 1px solid #eee;
  overflow-y: auto;
  flex-shrink: 0;
}

/* === Results Table === */

.results-list {
  height: 100%;
  overflow-y: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.results-table thead {
  position: sticky;
  top: 0;
  background: #f8f9fa;
  z-index: 1;
}

.results-table th {
  padding: 8px 6px;
  text-align: left;
  font-weight: 600;
  color: #555;
  border-bottom: 2px solid #ddd;
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}

.results-table th:hover { color: #1a1a2e; }

.results-row {
  cursor: pointer;
  transition: background 0.1s;
}

.results-row:hover { background: #f0f4ff; }

.results-row td {
  padding: 6px;
  border-bottom: 1px solid #f0f0f0;
}

.col-rank { width: 36px; color: #999; text-align: right; padding-right: 8px; }
.col-name { font-weight: 500; }
.col-state { width: 32px; color: #666; }
.col-score { width: 48px; font-weight: 600; text-align: right; }
.col-tier { width: 40px; }
.col-pop { width: 80px; text-align: right; color: #555; }
.col-income { width: 80px; text-align: right; color: #555; }

.tier-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 4px;
}

/* Give analysis-right panel padding when showing county detail */
.analysis-right .county-detail { padding: 20px; }
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/App.css
git commit -m "feat: add CSS styles for analysis view, filter panel, and results table"
```

---

## Phase 5: Build, Deploy, Verify

### Task 11: Build Frontend, Deploy Bundle, Deploy App, Verify

**Step 1: Build frontend**

```bash
cd src/app/frontend && npx vite build
```

**Step 2: Deploy bundle and app**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/store
databricks bundle deploy --profile=DEFAULT
databricks apps deploy store-siting --source-code-path /Workspace/Users/jesus.rodriguez@databricks.com/.bundle/store-siting/dev/files/src/app --profile=DEFAULT
```

**Step 3: Verify via Chrome DevTools**

- Reload the app
- Verify "Map" and "Analysis" tabs appear in the header
- Click "Analysis" tab
- Verify filter panel on left with all sliders
- Verify map shows colored counties
- Adjust population slider — map should dim non-matching counties
- Check tier checkboxes — uncheck F, verify red counties dim
- Verify results list on right shows matching counties
- Click a row — county detail panel should appear
- Click close — back to results list

**Step 4: Final commit**

```bash
git add -A && git commit -m "feat: complete analysis tab with filters, map, and results list"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-3 | Bug fixes: remove QSR, show N/A for missing data, re-run gold scoring |
| 2 | 4 | Enrich GeoJSON endpoint with additional metrics |
| 3 | 5 | Tab navigation in app shell |
| 4 | 6-10 | Analysis view: filter types, FilterPanel, ResultsList, AnalysisView, CSS |
| 5 | 11 | Build, deploy, verify |

**Total: 11 tasks across 5 phases.**
