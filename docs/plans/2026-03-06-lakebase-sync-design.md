# Lakebase Sync + App Rewire Design

## Overview

Replace the app's SQL Warehouse dependency with a Lakebase (managed PostgreSQL) database. An SDP pipeline syncs gold tables from Unity Catalog to Lakebase, triggered after the gold scoring job completes. The app connects directly to Lakebase via psycopg2 + Databricks SDK for auth.

## Architecture

```
Gold Tables (UC)  -->  SDP Pipeline (triggered)  -->  Lakebase PostgreSQL (autoscale XS->S)  -->  FastAPI App
```

## 1. Lakebase Database

- **Name:** `store_siting_app` (dev: `store_siting_app_dev`)
- **Tier:** Autoscale XS to S
- **Tables mirrored:**
  - `gold_county_growth_score`
  - `gold_county_details`
  - `gold_scoring_config`
- **GRANTs:** `SELECT`, `UPDATE` on all 3 tables for the app service principal (`UPDATE` needed for `gold_scoring_config` weight edits)

## 2. SDP Sync Pipeline

- **Single pipeline resource:** `store-siting-gold-sync`
- **3 streaming tables** reading from UC gold tables via `STREAMING TABLE ... AS SELECT * FROM`
- **Trigger:** Dependent task in `compute_gold_scores` job (runs after scoring completes)
- **DAB resource:** `resources/pipelines/gold_lakebase_sync.yml`
- **Pipeline code:** `src/pipelines/gold_lakebase_sync.py`

## 3. App Rewire

- **Replace `db.py`** with psycopg2 client using Databricks SDK to obtain Lakebase connection params at runtime
- **Remove:** `DATABRICKS_HTTP_PATH` and `warehouse_id` from app.yaml / databricks.yml
- **Add:** `LAKEBASE_DATABASE` env var to app.yaml
- **Routes unchanged** — same SQL queries, minor PostgreSQL syntax adjustments if needed
- **`gold_scoring_config` UPDATE** — use psycopg2 parameterized queries (safer than string interpolation)

## 4. DAB Changes

- `databricks.yml` — add `lakebase_database` variable, remove `warehouse_id`
- `resources/pipelines/gold_lakebase_sync.yml` — new SDP pipeline
- `resources/app.yml` — swap warehouse env vars for Lakebase env vars
- `resources/jobs/compute_gold_scores.yml` — add dependent task to trigger sync pipeline

## 5. Files Changed/Created

| Action | File |
|--------|------|
| Create | `resources/pipelines/gold_lakebase_sync.yml` |
| Create | `src/pipelines/gold_lakebase_sync.py` |
| Rewrite | `src/app/backend/db.py` |
| Edit | `src/app/app.yaml` |
| Edit | `resources/app.yml` |
| Edit | `databricks.yml` |
| Edit | `resources/jobs/compute_gold_scores.yml` |
| Edit | `src/app/backend/routes/scoring.py` |

## Decisions

- **Hard cutover** — no dual-mode/feature flag. Remove all SQL Warehouse references from the app.
- **Single SDP pipeline** for all 3 tables (not per-table pipelines).
- **Autoscale XS to S** — sufficient for ~3,200 rows across 3 tables.
- **Databricks-managed SP auth** — no manual secrets, SDK resolves connection at runtime.
