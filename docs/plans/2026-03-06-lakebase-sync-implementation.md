# Lakebase Sync + App Rewire Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Sync gold tables to a Lakebase PostgreSQL database via an SDP pipeline triggered after gold scoring, then rewire the app to query Lakebase directly instead of SQL Warehouse.

**Architecture:** Gold UC tables → SDP pipeline (triggered) → Lakebase autoscale (XS→S) → FastAPI app via psycopg2 + Databricks SDK auth. All resources defined in DABs.

**Tech Stack:** SDP (`pyspark.pipelines`), Lakebase Autoscaling, psycopg2, Databricks SDK, DABs

---

### Task 1: Create Lakebase Project

**Files:** None (CLI operations only)

**Step 1: Check CLI version**

Run: `databricks --version`
Expected: v0.285.0+ (required for `databricks postgres` commands)

**Step 2: Create the Lakebase project**

```bash
databricks postgres create-project store-siting-app \
  --json '{"spec": {"display_name": "Store Siting App"}}' \
  --no-wait \
  -p DEFAULT
```

This auto-creates a `production` branch with a `primary` read-write endpoint.

**Step 3: Wait for project to be ready**

```bash
# Check endpoint state (should be ACTIVE)
databricks postgres list-endpoints projects/store-siting-app/branches/production \
  -p DEFAULT -o json | jq '.[].status.current_state'
```

Expected: `"ACTIVE"`

**Step 4: Get the endpoint host**

```bash
databricks postgres list-endpoints projects/store-siting-app/branches/production \
  -p DEFAULT -o json | jq -r '.[0].status.hosts.host'
```

Save the host value — needed for app config later.

**Step 5: Create the database**

```bash
HOST=$(databricks postgres list-endpoints projects/store-siting-app/branches/production \
  -p DEFAULT -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential \
  projects/store-siting-app/branches/production/endpoints/primary \
  -p DEFAULT -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p DEFAULT -o json | jq -r '.userName')

PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=postgres user=$EMAIL sslmode=require" \
  -c "CREATE DATABASE store_siting_app;"
```

**Step 6: Create tables matching gold schema**

```bash
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=store_siting_app user=$EMAIL sslmode=require" -c "
CREATE TABLE gold_county_growth_score (
    fips VARCHAR(5) PRIMARY KEY,
    county_name VARCHAR(100),
    state VARCHAR(2),
    report_year INTEGER,
    report_quarter INTEGER,
    population BIGINT,
    median_income DOUBLE PRECISION,
    composite_score DOUBLE PRECISION,
    score_tier VARCHAR(1),
    rank_national INTEGER,
    component_scores JSONB,
    ssp_projected_pop BIGINT,
    ssp_projection_year INTEGER,
    ssp_growth_rate DOUBLE PRECISION
);

CREATE TABLE gold_county_details (
    fips VARCHAR(5) PRIMARY KEY,
    county_name VARCHAR(100),
    state VARCHAR(2),
    report_year INTEGER,
    report_quarter INTEGER,
    population BIGINT,
    median_income DOUBLE PRECISION,
    total_units_permitted INTEGER,
    single_family_units INTEGER,
    multi_family_units INTEGER,
    permits_per_1k_pop DOUBLE PRECISION,
    net_migration DOUBLE PRECISION,
    net_migration_rate DOUBLE PRECISION,
    vacancy_rate DOUBLE PRECISION,
    occupancy_rate DOUBLE PRECISION,
    total_employment BIGINT,
    avg_weekly_wage DOUBLE PRECISION,
    employment_per_capita DOUBLE PRECISION,
    total_enrollment BIGINT,
    enrollment_per_capita DOUBLE PRECISION,
    qsr_establishments INTEGER,
    retail_density DOUBLE PRECISION,
    ssp_projected_pop BIGINT,
    ssp_projection_year INTEGER,
    ssp_growth_rate DOUBLE PRECISION
);

CREATE TABLE gold_scoring_config (
    indicator VARCHAR(50) PRIMARY KEY,
    weight DOUBLE PRECISION NOT NULL
);
"
```

**Step 7: Grant permissions for the app service principal**

```bash
# Get the app's service principal name
# The Databricks App runs as a service principal — check the app config or use:
databricks apps get store-siting -p DEFAULT -o json | jq '.service_principal'

# Grant SELECT + UPDATE on all tables
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=store_siting_app user=$EMAIL sslmode=require" -c "
GRANT CONNECT ON DATABASE store_siting_app TO PUBLIC;
GRANT USAGE ON SCHEMA public TO PUBLIC;
GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE ON TABLES TO PUBLIC;
"
```

Note: In Lakebase, Databricks-managed service principals authenticate via OAuth. The GRANTs above use PUBLIC for simplicity — tighten to the specific SP role if needed in production.

---

### Task 2: Write the SDP Sync Pipeline

**Files:**
- Create: `src/pipelines/gold_lakebase_sync.sql`

**Step 1: Create the pipelines directory**

Run: `mkdir -p src/pipelines`

**Step 2: Write the SDP pipeline SQL**

The SDP pipeline reads from the UC gold tables and writes materialized views that sync to the Lakebase-connected catalog. Since gold tables are batch (overwritten each run), use materialized views (not streaming tables).

```sql
-- src/pipelines/gold_lakebase_sync.sql
-- SDP pipeline to sync Gold tables to Lakebase

CREATE OR REFRESH MATERIALIZED VIEW gold_county_growth_score
AS SELECT
    fips,
    county_name,
    state,
    report_year,
    report_quarter,
    population,
    median_income,
    composite_score,
    score_tier,
    rank_national,
    to_json(component_scores) AS component_scores,
    ssp_projected_pop,
    ssp_projection_year,
    ssp_growth_rate
FROM ${source_catalog}.gold.gold_county_growth_score;

CREATE OR REFRESH MATERIALIZED VIEW gold_county_details
AS SELECT *
FROM ${source_catalog}.gold.gold_county_details;

CREATE OR REFRESH MATERIALIZED VIEW gold_scoring_config
AS SELECT *
FROM ${source_catalog}.gold.gold_scoring_config;
```

Note: `component_scores` is a Spark struct in UC — we convert it to JSON string for PostgreSQL JSONB compatibility. The `${source_catalog}` parameter is set in the pipeline config.

**Step 3: Commit**

```bash
git add src/pipelines/gold_lakebase_sync.sql
git commit -m "feat: add SDP pipeline SQL for gold-to-Lakebase sync"
```

---

### Task 3: Create DAB Pipeline Resource

**Files:**
- Create: `resources/pipelines/gold_lakebase_sync.yml`

**Step 1: Create resources/pipelines directory**

Run: `mkdir -p resources/pipelines`

**Step 2: Write the pipeline resource YAML**

```yaml
# resources/pipelines/gold_lakebase_sync.yml
resources:
  pipelines:
    gold_lakebase_sync:
      name: "store-siting-gold-lakebase-sync"
      catalog: ${var.lakebase_catalog}
      schema: "public"
      libraries:
        - file:
            path: ../../src/pipelines/gold_lakebase_sync.sql
      configuration:
        source_catalog: ${var.catalog}
      development: false
```

Note: The `catalog` here is the Unity Catalog foreign catalog name that maps to the Lakebase database. This must be created via `databricks database create-database-catalog` (Task 1 Step 7 alternative) or configured as a variable.

**Step 3: Commit**

```bash
git add resources/pipelines/gold_lakebase_sync.yml
git commit -m "feat: add DAB pipeline resource for Lakebase sync"
```

---

### Task 4: Update compute_gold_scores Job to Trigger Sync

**Files:**
- Modify: `resources/jobs/compute_gold_scores.yml`

**Step 1: Read current file**

Current content has a single task `run_gold_scoring`.

**Step 2: Add dependent sync trigger task**

```yaml
resources:
  jobs:
    compute_gold_scores:
      name: "store-siting-compute-gold-scores"
      tasks:
        - task_key: run_gold_scoring
          environment_key: default
          spark_python_task:
            python_file: ../../src/jobs/compute_gold_scores.py
            source: WORKSPACE
        - task_key: sync_to_lakebase
          depends_on:
            - task_key: run_gold_scoring
          pipeline_task:
            pipeline_id: ${resources.pipelines.gold_lakebase_sync.id}
            full_refresh: true
      environments:
        - environment_key: default
          spec:
            client: "1"
```

The `sync_to_lakebase` task runs a full refresh of the SDP pipeline after gold scoring completes. It uses `full_refresh: true` because the gold tables are fully overwritten each run.

**Step 3: Commit**

```bash
git add resources/jobs/compute_gold_scores.yml
git commit -m "feat: add Lakebase sync trigger to gold scoring job"
```

---

### Task 5: Update databricks.yml Variables

**Files:**
- Modify: `databricks.yml`

**Step 1: Read current file**

Already read — has `catalog` and `warehouse_id` variables.

**Step 2: Add lakebase variables, keep warehouse_id for now (other jobs may use it)**

```yaml
bundle:
  name: store-siting

workspace:
  host: https://fe-vm-vdm-classic-rikfy0.cloud.databricks.com
  profile: DEFAULT

variables:
  catalog:
    default: store_siting
    description: "Unity Catalog name"
  warehouse_id:
    default: "5067b513037fbf07"
    description: "SQL Warehouse ID for pipeline jobs"
  lakebase_host:
    default: ""
    description: "Lakebase endpoint host (from postgres list-endpoints)"
  lakebase_database:
    default: "store_siting_app"
    description: "Lakebase PostgreSQL database name"
  lakebase_catalog:
    default: "store_siting_lakebase"
    description: "UC foreign catalog name mapped to Lakebase"

include:
  - resources/*.yml
  - resources/jobs/*.yml
  - resources/pipelines/*.yml

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: store_siting_dev
      lakebase_database: store_siting_app_dev

  prod:
    variables:
      catalog: store_siting
      lakebase_database: store_siting_app
```

Key changes:
- Added `lakebase_host`, `lakebase_database`, `lakebase_catalog` variables
- Added `resources/pipelines/*.yml` to include path
- Kept `warehouse_id` (ingestion/silver jobs may still need it)

**Step 3: Commit**

```bash
git add databricks.yml
git commit -m "feat: add Lakebase variables and pipeline include to DAB config"
```

---

### Task 6: Rewire db.py to Use Lakebase via psycopg2

**Files:**
- Rewrite: `src/app/backend/db.py`

**Step 1: Write the new db.py**

```python
"""Lakebase PostgreSQL connection for the FastAPI app."""

import os
import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _get_oauth_token() -> str:
    """Get OAuth token for Lakebase via Databricks SDK."""
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if token:
        return token
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        tok = w.config.authenticate()
        if isinstance(tok, dict):
            return tok.get("Authorization", "").replace("Bearer ", "")
        return str(tok).replace("Bearer ", "")
    except Exception as e:
        logger.warning("Could not get SDK token: %s", e)
        return ""


def _get_user_email() -> str:
    """Get current user email for Lakebase auth."""
    email = os.environ.get("LAKEBASE_USER", "")
    if email:
        return email
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        return w.current_user.me().user_name
    except Exception as e:
        logger.warning("Could not get user email: %s", e)
        return "token"


@contextmanager
def get_cursor():
    host = os.environ.get("LAKEBASE_HOST", "")
    database = os.environ.get("LAKEBASE_DATABASE", "store_siting_app")
    user = _get_user_email()
    password = _get_oauth_token()

    logger.debug("Connecting to Lakebase at %s db=%s user=%s", host, database, user)

    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname=database,
        user=user,
        password=password,
        sslmode="require",
    )
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_query(query: str, params: tuple | None = None) -> list[dict]:
    with get_cursor() as cursor:
        cursor.execute(query, params)
        if cursor.description is None:
            return []
        return [dict(row) for row in cursor.fetchall()]
```

Key differences from old db.py:
- Uses `psycopg2` instead of `databricks.sql`
- Uses `RealDictCursor` for dict results (same API contract)
- OAuth token from Databricks SDK (same auth pattern)
- Proper transaction handling (commit/rollback)
- `params` is now a tuple (psycopg2 convention) — callers using keyword params will need adjustment

**Step 2: Commit**

```bash
git add src/app/backend/db.py
git commit -m "feat: rewire db.py to Lakebase via psycopg2"
```

---

### Task 7: Update Routes for PostgreSQL Compatibility

**Files:**
- Modify: `src/app/backend/routes/scoring.py`
- Modify: `src/app/backend/routes/counties.py`
- Modify: `src/app/backend/routes/geojson.py`

**Step 1: Update scoring.py — use parameterized queries**

The current `scoring.py` uses string interpolation for the UPDATE query. Switch to psycopg2 parameterized queries and remove catalog prefix (Lakebase tables are just `public.table_name`).

```python
import os
from fastapi import APIRouter
from backend.db import execute_query
from backend.models.county import ScoringWeight, ScoringWeightsUpdate

router = APIRouter(prefix="/api")


@router.get("/scores/weights", response_model=list[ScoringWeight])
def get_weights():
    return execute_query("SELECT * FROM gold_scoring_config")


@router.put("/scores/weights")
def update_weights(payload: ScoringWeightsUpdate):
    valid_indicators = {
        "building_permits", "net_migration", "vacancy_change",
        "employment_growth", "school_enrollment_growth",
        "ssp_projected_growth",
    }
    for w in payload.weights:
        if w.indicator not in valid_indicators:
            continue
        execute_query(
            "UPDATE gold_scoring_config SET weight = %s WHERE indicator = %s",
            (float(w.weight), w.indicator),
        )
    return {"status": "updated", "message": "Re-run gold scoring job to apply new weights."}
```

Key changes:
- Removed catalog-prefixed table names (Lakebase uses unqualified names)
- UPDATE uses `%s` parameterized query (prevents SQL injection, psycopg2 convention)
- Removed `os` import (no longer needed)

**Step 2: Update counties.py — remove catalog prefix**

```python
import re
from fastapi import APIRouter, Query, HTTPException
from backend.db import execute_query
from backend.models.county import CountySummary

router = APIRouter(prefix="/api")

_FIPS_RE = re.compile(r"^\d{5}$")
_STATE_RE = re.compile(r"^[A-Za-z]{2}$")


@router.get("/counties", response_model=list[CountySummary])
def list_counties(state: str | None = Query(None)):
    query = "SELECT * FROM gold_county_growth_score"
    params = None
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += " WHERE state = %s"
        params = (state,)
    query += " ORDER BY composite_score DESC"
    return execute_query(query, params)


@router.get("/counties/top", response_model=list[CountySummary])
def top_counties(n: int = Query(25, ge=1, le=500), state: str | None = Query(None)):
    query = "SELECT * FROM gold_county_growth_score"
    params_list = []
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += " WHERE state = %s"
        params_list.append(state)
    query += " ORDER BY composite_score DESC LIMIT %s"
    params_list.append(n)
    return execute_query(query, tuple(params_list))


@router.get("/counties/{fips}")
def get_county(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    score_rows = execute_query(
        "SELECT * FROM gold_county_growth_score WHERE fips = %s", (fips,)
    )
    if not score_rows:
        raise HTTPException(status_code=404, detail=f"County {fips} not found")
    row = score_rows[0]

    detail_rows = execute_query(
        "SELECT * FROM gold_county_details WHERE fips = %s", (fips,)
    )
    if detail_rows:
        row.update({k: v for k, v in detail_rows[0].items() if k != "fips" and v is not None})

    cs = row.get("component_scores")
    if isinstance(cs, str):
        import json
        try:
            row["component_scores"] = json.loads(cs)
        except (json.JSONDecodeError, TypeError):
            row["component_scores"] = None

    return row


@router.get("/trends/{fips}")
def get_trends(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    return []
```

Key changes:
- Removed `os` import and catalog lookups
- All table names are now unqualified (Lakebase `public` schema)
- All queries use `%s` parameterized params (psycopg2 convention, fixes SQL injection)
- `execute_query` params passed as tuples

**Step 3: Update geojson.py — remove catalog prefix**

```python
import json
import os
from fastapi import APIRouter
from backend.db import execute_query

router = APIRouter(prefix="/api")

_geojson_cache: dict | None = None


@router.get("/geojson")
def get_geojson():
    global _geojson_cache

    scores = execute_query(
        """SELECT s.fips, s.composite_score, s.score_tier, s.rank_national,
                  s.population, s.median_income, s.ssp_growth_rate,
                  d.permits_per_1k_pop, d.net_migration_rate,
                  d.occupancy_rate, d.employment_per_capita
           FROM gold_county_growth_score s
           LEFT JOIN gold_county_details d ON s.fips = d.fips"""
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

Key change: Removed catalog prefix from SQL query.

**Step 4: Commit**

```bash
git add src/app/backend/routes/scoring.py src/app/backend/routes/counties.py src/app/backend/routes/geojson.py
git commit -m "feat: update routes for Lakebase — unqualified tables, parameterized queries"
```

---

### Task 8: Update App Config (app.yaml + resources/app.yml)

**Files:**
- Modify: `src/app/app.yaml`
- Modify: `resources/app.yml`

**Step 1: Update app.yaml**

```yaml
command:
  - python
  - app.py
env:
  - name: LAKEBASE_HOST
    value: "<ENDPOINT_HOST_FROM_TASK_1>"
  - name: LAKEBASE_DATABASE
    value: store_siting_app
```

Replace `<ENDPOINT_HOST_FROM_TASK_1>` with the actual host from Task 1 Step 4.

**Step 2: Update resources/app.yml**

```yaml
resources:
  apps:
    store_siting_app:
      name: "store-siting"
      description: "QSR Store Siting — Growth Score Explorer"
      source_code_path: ../src/app
      config:
        command:
          - python
          - app.py
        env:
          - name: DATABRICKS_HOST
            value: "https://fe-vm-vdm-classic-rikfy0.cloud.databricks.com"
          - name: LAKEBASE_HOST
            value: ${var.lakebase_host}
          - name: LAKEBASE_DATABASE
            value: ${var.lakebase_database}
          - name: CATALOG
            value: ${var.catalog}
```

Note: Keep `DATABRICKS_HOST` — needed by the Databricks SDK to get OAuth tokens. Keep `CATALOG` — may be used by future features. Removed `DATABRICKS_HTTP_PATH` (no longer connecting to SQL Warehouse).

**Step 3: Commit**

```bash
git add src/app/app.yaml resources/app.yml
git commit -m "feat: update app config for Lakebase connection"
```

---

### Task 9: Add psycopg2 to App Dependencies

**Files:**
- Modify: `src/app/requirements.txt` (or create if missing)

**Step 1: Check for existing requirements.txt**

Run: `ls src/app/requirements.txt`

**Step 2: Add/update requirements.txt**

Ensure these are present:

```
fastapi
uvicorn
psycopg2-binary
databricks-sdk
```

Note: Use `psycopg2-binary` (pre-compiled, no libpq build dependency). Remove `databricks-sql-connector` if present.

**Step 3: Commit**

```bash
git add src/app/requirements.txt
git commit -m "feat: add psycopg2-binary, remove databricks-sql-connector"
```

---

### Task 10: Register Lakebase as UC Foreign Catalog

**Files:** None (CLI operations only)

This step creates the UC foreign catalog that the SDP pipeline writes to.

**Step 1: Register the Lakebase database as a UC catalog**

```bash
databricks database create-database-catalog store_siting_lakebase store-siting-app store_siting_app \
  --create-database-if-not-exists \
  -p DEFAULT
```

This makes the Lakebase tables queryable as `store_siting_lakebase.public.gold_county_growth_score` from Databricks SQL/notebooks.

**Step 2: Verify**

```bash
databricks sql execute -p DEFAULT --warehouse-id 5067b513037fbf07 \
  -q "SHOW TABLES IN store_siting_lakebase.public"
```

---

### Task 11: Deploy and Test End-to-End

**Files:** None (deploy + verify)

**Step 1: Build frontend**

```bash
cd src/app/frontend && npx vite build && cd ../../..
```

**Step 2: Deploy the bundle**

```bash
databricks bundle deploy --profile=DEFAULT
```

**Step 3: Run the gold scoring job (triggers sync)**

```bash
databricks jobs run-now 196681278666944 --profile=DEFAULT
```

Wait for both tasks (`run_gold_scoring` + `sync_to_lakebase`) to complete.

**Step 4: Verify data in Lakebase**

```bash
HOST=$(databricks postgres list-endpoints projects/store-siting-app/branches/production \
  -p DEFAULT -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential \
  projects/store-siting-app/branches/production/endpoints/primary \
  -p DEFAULT -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p DEFAULT -o json | jq -r '.userName')

PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=store_siting_app user=$EMAIL sslmode=require" \
  -c "SELECT COUNT(*) FROM gold_county_growth_score;"
```

Expected: ~3221 rows

**Step 5: Deploy the app**

```bash
databricks apps deploy store-siting \
  --source-code-path /Workspace/Users/jesus.rodriguez@databricks.com/.bundle/store-siting/dev/files/src/app \
  --profile=DEFAULT
```

**Step 6: Verify app is working**

Open: `https://store-siting-1351565862180944.aws.databricksapps.com/`

- Map loads with choropleth
- County detail panel shows scores
- Weight tuner reads/writes correctly
- Analysis tab filters and results work

**Step 7: Verify via API**

```bash
curl https://store-siting-1351565862180944.aws.databricksapps.com/api/counties/top?n=5
curl https://store-siting-1351565862180944.aws.databricksapps.com/api/scores/weights
```

---

### Task 12: Update Unit Tests

**Files:**
- Modify: Tests that mock `backend.db`

**Step 1: Find affected tests**

Run: `grep -r "databricks.sql\|execute_query\|get_cursor\|DATABRICKS_HTTP_PATH" tests/`

**Step 2: Update mocks**

Any test mocking `databricks.sql.connect` should now mock `psycopg2.connect`. The `execute_query` function signature changed: `params` is now a `tuple` instead of `dict`.

Update test mocks accordingly. The route behavior is unchanged — only the DB layer changed.

**Step 3: Run tests**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/store && python -m pytest tests/ -v
```

**Step 4: Commit**

```bash
git add tests/
git commit -m "test: update db mocks for psycopg2 Lakebase connection"
```

---

## Summary of All Changes

| File | Action | Description |
|------|--------|-------------|
| `src/pipelines/gold_lakebase_sync.sql` | Create | SDP pipeline — 3 materialized views from UC gold |
| `resources/pipelines/gold_lakebase_sync.yml` | Create | DAB pipeline resource definition |
| `resources/jobs/compute_gold_scores.yml` | Modify | Add `sync_to_lakebase` dependent task |
| `databricks.yml` | Modify | Add Lakebase vars, pipeline include |
| `src/app/backend/db.py` | Rewrite | psycopg2 + Databricks SDK OAuth |
| `src/app/backend/routes/scoring.py` | Modify | Parameterized queries, unqualified tables |
| `src/app/backend/routes/counties.py` | Modify | Parameterized queries, unqualified tables |
| `src/app/backend/routes/geojson.py` | Modify | Unqualified table names |
| `src/app/app.yaml` | Modify | Lakebase env vars |
| `resources/app.yml` | Modify | Lakebase env vars via DAB variables |
| `src/app/requirements.txt` | Modify | Add psycopg2-binary, remove databricks-sql-connector |
| Tests | Modify | Update DB mocks |
