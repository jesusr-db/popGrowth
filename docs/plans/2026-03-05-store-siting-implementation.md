# Store Siting App Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a QSR store siting app that scores US counties by leading indicators of population growth, deployed on Databricks via DABs with a React + FastAPI frontend.

**Architecture:** Medallion (Bronze/Silver/Gold) data pipeline on Databricks with 10 open data sources ingested into Bronze, standardized to county-level FIPS in Silver, and scored via weighted composite in Gold. A React + FastAPI Databricks App renders a county choropleth map with drill-down and configurable weights.

**Tech Stack:** Python 3.11, PySpark, Delta Lake, Unity Catalog, FastAPI, React 18, TypeScript, deck.gl, Vite, DABs, pytest, Vitest

---

## Phase 1: Project Scaffolding & DABs Setup

### Task 1: Initialize DABs Bundle and Project Structure

**Files:**
- Create: `databricks.yml`
- Create: `src/__init__.py`
- Create: `src/ingestion/__init__.py`
- Create: `src/silver/__init__.py`
- Create: `src/gold/__init__.py`
- Create: `src/common/__init__.py`
- Create: `tests/__init__.py`
- Create: `requirements.txt`

**Step 1: Create databricks.yml**

```yaml
bundle:
  name: store-siting

workspace:
  host: ${var.workspace_host}

variables:
  workspace_host:
    description: "Databricks workspace URL"
  catalog:
    default: store_siting
    description: "Unity Catalog name"
  warehouse_id:
    description: "SQL Warehouse ID for app queries"

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: store_siting_dev

  prod:
    variables:
      catalog: store_siting
```

**Step 2: Create requirements.txt**

```
pyspark>=3.5.0
delta-spark>=3.1.0
requests>=2.31.0
databricks-sql-connector>=3.1.0
fastapi>=0.110.0
uvicorn>=0.27.0
pydantic>=2.6.0
```

**Step 3: Create all `__init__.py` files and directory structure**

```bash
mkdir -p src/{ingestion,silver,gold,common} tests/{unit,integration,fixtures} \
  src/app/backend/{routes,models} src/app/frontend \
  resources/jobs data/county_geojson
touch src/__init__.py src/ingestion/__init__.py src/silver/__init__.py \
  src/gold/__init__.py src/common/__init__.py tests/__init__.py \
  tests/unit/__init__.py tests/integration/__init__.py
```

**Step 4: Commit**

```bash
git add databricks.yml requirements.txt src/ tests/ resources/ data/
git commit -m "feat: scaffold DABs bundle and project structure"
```

---

### Task 2: Build Common Utilities (FIPS, Ingestion Logger, Config)

**Files:**
- Create: `src/common/fips.py`
- Create: `src/common/ingestion_logger.py`
- Create: `src/common/config.py`
- Create: `src/common/schemas.py`
- Test: `tests/unit/test_fips.py`
- Test: `tests/unit/test_config.py`

**Step 1: Write failing tests for FIPS utilities**

```python
# tests/unit/test_fips.py
import pytest
from src.common.fips import normalize_fips, validate_fips, zip_to_county_fips


def test_normalize_fips_pads_short_codes():
    assert normalize_fips("1001") == "01001"
    assert normalize_fips("6037") == "06037"


def test_normalize_fips_preserves_five_digit():
    assert normalize_fips("36061") == "36061"


def test_normalize_fips_rejects_invalid():
    with pytest.raises(ValueError):
        normalize_fips("abc")
    with pytest.raises(ValueError):
        normalize_fips("")
    with pytest.raises(ValueError):
        normalize_fips("123456")


def test_validate_fips_valid():
    assert validate_fips("01001") is True
    assert validate_fips("36061") is True


def test_validate_fips_invalid():
    assert validate_fips("00000") is False
    assert validate_fips("abc") is False
    assert validate_fips("") is False
```

**Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_fips.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.fips'`

**Step 3: Implement FIPS utilities**

```python
# src/common/fips.py
"""FIPS county code normalization and validation utilities."""

# Valid state FIPS codes (01-56, excluding gaps)
VALID_STATE_FIPS = {
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56",
}


def normalize_fips(raw: str) -> str:
    """Normalize a FIPS code to 5-digit zero-padded string."""
    cleaned = str(raw).strip()
    if not cleaned.isdigit():
        raise ValueError(f"FIPS code must be numeric, got: {raw!r}")
    if len(cleaned) > 5 or len(cleaned) < 1:
        raise ValueError(f"FIPS code must be 1-5 digits, got: {raw!r}")
    return cleaned.zfill(5)


def validate_fips(fips: str) -> bool:
    """Check if a 5-digit FIPS code has a valid state prefix."""
    if not fips or not fips.isdigit() or len(fips) != 5:
        return False
    state = fips[:2]
    return state in VALID_STATE_FIPS


def state_fips_from_county(county_fips: str) -> str:
    """Extract 2-digit state FIPS from 5-digit county FIPS."""
    normalized = normalize_fips(county_fips)
    return normalized[:2]
```

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_fips.py -v`
Expected: All PASS

**Step 5: Write config module**

```python
# src/common/config.py
"""Scoring weights and pipeline configuration."""

DEFAULT_WEIGHTS = {
    "building_permits": 0.25,
    "net_migration": 0.20,
    "vacancy_change": 0.15,
    "employment_growth": 0.15,
    "school_enrollment_growth": 0.10,
    "ssp_projected_growth": 0.10,
    "qsr_density_inv": 0.05,
}

SCORE_TIERS = {
    "A": (80, 100),
    "B": (60, 79),
    "C": (40, 59),
    "D": (20, 39),
    "F": (0, 19),
}

CATALOG = "store_siting"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

MIN_SOURCES_FOR_SCORE = 5
TOTAL_SOURCES = 10


def get_table_name(schema: str, table: str) -> str:
    """Return fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{table}"
```

**Step 6: Write failing test for config**

```python
# tests/unit/test_config.py
from src.common.config import DEFAULT_WEIGHTS, SCORE_TIERS, get_table_name


def test_weights_sum_to_one():
    assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9


def test_tiers_cover_full_range():
    all_values = set()
    for low, high in SCORE_TIERS.values():
        all_values.update(range(low, high + 1))
    assert all_values == set(range(0, 101))


def test_get_table_name():
    assert get_table_name("bronze", "permits") == "store_siting.bronze.permits"
```

**Step 7: Run tests**

Run: `python -m pytest tests/unit/test_config.py -v`
Expected: All PASS

**Step 8: Write ingestion logger**

```python
# src/common/ingestion_logger.py
"""Log ingestion runs to a tracking table for observability."""

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


INGESTION_LOG_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("status", StringType(), False),  # "success" | "failure"
    StructField("rows_loaded", IntegerType(), True),
    StructField("error_msg", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), False),
])


def log_ingestion(
    spark: SparkSession,
    source_name: str,
    status: str,
    rows_loaded: int | None = None,
    error_msg: str | None = None,
    started_at: datetime | None = None,
    catalog: str = "store_siting",
):
    """Write a row to the ingestion log table."""
    now = datetime.now(timezone.utc)
    row = [(
        source_name,
        status,
        rows_loaded,
        error_msg,
        started_at or now,
        now,
    )]
    df = spark.createDataFrame(row, schema=INGESTION_LOG_SCHEMA)
    df.write.mode("append").saveAsTable(f"{catalog}.bronze._ingestion_log")
```

**Step 9: Write common schemas**

```python
# src/common/schemas.py
"""Shared PySpark schemas for Bronze and Silver tables."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType,
)


def bronze_metadata_fields():
    """Common metadata fields appended to all Bronze tables."""
    return [
        StructField("source_date", DateType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
```

**Step 10: Commit**

```bash
git add src/common/ tests/unit/test_fips.py tests/unit/test_config.py
git commit -m "feat: add common utilities — FIPS normalization, config, ingestion logger"
```

---

## Phase 2: Data Ingestion (Bronze Layer)

Each ingestion module follows the same pattern: download from public URL, parse, add metadata columns, write to Bronze Delta table. We'll build 2 representative sources in full (building permits + migration), then provide the pattern for the remaining 8.

### Task 3: Ingest Census Building Permits (Bronze)

**Files:**
- Create: `src/ingestion/building_permits.py`
- Create: `tests/fixtures/building_permits_sample.csv`
- Test: `tests/unit/test_ingest_building_permits.py`

**Step 1: Create sample fixture data**

```csv
# tests/fixtures/building_permits_sample.csv
Survey Date,CSA/CBSA Code,CSA/CBSA Name,FIPS State Code,FIPS County Code,Region,Division,County Name,1-unit Bldgs,1-unit Units,2-unit Bldgs,2-unit Units,3-4 unit Bldgs,3-4 unit Units,5+ unit Bldgs,5+ unit Units
01/2025,33100,Miami-Fort Lauderdale-Pompano Beach FL,12,086,South,South Atlantic,Miami-Dade County,450,450,25,50,15,52,30,280
01/2025,35620,New York-Newark-Jersey City NY-NJ-PA,36,061,Northeast,Middle Atlantic,New York County,50,50,10,20,8,28,45,890
01/2025,31080,Los Angeles-Long Beach-Anaheim CA,06,037,West,Pacific,Los Angeles County,320,320,18,36,12,42,55,650
```

**Step 2: Write failing test**

```python
# tests/unit/test_ingest_building_permits.py
import pytest
import os
from unittest.mock import patch, MagicMock
from src.ingestion.building_permits import parse_building_permits_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2025, 1)
    assert "census.gov" in url
    assert "2025" in url


def test_parse_building_permits_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "building_permits_sample.csv"
    )
    rows = parse_building_permits_csv(fixture_path)
    assert len(rows) == 3
    # Check first row
    row = rows[0]
    assert row["fips"] == "12086"
    assert row["county_name"] == "Miami-Dade County"
    assert row["single_family_units"] == 450
    assert row["multi_family_units"] == 330  # 50 + 52 + 280 - 2unit + 3-4unit + 5+unit
    assert row["total_units"] == 780  # single + multi (corrected: 450 + 50 + 52 + 280)


def test_parse_building_permits_csv_computes_total():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "building_permits_sample.csv"
    )
    rows = parse_building_permits_csv(fixture_path)
    for row in rows:
        assert row["total_units"] == row["single_family_units"] + row["multi_family_units"]
```

**Step 3: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_ingest_building_permits.py -v`
Expected: FAIL

**Step 4: Implement building permits ingestion**

```python
# src/ingestion/building_permits.py
"""Ingest Census Bureau Building Permits Survey data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int, month: int) -> str:
    """Build the Census Building Permits CSV download URL."""
    mm = str(month).zfill(2)
    return (
        f"https://www2.census.gov/econ/bps/County/co{year}{mm}a.txt"
    )


def parse_building_permits_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse a building permits CSV file into a list of row dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            state_fips = record["FIPS State Code"].strip()
            county_fips = record["FIPS County Code"].strip()
            fips = normalize_fips(state_fips + county_fips)

            single = int(record["1-unit Units"].strip())
            two_unit = int(record["2-unit Units"].strip())
            three_four = int(record["3-4 unit Units"].strip())
            five_plus = int(record["5+ unit Units"].strip())
            multi = two_unit + three_four + five_plus
            total = single + multi

            rows.append({
                "fips": fips,
                "county_name": record["County Name"].strip(),
                "survey_date": record["Survey Date"].strip(),
                "single_family_units": single,
                "multi_family_units": multi,
                "total_units": total,
                "single_family_bldgs": int(record["1-unit Bldgs"].strip()),
            })
    return rows


def download_and_parse(year: int, month: int) -> list[dict[str, Any]]:
    """Download building permits data for a given year/month and parse it."""
    url = build_download_url(year, month)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return parse_building_permits_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, month: int, catalog: str = "store_siting"):
    """Full ingestion: download, parse, write to Bronze Delta table."""
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year, month)
        if not rows:
            log_ingestion(spark, "building_permits", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, month, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        table = f"{catalog}.bronze.building_permits"
        df.write.mode("append").saveAsTable(table)

        log_ingestion(spark, "building_permits", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "building_permits", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
```

**Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_ingest_building_permits.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ingestion/building_permits.py tests/unit/test_ingest_building_permits.py tests/fixtures/building_permits_sample.csv
git commit -m "feat: add Census building permits ingestion (Bronze)"
```

---

### Task 4: Ingest USPS Migration Data via HUD (Bronze)

**Files:**
- Create: `src/ingestion/migration.py`
- Create: `tests/fixtures/migration_sample.csv`
- Test: `tests/unit/test_ingest_migration.py`

**Step 1: Create sample fixture**

```csv
# tests/fixtures/migration_sample.csv
year,quarter,county_fips,county_name,state,total_in,total_out,net_change
2025,1,12086,Miami-Dade,FL,8500,6200,2300
2025,1,36061,New York,NY,4200,7800,-3600
2025,1,06037,Los Angeles,CA,9100,11200,-2100
```

**Step 2: Write failing test**

```python
# tests/unit/test_ingest_migration.py
import os
from src.ingestion.migration import parse_migration_csv


def test_parse_migration_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "migration_sample.csv"
    )
    rows = parse_migration_csv(fixture_path)
    assert len(rows) == 3

    miami = rows[0]
    assert miami["fips"] == "12086"
    assert miami["inflow"] == 8500
    assert miami["outflow"] == 6200
    assert miami["net_migration"] == 2300

    ny = rows[1]
    assert ny["net_migration"] == -3600


def test_parse_migration_csv_all_have_fips():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "migration_sample.csv"
    )
    rows = parse_migration_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
```

**Step 3: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_ingest_migration.py -v`
Expected: FAIL

**Step 4: Implement migration ingestion**

```python
# src/ingestion/migration.py
"""Ingest USPS Change-of-Address migration data via HUD into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips


MIGRATION_URL = "https://www.huduser.gov/portal/datasets/usps/USPS_Migration_{year}q{quarter}.csv"


def build_download_url(year: int, quarter: int) -> str:
    return MIGRATION_URL.format(year=year, quarter=quarter)


def parse_migration_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse migration CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            inflow = int(record["total_in"].strip())
            outflow = int(record["total_out"].strip())
            net = int(record["net_change"].strip())
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "report_year": int(record["year"].strip()),
                "report_quarter": int(record["quarter"].strip()),
                "inflow": inflow,
                "outflow": outflow,
                "net_migration": net,
            })
    return rows


def download_and_parse(year: int, quarter: int) -> list[dict[str, Any]]:
    url = build_download_url(year, quarter)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return parse_migration_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, quarter: int, catalog: str = "store_siting"):
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year, quarter)
        if not rows:
            log_ingestion(spark, "migration", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, quarter * 3, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.migration")

        log_ingestion(spark, "migration", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "migration", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
```

**Step 5: Run tests**

Run: `python -m pytest tests/unit/test_ingest_migration.py -v`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/ingestion/migration.py tests/unit/test_ingest_migration.py tests/fixtures/migration_sample.csv
git commit -m "feat: add USPS migration ingestion via HUD (Bronze)"
```

---

### Task 5: Implement Remaining 8 Ingestion Modules (Bronze)

**Files:**
- Create: `src/ingestion/hud_construction.py`
- Create: `src/ingestion/vacancy.py`
- Create: `src/ingestion/employment.py`
- Create: `src/ingestion/school_enrollment.py`
- Create: `src/ingestion/acs_demographics.py`
- Create: `src/ingestion/business_patterns.py`
- Create: `src/ingestion/ssp_projections.py`
- Create: `src/ingestion/national_projections.py`
- Create: `tests/fixtures/` (one sample CSV per source)
- Test: `tests/unit/test_ingest_*.py` (one test file per source)

Each module follows the **exact same pattern** as Tasks 3-4:
1. Create sample fixture CSV
2. Write failing test for `parse_*_csv()` and `build_download_url()`
3. Run test to see it fail
4. Implement `parse_*_csv()`, `build_download_url()`, `download_and_parse()`, `ingest()`
5. Run test to see it pass
6. Commit

**Source-specific details:**

| Module | URL Pattern | Key Parse Fields |
|--------|-------------|-----------------|
| `hud_construction.py` | huduser.gov SOCDS permits | fips, permitted_units, structure_type |
| `vacancy.py` | huduser.gov USPS vacancy | fips, total_addresses, vacant_addresses, vacancy_rate |
| `employment.py` | bls.gov QCEW CSV | fips, total_employment, total_wages, establishments |
| `school_enrollment.py` | nces.ed.gov CCD | fips (derived from district), total_enrollment |
| `acs_demographics.py` | Census API (requests + JSON) | fips, population, median_income, median_age, households |
| `business_patterns.py` | Census API CBP | fips, naics_code, establishments, employees |
| `ssp_projections.py` | data.gov CSV | fips, projection_year, scenario, projected_pop |
| `national_projections.py` | census.gov CSV | state_fips, projection_year, projected_pop |

**Step-per-module:** Write fixture, test, implement, test, commit. One commit per module.

---

## Phase 3: Silver Transforms

### Task 6: Silver Transform — Building Permits

**Files:**
- Create: `src/silver/building_permits.py`
- Test: `tests/unit/test_silver_building_permits.py`

**Step 1: Write failing test**

```python
# tests/unit/test_silver_building_permits.py
import pytest
from pyspark.sql import SparkSession
from src.silver.building_permits import transform_building_permits


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_building_permits_aggregates_monthly_to_quarterly(spark):
    data = [
        ("12086", "Miami-Dade County", "01/2025", 450, 330, 780),
        ("12086", "Miami-Dade County", "02/2025", 400, 280, 680),
        ("12086", "Miami-Dade County", "03/2025", 500, 350, 850),
    ]
    columns = ["fips", "county_name", "survey_date", "single_family_units",
               "multi_family_units", "total_units"]
    bronze_df = spark.createDataFrame(data, columns)

    result = transform_building_permits(bronze_df)
    rows = result.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["fips"] == "12086"
    assert row["report_year"] == 2025
    assert row["report_quarter"] == 1
    assert row["total_units_permitted"] == 780 + 680 + 850  # 2310


def test_transform_has_required_columns(spark):
    data = [("12086", "Miami-Dade County", "01/2025", 450, 330, 780)]
    columns = ["fips", "county_name", "survey_date", "single_family_units",
               "multi_family_units", "total_units"]
    bronze_df = spark.createDataFrame(data, columns)

    result = transform_building_permits(bronze_df)
    required = {"fips", "report_year", "report_quarter", "total_units_permitted",
                "single_family_units", "multi_family_units"}
    assert required.issubset(set(result.columns))
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_silver_building_permits.py -v`
Expected: FAIL

**Step 3: Implement Silver transform**

```python
# src/silver/building_permits.py
"""Transform Bronze building permits to Silver — aggregate monthly to quarterly at county level."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, quarter as spark_quarter,
    year as spark_year, to_date, concat, lit,
)


def transform_building_permits(bronze_df: DataFrame) -> DataFrame:
    """Aggregate monthly building permits to quarterly at county FIPS level."""
    parsed = bronze_df.withColumn(
        "parsed_date", to_date(col("survey_date"), "MM/yyyy")
    ).withColumn(
        "report_year", spark_year("parsed_date")
    ).withColumn(
        "report_quarter", spark_quarter("parsed_date")
    )

    aggregated = (
        parsed
        .groupBy("fips", "county_name", "report_year", "report_quarter")
        .agg(
            spark_sum("total_units").alias("total_units_permitted"),
            spark_sum("single_family_units").alias("single_family_units"),
            spark_sum("multi_family_units").alias("multi_family_units"),
        )
    )
    return aggregated
```

**Step 4: Run tests**

Run: `python -m pytest tests/unit/test_silver_building_permits.py -v`
Expected: All PASS

**Step 5: Commit**

```bash
git add src/silver/building_permits.py tests/unit/test_silver_building_permits.py
git commit -m "feat: add Silver transform for building permits (monthly -> quarterly)"
```

---

### Task 7: Silver Transform — Migration

**Files:**
- Create: `src/silver/migration.py`
- Test: `tests/unit/test_silver_migration.py`

**Step 1: Write failing test**

```python
# tests/unit/test_silver_migration.py
import pytest
from pyspark.sql import SparkSession
from src.silver.migration import transform_migration


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_migration_computes_rate(spark):
    # Migration data already at quarterly grain, just need rate computation
    data = [
        ("12086", "Miami-Dade", "FL", 2025, 1, 8500, 6200, 2300),
        ("36061", "New York", "NY", 2025, 1, 4200, 7800, -3600),
    ]
    columns = ["fips", "county_name", "state", "report_year", "report_quarter",
               "inflow", "outflow", "net_migration"]
    bronze_df = spark.createDataFrame(data, columns)

    # Need ACS population for rate calculation
    pop_data = [("12086", 2800000), ("36061", 1630000)]
    pop_df = spark.createDataFrame(pop_data, ["fips", "population"])

    result = transform_migration(bronze_df, pop_df)
    rows = {r["fips"]: r for r in result.collect()}

    assert rows["12086"]["net_migration_rate"] == pytest.approx(2300 / 2800000 * 1000, rel=0.01)
    assert rows["36061"]["net_migration_rate"] < 0  # net outflow
```

**Step 2: Run test — expect FAIL**

**Step 3: Implement**

```python
# src/silver/migration.py
"""Transform Bronze migration data to Silver with net migration rates."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_migration(bronze_df: DataFrame, population_df: DataFrame) -> DataFrame:
    """Join migration with population to compute per-capita migration rate."""
    joined = bronze_df.join(population_df, on="fips", how="left")
    result = joined.withColumn(
        "net_migration_rate",
        (col("net_migration") / col("population")) * 1000
    )
    return result.select(
        "fips", "report_year", "report_quarter",
        "inflow", "outflow", "net_migration", "net_migration_rate"
    )
```

**Step 4: Run tests — expect PASS**

**Step 5: Commit**

```bash
git add src/silver/migration.py tests/unit/test_silver_migration.py
git commit -m "feat: add Silver transform for migration with per-capita rate"
```

---

### Task 8: Silver Transforms — Remaining 8 Sources

**Files:**
- Create: `src/silver/vacancy.py`
- Create: `src/silver/hud_construction.py`
- Create: `src/silver/employment.py`
- Create: `src/silver/school_enrollment.py`
- Create: `src/silver/acs_demographics.py`
- Create: `src/silver/business_patterns.py`
- Create: `src/silver/ssp_projections.py`
- Create: `src/silver/national_projections.py`
- Test: one test file per source in `tests/unit/`

Follow exact same pattern as Tasks 6-7. Key transforms per source:

| Module | Key Transform Logic |
|--------|-------------------|
| `vacancy.py` | Compute `vacancy_rate = vacant / total_addresses`, `vacancy_rate_yoy_change` via window function |
| `hud_construction.py` | Compute `construction_growth_rate` as QoQ change in permitted units |
| `employment.py` | Compute `employment_growth_rate` as QoQ change, `avg_weekly_wage = total_wages / (employment * 13)` |
| `school_enrollment.py` | Aggregate district-level to county, compute `enrollment_growth_rate` YoY |
| `acs_demographics.py` | Passthrough — already at county level. Ensure proper types. |
| `business_patterns.py` | Filter NAICS 7222 (QSR), compute `qsr_establishments`, `retail_density = establishments / population * 1000` |
| `ssp_projections.py` | Passthrough with scenario column. Compute `projected_growth_rate` from base year. |
| `national_projections.py` | Passthrough. State-level context table. |

One commit per module: fixture, test, implement, test, commit.

---

### Task 9: Silver Orchestration — Master Transform Job

**Files:**
- Create: `src/silver/transforms.py`
- Create: `resources/jobs/transform_silver.yml`

**Step 1: Write the orchestrator**

```python
# src/silver/transforms.py
"""Master Silver transform job — reads all Bronze tables, writes Silver."""

from pyspark.sql import SparkSession
from src.common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA
from src.silver.building_permits import transform_building_permits
from src.silver.migration import transform_migration
from src.silver.vacancy import transform_vacancy
from src.silver.hud_construction import transform_hud_construction
from src.silver.employment import transform_employment
from src.silver.school_enrollment import transform_school_enrollment
from src.silver.acs_demographics import transform_acs_demographics
from src.silver.business_patterns import transform_business_patterns
from src.silver.ssp_projections import transform_ssp_projections
from src.silver.national_projections import transform_national_projections


def run_all_silver_transforms(spark: SparkSession, catalog: str = CATALOG):
    """Execute all Silver transforms and write to Unity Catalog."""
    bronze = f"{catalog}.{BRONZE_SCHEMA}"
    silver = f"{catalog}.{SILVER_SCHEMA}"

    # Load population for rate calculations
    pop_df = spark.table(f"{silver}.silver_acs_demographics").select("fips", "population")

    transforms = [
        ("building_permits", lambda: transform_building_permits(
            spark.table(f"{bronze}.building_permits"))),
        ("migration", lambda: transform_migration(
            spark.table(f"{bronze}.migration"), pop_df)),
        ("vacancy", lambda: transform_vacancy(
            spark.table(f"{bronze}.vacancy"))),
        ("hud_construction", lambda: transform_hud_construction(
            spark.table(f"{bronze}.hud_construction"))),
        ("employment", lambda: transform_employment(
            spark.table(f"{bronze}.employment"))),
        ("school_enrollment", lambda: transform_school_enrollment(
            spark.table(f"{bronze}.school_enrollment"))),
        ("acs_demographics", lambda: transform_acs_demographics(
            spark.table(f"{bronze}.acs_demographics"))),
        ("business_patterns", lambda: transform_business_patterns(
            spark.table(f"{bronze}.business_patterns"), pop_df)),
        ("ssp_projections", lambda: transform_ssp_projections(
            spark.table(f"{bronze}.ssp_projections"))),
        ("national_projections", lambda: transform_national_projections(
            spark.table(f"{bronze}.national_projections"))),
    ]

    for name, transform_fn in transforms:
        df = transform_fn()
        df.write.mode("overwrite").saveAsTable(f"{silver}.silver_{name}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-transforms").getOrCreate()
    run_all_silver_transforms(spark)
```

**Step 2: Create job definition**

```yaml
# resources/jobs/transform_silver.yml
resources:
  jobs:
    transform_silver:
      name: "store-siting-transform-silver"
      tasks:
        - task_key: run_silver_transforms
          python_wheel_task:
            package_name: store_siting
            entry_point: silver_transforms
          libraries:
            - whl: ../dist/*.whl
          existing_cluster_id: ${var.cluster_id}
```

**Step 3: Commit**

```bash
git add src/silver/transforms.py resources/jobs/transform_silver.yml
git commit -m "feat: add Silver transform orchestrator and job definition"
```

---

## Phase 4: Gold Scoring

### Task 10: Gold Scoring Engine

**Files:**
- Create: `src/gold/scoring.py`
- Test: `tests/unit/test_scoring.py`

**Step 1: Write failing tests**

```python
# tests/unit/test_scoring.py
import pytest
from src.gold.scoring import min_max_normalize, compute_composite_score, assign_tier


def test_min_max_normalize():
    values = [10, 20, 30, 40, 50]
    result = min_max_normalize(values)
    assert result == [0.0, 0.25, 0.5, 0.75, 1.0]


def test_min_max_normalize_single_value():
    result = min_max_normalize([42])
    assert result == [0.0]


def test_min_max_normalize_identical_values():
    result = min_max_normalize([5, 5, 5])
    assert result == [0.0, 0.0, 0.0]


def test_compute_composite_score():
    indicators = {
        "building_permits": 0.8,
        "net_migration": 0.6,
        "vacancy_change": 0.7,
        "employment_growth": 0.5,
        "school_enrollment_growth": 0.9,
        "ssp_projected_growth": 0.4,
        "qsr_density_inv": 0.3,
    }
    weights = {
        "building_permits": 0.25,
        "net_migration": 0.20,
        "vacancy_change": 0.15,
        "employment_growth": 0.15,
        "school_enrollment_growth": 0.10,
        "ssp_projected_growth": 0.10,
        "qsr_density_inv": 0.05,
    }
    score = compute_composite_score(indicators, weights)
    expected = (0.8*0.25 + 0.6*0.20 + 0.7*0.15 + 0.5*0.15 + 0.9*0.10 + 0.4*0.10 + 0.3*0.05) * 100
    assert score == pytest.approx(expected, rel=0.01)


def test_assign_tier():
    assert assign_tier(95) == "A"
    assert assign_tier(80) == "A"
    assert assign_tier(79) == "B"
    assert assign_tier(60) == "B"
    assert assign_tier(59) == "C"
    assert assign_tier(40) == "C"
    assert assign_tier(39) == "D"
    assert assign_tier(20) == "D"
    assert assign_tier(19) == "F"
    assert assign_tier(0) == "F"
```

**Step 2: Run tests — expect FAIL**

Run: `python -m pytest tests/unit/test_scoring.py -v`

**Step 3: Implement scoring engine**

```python
# src/gold/scoring.py
"""Gold layer scoring engine — weighted composite score per county."""

from src.common.config import DEFAULT_WEIGHTS, SCORE_TIERS


def min_max_normalize(values: list[float]) -> list[float]:
    """Min-max normalize a list of values to [0, 1]."""
    if len(values) <= 1:
        return [0.0] * len(values)
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.0] * len(values)
    return [(v - min_v) / (max_v - min_v) for v in values]


def compute_composite_score(
    indicators: dict[str, float],
    weights: dict[str, float] | None = None,
) -> float:
    """Compute weighted composite score from normalized indicator values.

    Args:
        indicators: dict of indicator_name -> normalized value (0-1)
        weights: dict of indicator_name -> weight (should sum to 1.0)

    Returns:
        Score between 0 and 100.
    """
    w = weights or DEFAULT_WEIGHTS
    score = sum(indicators.get(k, 0.0) * w.get(k, 0.0) for k in w)
    return score * 100


def assign_tier(score: float) -> str:
    """Assign A/B/C/D/F tier based on score."""
    for tier, (low, high) in SCORE_TIERS.items():
        if low <= score <= high:
            return tier
    return "F"
```

**Step 4: Run tests — expect PASS**

Run: `python -m pytest tests/unit/test_scoring.py -v`

**Step 5: Commit**

```bash
git add src/gold/scoring.py tests/unit/test_scoring.py
git commit -m "feat: add Gold scoring engine — composite score, normalization, tiers"
```

---

### Task 11: Gold Spark Job — Join Silver Tables and Score

**Files:**
- Create: `src/gold/compute_scores.py`
- Create: `resources/jobs/compute_gold_scores.yml`

**Step 1: Implement Gold compute job**

```python
# src/gold/compute_scores.py
"""Gold scoring job — join all Silver tables, normalize, score, write Gold."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, struct, lit, udf, row_number, coalesce,
    count as spark_count,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

from src.common.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA, DEFAULT_WEIGHTS, MIN_SOURCES_FOR_SCORE
from src.gold.scoring import compute_composite_score, assign_tier


def build_indicator_table(spark: SparkSession, catalog: str = CATALOG) -> DataFrame:
    """Join all Silver tables into a single indicator table per county per quarter."""
    silver = f"{catalog}.{SILVER_SCHEMA}"

    # Start with the most complete table (ACS demographics — has all counties)
    base = (
        spark.table(f"{silver}.silver_acs_demographics")
        .select("fips", "report_year", col("population"), col("median_income"))
    )

    # Building permits — latest quarter per year
    permits = (
        spark.table(f"{silver}.silver_building_permits")
        .select("fips", "report_year", "report_quarter",
                col("total_units_permitted"),
                col("single_family_units").alias("bp_single"),
                col("multi_family_units").alias("bp_multi"))
    )

    # Migration
    migration = (
        spark.table(f"{silver}.silver_migration")
        .select("fips", "report_year", "report_quarter",
                "net_migration", "net_migration_rate")
    )

    # Vacancy
    vacancy = (
        spark.table(f"{silver}.silver_vacancy")
        .select("fips", "report_year", "report_quarter",
                "vacancy_rate", "vacancy_rate_yoy_change")
    )

    # Employment
    employment = (
        spark.table(f"{silver}.silver_employment")
        .select("fips", "report_year", "report_quarter",
                "employment_growth_rate", "avg_weekly_wage")
    )

    # School enrollment (annual)
    school = (
        spark.table(f"{silver}.silver_school_enrollment")
        .select("fips", "report_year", "enrollment_growth_rate")
    )

    # Business patterns (annual)
    business = (
        spark.table(f"{silver}.silver_business_patterns")
        .select("fips", "report_year", "qsr_establishments", "retail_density")
    )

    # SSP projections — use middle scenario (SSP2)
    ssp = (
        spark.table(f"{silver}.silver_ssp_projections")
        .filter(col("scenario") == "SSP2")
        .select("fips", col("projection_year").alias("report_year"),
                col("projected_population"))
    )

    # Join everything on fips + report_year (quarterly sources use latest quarter)
    join_keys_yr = ["fips", "report_year"]
    join_keys_qtr = ["fips", "report_year", "report_quarter"]

    # For quarterly data, get the latest available quarter
    latest_q = (
        permits.join(migration, on=join_keys_qtr, how="outer")
               .join(vacancy, on=join_keys_qtr, how="outer")
               .join(employment, on=join_keys_qtr, how="outer")
    )

    # Join annual data
    combined = (
        latest_q
        .join(base, on=join_keys_yr[:2], how="outer")
        .join(school, on=join_keys_yr[:2], how="left")
        .join(business, on=join_keys_yr[:2], how="left")
        .join(ssp, on=join_keys_yr[:2], how="left")
    )

    # Compute permits_per_1k_pop
    combined = combined.withColumn(
        "permits_per_1k_pop",
        coalesce(col("total_units_permitted"), lit(0)) / col("population") * 1000
    )

    return combined


def score_counties(indicator_df: DataFrame, weights: dict | None = None) -> DataFrame:
    """Normalize indicators and compute composite scores."""
    w = weights or DEFAULT_WEIGHTS

    # Collect values for normalization
    # We'll use Spark window functions for min-max normalization
    from pyspark.sql.functions import min as spark_min, max as spark_max

    indicator_cols = {
        "building_permits": "permits_per_1k_pop",
        "net_migration": "net_migration_rate",
        "vacancy_change": "vacancy_rate_yoy_change",
        "employment_growth": "employment_growth_rate",
        "school_enrollment_growth": "enrollment_growth_rate",
        "ssp_projected_growth": "projected_population",
        "qsr_density_inv": "qsr_establishments",
    }

    # Invert indicators where lower = better
    inverted = {"vacancy_change", "qsr_density_inv"}

    df = indicator_df
    for indicator_name, source_col in indicator_cols.items():
        min_col = f"_min_{indicator_name}"
        max_col = f"_max_{indicator_name}"

        # Add min/max as columns (window over entire dataset)
        df = df.withColumn(min_col, spark_min(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))
        df = df.withColumn(max_col, spark_max(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))

        # Min-max normalize
        norm_col = f"_norm_{indicator_name}"
        range_expr = col(max_col) - col(min_col)
        normalized = (col(source_col) - col(min_col)) / range_expr

        if indicator_name in inverted:
            normalized = lit(1.0) - normalized

        df = df.withColumn(norm_col, coalesce(normalized, lit(0.0)))

        # Drop temp columns
        df = df.drop(min_col, max_col)

    # Compute composite score
    score_expr = sum(
        col(f"_norm_{name}") * lit(w[name])
        for name in w
    ) * 100

    df = df.withColumn("composite_score", score_expr)
    df = df.withColumn("score_tier", udf(assign_tier, StringType())(col("composite_score")))

    # National rank
    rank_window = Window.orderBy(col("composite_score").desc())
    df = df.withColumn("rank_national", row_number().over(rank_window))

    # Pack component scores into struct
    df = df.withColumn("component_scores", struct(
        *[col(f"_norm_{name}").alias(name) for name in w]
    ))

    # Drop internal columns
    for name in w:
        df = df.drop(f"_norm_{name}")

    return df


def run_gold_scoring(spark: SparkSession, catalog: str = CATALOG):
    """Main entry point for Gold scoring job."""
    indicator_df = build_indicator_table(spark, catalog)
    scored_df = score_counties(indicator_df)

    # Write gold_county_growth_score
    scored_df.select(
        "fips", "report_year", "report_quarter",
        "population", "median_income",
        "composite_score", "score_tier", "rank_national",
        "component_scores",
    ).write.mode("overwrite").saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_growth_score")

    # Write gold_county_details (wide table with all metrics)
    indicator_df.write.mode("overwrite").saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_details")

    # Write/update scoring config
    from pyspark.sql import Row
    config_rows = [Row(indicator=k, weight=v) for k, v in DEFAULT_WEIGHTS.items()]
    spark.createDataFrame(config_rows).write.mode("overwrite").saveAsTable(
        f"{catalog}.{GOLD_SCHEMA}.gold_scoring_config"
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold-scoring").getOrCreate()
    run_gold_scoring(spark)
```

**Step 2: Create job definition**

```yaml
# resources/jobs/compute_gold_scores.yml
resources:
  jobs:
    compute_gold_scores:
      name: "store-siting-compute-gold-scores"
      tasks:
        - task_key: run_gold_scoring
          python_wheel_task:
            package_name: store_siting
            entry_point: gold_scoring
          libraries:
            - whl: ../dist/*.whl
          existing_cluster_id: ${var.cluster_id}
```

**Step 3: Commit**

```bash
git add src/gold/compute_scores.py resources/jobs/compute_gold_scores.yml
git commit -m "feat: add Gold scoring job — join Silver, normalize, composite score"
```

---

## Phase 5: FastAPI Backend

### Task 12: FastAPI App Skeleton and County Endpoints

**Files:**
- Create: `src/app/backend/main.py`
- Create: `src/app/backend/routes/__init__.py`
- Create: `src/app/backend/routes/counties.py`
- Create: `src/app/backend/models/__init__.py`
- Create: `src/app/backend/models/county.py`
- Create: `src/app/backend/db.py`
- Create: `src/app/backend/requirements.txt`
- Test: `tests/unit/test_api_counties.py`

**Step 1: Write response models**

```python
# src/app/backend/models/county.py
from pydantic import BaseModel


class ComponentScores(BaseModel):
    building_permits: float
    net_migration: float
    vacancy_change: float
    employment_growth: float
    school_enrollment_growth: float
    ssp_projected_growth: float
    qsr_density_inv: float


class CountySummary(BaseModel):
    fips: str
    county_name: str
    state: str
    composite_score: float
    score_tier: str
    rank_national: int
    population: int | None = None
    median_income: float | None = None


class CountyDetail(CountySummary):
    component_scores: ComponentScores
    permits_per_1k_pop: float | None = None
    net_migration_rate: float | None = None
    vacancy_rate_yoy_change: float | None = None
    employment_growth_rate: float | None = None
    enrollment_growth_rate: float | None = None


class ScoringWeight(BaseModel):
    indicator: str
    weight: float


class ScoringWeightsUpdate(BaseModel):
    weights: list[ScoringWeight]
```

**Step 2: Write database connector**

```python
# src/app/backend/db.py
"""Databricks SQL Connector for the FastAPI app."""

import os
from databricks import sql as databricks_sql
from contextlib import contextmanager


def get_connection_params():
    return {
        "server_hostname": os.environ["DATABRICKS_HOST"],
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "access_token": os.environ.get("DATABRICKS_TOKEN", ""),
    }


@contextmanager
def get_cursor():
    params = get_connection_params()
    conn = databricks_sql.connect(**params)
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute_query(query: str, params: dict | None = None) -> list[dict]:
    with get_cursor() as cursor:
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
```

**Step 3: Write county routes**

```python
# src/app/backend/routes/counties.py
from fastapi import APIRouter, Query
from src.app.backend.db import execute_query
from src.app.backend.models.county import CountySummary, CountyDetail

router = APIRouter(prefix="/api")
CATALOG = os.environ.get("CATALOG", "store_siting")


import os

@router.get("/counties", response_model=list[CountySummary])
def list_counties(state: str | None = Query(None)):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"SELECT * FROM {catalog}.gold.gold_county_growth_score"
    if state:
        query += f" WHERE state = '{state}'"
    query += " ORDER BY composite_score DESC"
    rows = execute_query(query)
    return rows


@router.get("/counties/top", response_model=list[CountySummary])
def top_counties(n: int = Query(25), state: str | None = Query(None)):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"SELECT * FROM {catalog}.gold.gold_county_growth_score"
    if state:
        query += f" WHERE state = '{state}'"
    query += f" ORDER BY composite_score DESC LIMIT {n}"
    rows = execute_query(query)
    return rows


@router.get("/counties/{fips}", response_model=CountyDetail)
def get_county(fips: str):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"""
        SELECT s.*, d.*
        FROM {catalog}.gold.gold_county_growth_score s
        JOIN {catalog}.gold.gold_county_details d ON s.fips = d.fips
        WHERE s.fips = '{fips}'
    """
    rows = execute_query(query)
    if not rows:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"County {fips} not found")
    return rows[0]


@router.get("/trends/{fips}")
def get_trends(fips: str):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"""
        SELECT report_year, report_quarter,
               permits_per_1k_pop, net_migration_rate,
               vacancy_rate_yoy_change, employment_growth_rate
        FROM {catalog}.gold.gold_county_details
        WHERE fips = '{fips}'
        ORDER BY report_year, report_quarter
    """
    return execute_query(query)
```

**Step 4: Write GeoJSON and scoring routes**

```python
# src/app/backend/routes/geojson.py
import json
import os
from fastapi import APIRouter
from src.app.backend.db import execute_query

router = APIRouter(prefix="/api")

# Cache the GeoJSON in memory after first load
_geojson_cache: dict | None = None


@router.get("/geojson")
def get_geojson():
    global _geojson_cache

    catalog = os.environ.get("CATALOG", "store_siting")
    scores = execute_query(
        f"SELECT fips, composite_score, score_tier, rank_national, population "
        f"FROM {catalog}.gold.gold_county_growth_score"
    )
    score_lookup = {r["fips"]: r for r in scores}

    if _geojson_cache is None:
        geojson_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "..", "data", "county_geojson", "us-counties.json"
        )
        with open(geojson_path) as f:
            _geojson_cache = json.load(f)

    # Enrich features with scores
    for feature in _geojson_cache.get("features", []):
        fips = feature.get("properties", {}).get("GEOID", "")
        if fips in score_lookup:
            feature["properties"].update(score_lookup[fips])

    return _geojson_cache
```

```python
# src/app/backend/routes/scoring.py
import os
from fastapi import APIRouter
from src.app.backend.db import execute_query
from src.app.backend.models.county import ScoringWeight, ScoringWeightsUpdate

router = APIRouter(prefix="/api")


@router.get("/scores/weights", response_model=list[ScoringWeight])
def get_weights():
    catalog = os.environ.get("CATALOG", "store_siting")
    return execute_query(f"SELECT * FROM {catalog}.gold.gold_scoring_config")


@router.put("/scores/weights")
def update_weights(payload: ScoringWeightsUpdate):
    catalog = os.environ.get("CATALOG", "store_siting")
    # Update config table
    for w in payload.weights:
        execute_query(
            f"UPDATE {catalog}.gold.gold_scoring_config "
            f"SET weight = {w.weight} WHERE indicator = '{w.indicator}'"
        )
    return {"status": "updated", "message": "Re-run gold scoring job to apply new weights."}
```

**Step 5: Write main.py**

```python
# src/app/backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI(title="Store Siting API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from src.app.backend.routes.counties import router as counties_router
from src.app.backend.routes.geojson import router as geojson_router
from src.app.backend.routes.scoring import router as scoring_router

app.include_router(counties_router)
app.include_router(geojson_router)
app.include_router(scoring_router)

# Serve React static files in production
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.isdir(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
```

**Step 6: Write backend requirements.txt**

```
fastapi>=0.110.0
uvicorn>=0.27.0
pydantic>=2.6.0
databricks-sql-connector>=3.1.0
```

**Step 7: Write failing test**

```python
# tests/unit/test_api_counties.py
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.app.backend.main import app

client = TestClient(app)


@patch("src.app.backend.routes.counties.execute_query")
def test_list_counties(mock_query):
    mock_query.return_value = [
        {"fips": "12086", "county_name": "Miami-Dade", "state": "FL",
         "composite_score": 85.5, "score_tier": "A", "rank_national": 12,
         "population": 2800000, "median_income": 55000.0}
    ]
    response = client.get("/api/counties")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["fips"] == "12086"


@patch("src.app.backend.routes.counties.execute_query")
def test_top_counties_default_25(mock_query):
    mock_query.return_value = []
    response = client.get("/api/counties/top")
    assert response.status_code == 200
    call_args = mock_query.call_args[0][0]
    assert "LIMIT 25" in call_args


@patch("src.app.backend.routes.counties.execute_query")
def test_get_county_not_found(mock_query):
    mock_query.return_value = []
    response = client.get("/api/counties/99999")
    assert response.status_code == 404
```

**Step 8: Run tests**

Run: `python -m pytest tests/unit/test_api_counties.py -v`
Expected: All PASS

**Step 9: Commit**

```bash
git add src/app/backend/ tests/unit/test_api_counties.py
git commit -m "feat: add FastAPI backend — county, geojson, scoring endpoints"
```

---

## Phase 6: React Frontend

### Task 13: React Project Setup

**Files:**
- Create: `src/app/frontend/package.json`
- Create: `src/app/frontend/tsconfig.json`
- Create: `src/app/frontend/vite.config.ts`
- Create: `src/app/frontend/index.html`
- Create: `src/app/frontend/src/main.tsx`
- Create: `src/app/frontend/src/App.tsx`
- Create: `src/app/frontend/src/api/client.ts`

**Step 1: Initialize React project**

```bash
cd src/app/frontend && npm create vite@latest . -- --template react-ts
npm install deck.gl @deck.gl/react @deck.gl/layers @deck.gl/geo-layers react-map-gl maplibre-gl
npm install recharts
npm install @types/react @types/react-dom
```

**Step 2: Create API client**

```typescript
// src/app/frontend/src/api/client.ts
const BASE_URL = "/api";

export interface CountySummary {
  fips: string;
  county_name: string;
  state: string;
  composite_score: number;
  score_tier: string;
  rank_national: number;
  population?: number;
  median_income?: number;
}

export interface ComponentScores {
  building_permits: number;
  net_migration: number;
  vacancy_change: number;
  employment_growth: number;
  school_enrollment_growth: number;
  ssp_projected_growth: number;
  qsr_density_inv: number;
}

export interface CountyDetail extends CountySummary {
  component_scores: ComponentScores;
  permits_per_1k_pop?: number;
  net_migration_rate?: number;
  vacancy_rate_yoy_change?: number;
  employment_growth_rate?: number;
  enrollment_growth_rate?: number;
}

export interface ScoringWeight {
  indicator: string;
  weight: number;
}

export interface TrendPoint {
  report_year: number;
  report_quarter: number;
  permits_per_1k_pop?: number;
  net_migration_rate?: number;
  vacancy_rate_yoy_change?: number;
  employment_growth_rate?: number;
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const api = {
  getCounties: (state?: string) =>
    fetchJson<CountySummary[]>(`/counties${state ? `?state=${state}` : ""}`),

  getTopCounties: (n = 25, state?: string) =>
    fetchJson<CountySummary[]>(`/counties/top?n=${n}${state ? `&state=${state}` : ""}`),

  getCounty: (fips: string) =>
    fetchJson<CountyDetail>(`/counties/${fips}`),

  getGeoJson: () => fetchJson<GeoJSON.FeatureCollection>("/geojson"),

  getTrends: (fips: string) =>
    fetchJson<TrendPoint[]>(`/trends/${fips}`),

  getWeights: () =>
    fetchJson<ScoringWeight[]>("/scores/weights"),

  updateWeights: (weights: ScoringWeight[]) =>
    fetch(`${BASE_URL}/scores/weights`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ weights }),
    }),
};
```

**Step 3: Create App.tsx shell**

```tsx
// src/app/frontend/src/App.tsx
import { useState } from "react";
import { NationalMap } from "./components/NationalMap";
import { CountyDetail } from "./components/CountyDetail";
import { WeightTuner } from "./components/WeightTuner";
import "./App.css";

export default function App() {
  const [selectedFips, setSelectedFips] = useState<string | null>(null);
  const [showWeightTuner, setShowWeightTuner] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Store Siting — Growth Score Explorer</h1>
        <button onClick={() => setShowWeightTuner(!showWeightTuner)}>
          {showWeightTuner ? "Hide Weights" : "Adjust Weights"}
        </button>
      </header>

      <div className="app-body">
        <div className="map-container">
          <NationalMap
            onSelectCounty={setSelectedFips}
            refreshKey={refreshKey}
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

**Step 4: Commit**

```bash
git add src/app/frontend/
git commit -m "feat: scaffold React frontend with API client and App shell"
```

---

### Task 14: National Map Component (Choropleth)

**Files:**
- Create: `src/app/frontend/src/components/NationalMap.tsx`

**Step 1: Implement choropleth map**

```tsx
// src/app/frontend/src/components/NationalMap.tsx
import { useEffect, useState, useCallback } from "react";
import DeckGL from "@deck.gl/react";
import { GeoJsonLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { api } from "../api/client";

const INITIAL_VIEW = {
  longitude: -98.5,
  latitude: 39.8,
  zoom: 4,
  pitch: 0,
  bearing: 0,
};

const TIER_COLORS: Record<string, [number, number, number, number]> = {
  A: [34, 139, 34, 200],   // green
  B: [144, 238, 144, 200], // light green
  C: [255, 215, 0, 200],   // yellow
  D: [255, 140, 0, 200],   // orange
  F: [220, 20, 60, 200],   // red
};

interface Props {
  onSelectCounty: (fips: string) => void;
  refreshKey: number;
}

export function NationalMap({ onSelectCounty, refreshKey }: Props) {
  const [geojson, setGeojson] = useState<GeoJSON.FeatureCollection | null>(null);
  const [hovered, setHovered] = useState<any>(null);
  const [stateFilter, setStateFilter] = useState<string>("");
  const [tierFilter, setTierFilter] = useState<string>("");
  const [minScore, setMinScore] = useState<number>(0);

  useEffect(() => {
    api.getGeoJson().then(setGeojson);
  }, [refreshKey]);

  const layers = geojson
    ? [
        new GeoJsonLayer({
          id: "counties",
          data: geojson,
          filled: true,
          stroked: true,
          getLineColor: [100, 100, 100, 80],
          lineWidthMinPixels: 0.5,
          getFillColor: (f: any) => {
            const props = f.properties || {};
            const tier = props.score_tier || "F";
            const score = props.composite_score || 0;

            if (stateFilter && props.STATE !== stateFilter) return [200, 200, 200, 50];
            if (tierFilter && tier !== tierFilter) return [200, 200, 200, 50];
            if (score < minScore) return [200, 200, 200, 50];

            return TIER_COLORS[tier] || TIER_COLORS.F;
          },
          pickable: true,
          onHover: (info: any) => setHovered(info.object ? info : null),
          onClick: (info: any) => {
            const fips = info.object?.properties?.GEOID;
            if (fips) onSelectCounty(fips);
          },
          updateTriggers: {
            getFillColor: [stateFilter, tierFilter, minScore],
          },
        }),
      ]
    : [];

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <div className="map-filters">
        <select value={stateFilter} onChange={(e) => setStateFilter(e.target.value)}>
          <option value="">All States</option>
          {/* State options populated from data */}
        </select>
        <select value={tierFilter} onChange={(e) => setTierFilter(e.target.value)}>
          <option value="">All Tiers</option>
          {["A", "B", "C", "D", "F"].map((t) => (
            <option key={t} value={t}>Tier {t}</option>
          ))}
        </select>
        <label>
          Min Score: {minScore}
          <input
            type="range" min={0} max={100} value={minScore}
            onChange={(e) => setMinScore(Number(e.target.value))}
          />
        </label>
      </div>

      <DeckGL initialViewState={INITIAL_VIEW} controller layers={layers}>
        <Map mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json" />
      </DeckGL>

      {hovered && hovered.object && (
        <div
          className="tooltip"
          style={{ left: hovered.x + 10, top: hovered.y + 10 }}
        >
          <strong>{hovered.object.properties.NAME}</strong>
          <br />
          Score: {hovered.object.properties.composite_score?.toFixed(1) ?? "N/A"}
          <br />
          Tier: {hovered.object.properties.score_tier ?? "N/A"}
          <br />
          Rank: #{hovered.object.properties.rank_national ?? "N/A"}
        </div>
      )}
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/NationalMap.tsx
git commit -m "feat: add county choropleth map component with filters and tooltips"
```

---

### Task 15: County Detail Component

**Files:**
- Create: `src/app/frontend/src/components/CountyDetail.tsx`

**Step 1: Implement detail panel with radar chart and sparklines**

```tsx
// src/app/frontend/src/components/CountyDetail.tsx
import { useEffect, useState } from "react";
import {
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
} from "recharts";
import { api, CountyDetail as CountyDetailType, TrendPoint } from "../api/client";

interface Props {
  fips: string;
  onClose: () => void;
}

const INDICATOR_LABELS: Record<string, string> = {
  building_permits: "Building Permits",
  net_migration: "Net Migration",
  vacancy_change: "Vacancy Change",
  employment_growth: "Employment Growth",
  school_enrollment_growth: "School Enrollment",
  ssp_projected_growth: "SSP Projections",
  qsr_density_inv: "QSR White Space",
};

const TIER_BADGE_COLORS: Record<string, string> = {
  A: "#228B22", B: "#90EE90", C: "#FFD700", D: "#FF8C00", F: "#DC143C",
};

export function CountyDetail({ fips, onClose }: Props) {
  const [detail, setDetail] = useState<CountyDetailType | null>(null);
  const [trends, setTrends] = useState<TrendPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([api.getCounty(fips), api.getTrends(fips)]).then(
      ([d, t]) => {
        setDetail(d);
        setTrends(t);
        setLoading(false);
      }
    );
  }, [fips]);

  if (loading || !detail) return <div className="loading">Loading...</div>;

  const radarData = Object.entries(detail.component_scores).map(([key, value]) => ({
    indicator: INDICATOR_LABELS[key] || key,
    value: (value as number) * 100,
  }));

  const trendData = trends.map((t) => ({
    period: `${t.report_year} Q${t.report_quarter}`,
    ...t,
  }));

  return (
    <div className="county-detail">
      <div className="detail-header">
        <div>
          <h2>{detail.county_name}, {detail.state}</h2>
          <span
            className="tier-badge"
            style={{ backgroundColor: TIER_BADGE_COLORS[detail.score_tier] }}
          >
            Tier {detail.score_tier}
          </span>
          <span className="rank">Rank #{detail.rank_national}</span>
        </div>
        <button className="close-btn" onClick={onClose}>x</button>
      </div>

      <div className="score-display">
        <span className="big-score">{detail.composite_score.toFixed(1)}</span>
        <span className="score-label">/ 100</span>
      </div>

      <div className="metric-cards">
        <div className="card">
          <div className="card-value">{detail.population?.toLocaleString() ?? "N/A"}</div>
          <div className="card-label">Population</div>
        </div>
        <div className="card">
          <div className="card-value">
            ${detail.median_income?.toLocaleString() ?? "N/A"}
          </div>
          <div className="card-label">Median Income</div>
        </div>
        <div className="card">
          <div className="card-value">{detail.permits_per_1k_pop?.toFixed(1) ?? "N/A"}</div>
          <div className="card-label">Permits / 1K Pop</div>
        </div>
        <div className="card">
          <div className="card-value">{detail.net_migration_rate?.toFixed(2) ?? "N/A"}</div>
          <div className="card-label">Net Migration Rate</div>
        </div>
      </div>

      <h3>Component Scores</h3>
      <ResponsiveContainer width="100%" height={250}>
        <RadarChart data={radarData}>
          <PolarGrid />
          <PolarAngleAxis dataKey="indicator" tick={{ fontSize: 10 }} />
          <PolarRadiusAxis domain={[0, 100]} />
          <Radar dataKey="value" stroke="#4A90D9" fill="#4A90D9" fillOpacity={0.3} />
        </RadarChart>
      </ResponsiveContainer>

      {trendData.length > 0 && (
        <>
          <h3>Historical Trends</h3>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={trendData}>
              <XAxis dataKey="period" tick={{ fontSize: 10 }} />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="permits_per_1k_pop" stroke="#228B22" name="Permits" dot={false} />
              <Line type="monotone" dataKey="net_migration_rate" stroke="#4A90D9" name="Migration" dot={false} />
              <Line type="monotone" dataKey="employment_growth_rate" stroke="#FF8C00" name="Employment" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/CountyDetail.tsx
git commit -m "feat: add county detail panel with radar chart and trend sparklines"
```

---

### Task 16: Weight Tuner Component

**Files:**
- Create: `src/app/frontend/src/components/WeightTuner.tsx`

**Step 1: Implement weight sliders**

```tsx
// src/app/frontend/src/components/WeightTuner.tsx
import { useEffect, useState } from "react";
import { api, ScoringWeight } from "../api/client";

const INDICATOR_LABELS: Record<string, string> = {
  building_permits: "Building Permits",
  net_migration: "Net Migration",
  vacancy_change: "Vacancy Change",
  employment_growth: "Employment Growth",
  school_enrollment_growth: "School Enrollment",
  ssp_projected_growth: "SSP Projections",
  qsr_density_inv: "QSR White Space",
};

interface Props {
  onRecalculate: () => void;
}

export function WeightTuner({ onRecalculate }: Props) {
  const [weights, setWeights] = useState<ScoringWeight[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.getWeights().then(setWeights);
  }, []);

  const total = weights.reduce((sum, w) => sum + w.weight, 0);
  const isValid = Math.abs(total - 1.0) < 0.01;

  const handleChange = (indicator: string, value: number) => {
    setWeights((prev) =>
      prev.map((w) => (w.indicator === indicator ? { ...w, weight: value } : w))
    );
  };

  const handleRecalculate = async () => {
    if (!isValid) return;
    setSaving(true);
    await api.updateWeights(weights);
    setSaving(false);
    onRecalculate();
  };

  return (
    <div className="weight-tuner">
      <h3>Scoring Weights</h3>
      <p className={`total ${isValid ? "valid" : "invalid"}`}>
        Total: {(total * 100).toFixed(0)}% {isValid ? "" : "(must equal 100%)"}
      </p>

      {weights.map((w) => (
        <div key={w.indicator} className="weight-slider">
          <label>
            {INDICATOR_LABELS[w.indicator] || w.indicator}:
            {(w.weight * 100).toFixed(0)}%
          </label>
          <input
            type="range"
            min={0}
            max={0.5}
            step={0.01}
            value={w.weight}
            onChange={(e) => handleChange(w.indicator, Number(e.target.value))}
          />
        </div>
      ))}

      <button
        className="recalculate-btn"
        onClick={handleRecalculate}
        disabled={!isValid || saving}
      >
        {saving ? "Saving..." : "Recalculate Scores"}
      </button>
    </div>
  );
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/components/WeightTuner.tsx
git commit -m "feat: add weight tuner component with sliders and validation"
```

---

### Task 17: Frontend Styles and Polish

**Files:**
- Create: `src/app/frontend/src/App.css`

**Step 1: Write CSS**

```css
/* src/app/frontend/src/App.css */
* { margin: 0; padding: 0; box-sizing: border-box; }

.app {
  height: 100vh;
  display: flex;
  flex-direction: column;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}

.app-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 24px;
  background: #1a1a2e;
  color: white;
}

.app-header h1 { font-size: 18px; font-weight: 600; }

.app-header button {
  padding: 8px 16px;
  border: 1px solid rgba(255,255,255,0.3);
  background: transparent;
  color: white;
  border-radius: 6px;
  cursor: pointer;
}

.app-body {
  flex: 1;
  display: flex;
  position: relative;
  overflow: hidden;
}

.map-container { flex: 1; position: relative; }

.map-filters {
  position: absolute;
  top: 12px;
  left: 12px;
  z-index: 10;
  display: flex;
  gap: 8px;
  background: white;
  padding: 8px 12px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}

.map-filters select, .map-filters input {
  padding: 4px 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
}

.tooltip {
  position: absolute;
  z-index: 100;
  background: white;
  padding: 8px 12px;
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.2);
  font-size: 13px;
  pointer-events: none;
}

.detail-panel {
  width: 400px;
  background: white;
  border-left: 1px solid #eee;
  overflow-y: auto;
  padding: 20px;
}

.weight-panel {
  width: 300px;
  background: #f8f9fa;
  border-left: 1px solid #eee;
  overflow-y: auto;
  padding: 20px;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
}

.detail-header h2 { font-size: 18px; margin-bottom: 8px; }

.tier-badge {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 12px;
  color: white;
  font-weight: 700;
  font-size: 13px;
  margin-right: 8px;
}

.rank { color: #666; font-size: 14px; }

.close-btn {
  background: none;
  border: none;
  font-size: 20px;
  cursor: pointer;
  color: #999;
}

.score-display {
  text-align: center;
  margin: 16px 0;
}

.big-score { font-size: 48px; font-weight: 700; color: #1a1a2e; }
.score-label { font-size: 18px; color: #999; }

.metric-cards {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin: 16px 0;
}

.card {
  background: #f8f9fa;
  padding: 12px;
  border-radius: 8px;
  text-align: center;
}

.card-value { font-size: 18px; font-weight: 600; color: #1a1a2e; }
.card-label { font-size: 12px; color: #666; margin-top: 4px; }

.weight-tuner h3 { margin-bottom: 12px; }

.weight-slider {
  margin-bottom: 12px;
}

.weight-slider label {
  display: block;
  font-size: 13px;
  margin-bottom: 4px;
}

.weight-slider input[type="range"] { width: 100%; }

.total { font-size: 14px; margin-bottom: 16px; font-weight: 600; }
.total.valid { color: #228B22; }
.total.invalid { color: #DC143C; }

.recalculate-btn {
  width: 100%;
  padding: 10px;
  background: #4A90D9;
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  cursor: pointer;
  margin-top: 12px;
}

.recalculate-btn:disabled {
  background: #ccc;
  cursor: not-allowed;
}

.loading {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #999;
}
```

**Step 2: Commit**

```bash
git add src/app/frontend/src/App.css
git commit -m "feat: add frontend styles for map, detail panel, and weight tuner"
```

---

## Phase 7: GeoJSON Data & DABs App Config

### Task 18: Download and Simplify County GeoJSON

**Files:**
- Create: `scripts/download_geojson.py`
- Create: `data/county_geojson/us-counties.json`

**Step 1: Write download script**

```python
# scripts/download_geojson.py
"""Download and simplify US county boundaries from Census TIGER/Line."""

import json
import requests
import sys

# Census Bureau provides TopoJSON via their cartographic boundary files
# Using the 500k resolution (good balance of detail vs size)
URL = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"


def download_counties(output_path: str):
    print(f"Downloading county GeoJSON from {URL}...")
    response = requests.get(URL, timeout=120)
    response.raise_for_status()

    geojson = response.json()
    print(f"Downloaded {len(geojson['features'])} county features")

    # Ensure each feature has GEOID property for FIPS matching
    for feature in geojson["features"]:
        props = feature.get("properties", {})
        if "GEO_ID" in props and "GEOID" not in props:
            # Extract FIPS from GEO_ID format "0500000US01001"
            props["GEOID"] = props["GEO_ID"].split("US")[-1]

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    print(f"Saved to {output_path}")


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "data/county_geojson/us-counties.json"
    download_counties(output)
```

**Step 2: Run the script**

```bash
python scripts/download_geojson.py
```

**Step 3: Commit (track the script, .gitignore the large GeoJSON)**

```bash
echo "data/county_geojson/us-counties.json" >> .gitignore
git add scripts/download_geojson.py .gitignore
git commit -m "feat: add county GeoJSON download script"
```

---

### Task 19: DABs App Resource and Ingestion Job Definitions

**Files:**
- Create: `resources/app.yml`
- Create: `resources/jobs/ingest_building_permits.yml`
- Create: `resources/jobs/ingest_migration.yml`
- Create: `resources/jobs/ingest_hud_construction.yml`
- Create: `resources/jobs/ingest_vacancy.yml`
- Create: `resources/jobs/ingest_employment.yml`
- Create: `resources/jobs/ingest_school_enrollment.yml`
- Create: `resources/jobs/ingest_acs_demographics.yml`
- Create: `resources/jobs/ingest_business_patterns.yml`
- Create: `resources/jobs/ingest_ssp_projections.yml`
- Create: `resources/jobs/ingest_national_projections.yml`

**Step 1: Create app resource**

```yaml
# resources/app.yml
resources:
  apps:
    store_siting_app:
      name: "store-siting"
      description: "QSR Store Siting — Growth Score Explorer"
      source_code_path: src/app
      config:
        command:
          - uvicorn
          - src.app.backend.main:app
          - --host
          - 0.0.0.0
          - --port
          - "8000"
        env:
          - name: DATABRICKS_HOST
            value: ${var.workspace_host}
          - name: DATABRICKS_HTTP_PATH
            value: /sql/1.0/warehouses/${var.warehouse_id}
          - name: CATALOG
            value: ${var.catalog}
```

**Step 2: Create one representative ingestion job (others follow same pattern)**

```yaml
# resources/jobs/ingest_building_permits.yml
resources:
  jobs:
    ingest_building_permits:
      name: "store-siting-ingest-building-permits"
      schedule:
        quartz_cron_expression: "0 0 6 1 * ?"  # 1st of each month at 6am
        timezone_id: "America/New_York"
      tasks:
        - task_key: ingest
          python_wheel_task:
            package_name: store_siting
            entry_point: ingest_building_permits
          libraries:
            - whl: ../dist/*.whl
          existing_cluster_id: ${var.cluster_id}
```

**Step 3: Create remaining job YAMLs** (same pattern, different schedules for quarterly/annual sources)

**Step 4: Commit**

```bash
git add resources/
git commit -m "feat: add DABs app resource and all ingestion job definitions"
```

---

## Phase 8: Integration Testing & Final Assembly

### Task 20: Integration Tests with Sample Data

**Files:**
- Create: `tests/integration/test_pipeline_e2e.py`

**Step 1: Write end-to-end pipeline test using local Spark**

```python
# tests/integration/test_pipeline_e2e.py
"""End-to-end test: Bronze fixtures -> Silver transforms -> Gold scoring."""

import pytest
from pyspark.sql import SparkSession
from src.silver.building_permits import transform_building_permits
from src.silver.migration import transform_migration
from src.gold.scoring import compute_composite_score, assign_tier, min_max_normalize


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("e2e-test").getOrCreate()


def test_full_scoring_pipeline(spark):
    """Test that Bronze -> Silver -> Gold scoring produces valid output."""
    # Bronze building permits
    bp_data = [
        ("12086", "Miami-Dade", "01/2025", 450, 330, 780),
        ("36061", "New York", "01/2025", 50, 910, 960),
        ("06037", "Los Angeles", "01/2025", 320, 692, 1012),
    ]
    bp_cols = ["fips", "county_name", "survey_date", "single_family_units",
               "multi_family_units", "total_units"]
    bronze_bp = spark.createDataFrame(bp_data, bp_cols)

    # Silver transform
    silver_bp = transform_building_permits(bronze_bp)
    assert silver_bp.count() == 3

    # Score computation
    indicators = {
        "building_permits": 0.8,
        "net_migration": 0.6,
        "vacancy_change": 0.7,
        "employment_growth": 0.5,
        "school_enrollment_growth": 0.9,
        "ssp_projected_growth": 0.4,
        "qsr_density_inv": 0.3,
    }
    score = compute_composite_score(indicators)
    assert 0 <= score <= 100
    tier = assign_tier(score)
    assert tier in ("A", "B", "C", "D", "F")


def test_normalization_produces_valid_range():
    values = [100, 200, 300, 400, 500]
    normalized = min_max_normalize(values)
    for v in normalized:
        assert 0.0 <= v <= 1.0
    assert normalized[0] == 0.0
    assert normalized[-1] == 1.0
```

**Step 2: Run integration tests**

Run: `python -m pytest tests/integration/test_pipeline_e2e.py -v`
Expected: All PASS

**Step 3: Commit**

```bash
git add tests/integration/test_pipeline_e2e.py
git commit -m "test: add end-to-end pipeline integration tests"
```

---

### Task 21: Frontend Tests

**Files:**
- Create: `src/app/frontend/src/components/__tests__/WeightTuner.test.tsx`

**Step 1: Set up Vitest**

```bash
cd src/app/frontend && npm install -D vitest @testing-library/react @testing-library/jest-dom jsdom
```

Add to `vite.config.ts`:
```typescript
/// <reference types="vitest" />
export default defineConfig({
  test: {
    globals: true,
    environment: 'jsdom',
  },
  // ...existing config
})
```

**Step 2: Write component test**

```tsx
// src/app/frontend/src/components/__tests__/WeightTuner.test.tsx
import { render, screen } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { WeightTuner } from "../WeightTuner";

// Mock the API
vi.mock("../../api/client", () => ({
  api: {
    getWeights: vi.fn().mockResolvedValue([
      { indicator: "building_permits", weight: 0.25 },
      { indicator: "net_migration", weight: 0.20 },
      { indicator: "vacancy_change", weight: 0.15 },
      { indicator: "employment_growth", weight: 0.15 },
      { indicator: "school_enrollment_growth", weight: 0.10 },
      { indicator: "ssp_projected_growth", weight: 0.10 },
      { indicator: "qsr_density_inv", weight: 0.05 },
    ]),
    updateWeights: vi.fn().mockResolvedValue({ ok: true }),
  },
}));

describe("WeightTuner", () => {
  it("renders heading", async () => {
    render(<WeightTuner onRecalculate={vi.fn()} />);
    expect(screen.getByText("Scoring Weights")).toBeInTheDocument();
  });

  it("shows recalculate button", async () => {
    render(<WeightTuner onRecalculate={vi.fn()} />);
    expect(screen.getByText("Recalculate Scores")).toBeInTheDocument();
  });
});
```

**Step 3: Run tests**

```bash
cd src/app/frontend && npx vitest run
```

**Step 4: Commit**

```bash
git add src/app/frontend/
git commit -m "test: add frontend component tests with Vitest"
```

---

### Task 22: Final DABs Validation and README

**Files:**
- Create: `README.md`

**Step 1: Write README**

```markdown
# Store Siting App

QSR store siting app that scores US counties by leading indicators of population growth.

## Quick Start

1. Configure Databricks CLI: `databricks configure`
2. Download county GeoJSON: `python scripts/download_geojson.py`
3. Deploy: `databricks bundle deploy -t dev`
4. Run ingestion jobs: `databricks bundle run ingest_building_permits -t dev`
5. Run Silver transforms: `databricks bundle run transform_silver -t dev`
6. Run Gold scoring: `databricks bundle run compute_gold_scores -t dev`
7. Access app at the Databricks Apps URL

## Architecture

Bronze (10 open data sources) -> Silver (county-level FIPS standardized) -> Gold (composite growth score) -> React + FastAPI App (choropleth map)

## Data Sources

- Census Building Permits (monthly)
- HUD Residential Construction Permits (quarterly)
- USPS Migration via HUD (quarterly)
- USPS Vacancy via HUD (quarterly)
- BLS QCEW Employment (quarterly)
- NCES School Enrollment (annual)
- ACS 1-Year Estimates (annual)
- Census County Business Patterns (annual)
- County-Level SSP Projections (periodic)
- Census National Population Projections (periodic)

## Testing

```bash
python -m pytest tests/ -v              # Python tests
cd src/app/frontend && npx vitest run   # Frontend tests
```
```

**Step 2: Validate DABs bundle**

```bash
databricks bundle validate -t dev
```

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add README with quick start and architecture overview"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-2 | Scaffolding, DABs config, common utilities |
| 2 | 3-5 | Bronze ingestion (10 data sources) |
| 3 | 6-9 | Silver transforms (standardize to county FIPS) |
| 4 | 10-11 | Gold scoring engine and Spark job |
| 5 | 12 | FastAPI backend (endpoints + DB connector) |
| 6 | 13-17 | React frontend (map, detail, weight tuner, styles) |
| 7 | 18-19 | GeoJSON data, DABs app + job configs |
| 8 | 20-22 | Integration tests, frontend tests, README |

**Total: 22 tasks across 8 phases.**
