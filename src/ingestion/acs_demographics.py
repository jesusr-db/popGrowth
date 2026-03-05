"""Ingest ACS demographic data from Census Bureau API into Bronze.

Uses the Census ACS 5-Year API (no key needed) to retrieve county-level
demographic variables:
  B01003_001E = total population
  B19013_001E = median household income
  B01002_001E = median age
  B11001_001E = total households

URL pattern:
  https://api.census.gov/data/{year}/acs/acs5?get=B01003_001E,B19013_001E,B01002_001E,B11001_001E,NAME&for=county:*&in=state:*
"""

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from src.common.fips import normalize_fips

logger = logging.getLogger(__name__)


def _fetch_demographics(year: int) -> list[dict[str, Any]]:
    """Fetch county-level demographic data from Census ACS API."""
    variables = "B01003_001E,B19013_001E,B01002_001E,B11001_001E,NAME"
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={variables}&for=county:*&in=state:*"
    )
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    data = response.json()

    header = data[0]
    rows: list[dict[str, Any]] = []
    for record in data[1:]:
        row = dict(zip(header, record))
        state_fips = row["state"]
        county_fips = row["county"]
        fips = normalize_fips(state_fips + county_fips)

        # Parse numeric fields, skipping rows with missing data
        try:
            population = int(row["B01003_001E"])
            median_income = int(row["B19013_001E"]) if row["B19013_001E"] not in (None, "", "-") else None
            median_age = float(row["B01002_001E"]) if row["B01002_001E"] not in (None, "", "-") else None
            households = int(row["B11001_001E"]) if row["B11001_001E"] not in (None, "", "-") else None
        except (ValueError, TypeError):
            continue

        # Extract county name from NAME field (format: "County Name, State")
        county_name = row.get("NAME", "")

        rows.append({
            "fips": fips,
            "report_year": year,
            "county_name": county_name,
            "population": population,
            "median_income": median_income,
            "median_age": median_age,
            "households": households,
            "data_source": "census_acs",
        })
    return rows


def ingest(spark, year: int, catalog: str | None = None):
    """Ingest ACS demographics data into Bronze."""
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = _fetch_demographics(year)
        if not rows:
            log_ingestion(spark, "acs_demographics", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_year", lit(year))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.acs_demographics")

        log_ingestion(spark, "acs_demographics", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "acs_demographics", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
