"""Ingest Census County Business Patterns data from Census CBP API into Bronze.

Uses the Census CBP API to retrieve county-level business pattern data:
  ESTAB = number of establishments
  EMP = number of employees
  NAICS2017 = NAICS industry code

URL pattern:
  https://api.census.gov/data/{year}/cbp?get=ESTAB,EMP,NAICS2017&for=county:*&in=state:*
"""

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from src.common.fips import normalize_fips

logger = logging.getLogger(__name__)


def _fetch_business_patterns(year: int) -> list[dict[str, Any]]:
    """Fetch county-level business patterns from Census CBP API."""
    url = (
        f"https://api.census.gov/data/{year}/cbp"
        f"?get=ESTAB,EMP,NAICS2017&for=county:*&in=state:*"
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

        naics_code = row.get("NAICS2017", "")

        try:
            establishments = int(row["ESTAB"]) if row["ESTAB"] not in (None, "", "-") else 0
            employees = int(row["EMP"]) if row["EMP"] not in (None, "", "-") else 0
        except (ValueError, TypeError):
            continue

        rows.append({
            "fips": fips,
            "report_year": year,
            "naics_code": naics_code,
            "establishments": establishments,
            "employees": employees,
            "data_source": "census_cbp",
        })
    return rows


def ingest(spark, year: int, catalog: str | None = None):
    """Ingest County Business Patterns data into Bronze."""
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = _fetch_business_patterns(year)
        if not rows:
            log_ingestion(spark, "business_patterns", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_year", lit(year))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.business_patterns")

        log_ingestion(spark, "business_patterns", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "business_patterns", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
