"""Ingest Census ACS vacancy data into Bronze.

Uses the Census Bureau ACS 5-year estimates API to retrieve county-level
vacancy data (table B25002: Occupancy Status).

- B25002_001E = total housing units
- B25002_003E = vacant housing units
"""

import logging
import requests
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips

logger = logging.getLogger(__name__)

CENSUS_ACS_URL = (
    "https://api.census.gov/data/{year}/acs/acs5"
    "?get=B25002_001E,B25002_003E,NAME&for=county:*&in=state:*"
)


def download_and_parse(year: int, quarter: int) -> list[dict[str, Any]]:
    """Download vacancy data from Census ACS API and parse into rows.

    Uses year-1 since ACS data lags by one year.
    """
    acs_year = year - 1
    url = CENSUS_ACS_URL.format(year=acs_year)
    logger.info("Downloading Census ACS vacancy data from %s", url)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    data = response.json()
    # First row is headers: ['B25002_001E', 'B25002_003E', 'NAME', 'state', 'county']
    headers = data[0]
    rows: list[dict[str, Any]] = []

    for record in data[1:]:
        row = dict(zip(headers, record))
        state_fips = row["state"].zfill(2)
        county_fips = row["county"].zfill(3)
        fips = normalize_fips(f"{state_fips}{county_fips}")

        total = int(row["B25002_001E"])
        vacant = int(row["B25002_003E"])

        rows.append({
            "fips": fips,
            "report_year": year,
            "report_quarter": quarter,
            "total_addresses": total,
            "vacant_addresses": vacant,
            "data_source": "census_acs",
        })

    logger.info(
        "Downloaded %d county vacancy rows from Census ACS (%d) for %dQ%d",
        len(rows), acs_year, year, quarter,
    )
    return rows


# ---------------------------------------------------------------------------
# Spark ingestion entry point
# ---------------------------------------------------------------------------


def ingest(spark, year: int, quarter: int, catalog: str | None = None):
    """Ingest vacancy data into Bronze layer.

    Parameters
    ----------
    spark : SparkSession
    year : int
    quarter : int (1-4)
    catalog : str or None
        Unity Catalog name. Defaults to config.CATALOG if None.
    """
    from pyspark.sql.functions import lit, current_timestamp

    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year, quarter)
        if not rows:
            log_ingestion(
                spark, "vacancy", "success", 0,
                started_at=started_at, catalog=catalog,
            )
            return

        df = spark.createDataFrame(rows)
        quarter_end_month = quarter * 3
        df = (
            df.withColumn(
                "source_date", lit(date(year, quarter_end_month, 1))
            ).withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.vacancy")

        log_ingestion(
            spark, "vacancy", "success", len(rows),
            started_at=started_at, catalog=catalog,
        )
    except Exception as e:
        log_ingestion(
            spark, "vacancy", "failure",
            error_msg=str(e)[:500], started_at=started_at, catalog=catalog,
        )
        raise
