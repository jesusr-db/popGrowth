"""Ingest Census national population projections data into Bronze.

The Census Bureau publishes state-level population projections, but
download URLs and file formats change between releases. This module
attempts to use the Census Population Projections API when available.

API endpoint (2017 vintage):
  https://api.census.gov/data/2017/popproj/pop?get=POP,YEAR&for=state:*
"""

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from src.common.fips import normalize_fips

logger = logging.getLogger(__name__)


def _fetch_projections(year: int) -> list[dict[str, Any]]:
    """Attempt to fetch national population projections from Census API.

    Tries the Census Population Projections API. If the API is not
    available for the requested vintage, raises NotImplementedError.
    """
    url = (
        f"https://api.census.gov/data/{year}/popproj/pop"
        f"?get=POP,YEAR,SEX&for=state:*"
    )
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise NotImplementedError(
            f"Census population projections API not available for vintage {year}. "
            f"HTTP error: {exc}. "
            "National projections require manual download from Census.gov: "
            "https://www.census.gov/programs-surveys/popproj.html"
        ) from exc

    data = response.json()
    header = data[0]
    rows: list[dict[str, Any]] = []
    for record in data[1:]:
        row = dict(zip(header, record))

        # Only keep total (both sexes combined, SEX=0)
        if row.get("SEX", "0") != "0":
            continue

        state_fips = row.get("state", "")
        if not state_fips:
            continue

        try:
            projected_population = int(row["POP"])
            projection_year = int(row["YEAR"])
        except (ValueError, TypeError, KeyError):
            continue

        rows.append({
            "state_fips": state_fips.zfill(2),
            "projection_year": projection_year,
            "projected_population": projected_population,
            "data_source": "census_popproj",
        })
    return rows


def ingest(spark, year: int = 2023, catalog: str | None = None):
    """Ingest national population projections into Bronze."""
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = _fetch_projections(year)
        if not rows:
            log_ingestion(spark, "national_projections", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = df.withColumn("ingested_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.national_projections")

        log_ingestion(spark, "national_projections", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except NotImplementedError:
        raise
    except Exception as e:
        log_ingestion(spark, "national_projections", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
