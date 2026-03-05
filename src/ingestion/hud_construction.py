"""Ingest construction permit data from Census Building Permits Survey into Bronze.

Uses the same Census BPS county-level flat files as building_permits.py,
but fetches the December file (month 12) which represents annual totals.

URL pattern:
  https://www2.census.gov/econ/bps/County/co{yy}12c.txt
"""

import csv
import logging
import os
import tempfile
from datetime import datetime, timezone, date
from typing import Any

import requests

from src.common.fips import normalize_fips

logger = logging.getLogger(__name__)


def _build_download_url(year: int) -> str:
    """Build the Census BPS annual (December) CSV download URL."""
    yy = str(year % 100).zfill(2)
    return f"https://www2.census.gov/econ/bps/County/co{yy}12c.txt"


def _parse_bps_csv(filepath: str, year: int, quarter: int) -> list[dict[str, Any]]:
    """Parse a Census building permits CSV (two-header-row format)."""
    rows: list[dict[str, Any]] = []
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip category header row
        next(reader)  # skip sub-header row
        for fields in reader:
            if len(fields) < 17 or not fields[0].strip():
                continue
            state_fips = fields[1].strip()
            county_fips = fields[2].strip().zfill(3)
            fips = normalize_fips(state_fips + county_fips)
            county_name = fields[5].strip()

            single_units = int(fields[7].strip() or 0)
            two_units = int(fields[10].strip() or 0)
            three_four_units = int(fields[13].strip() or 0)
            five_plus_units = int(fields[16].strip() or 0)
            total_units = single_units + two_units + three_four_units + five_plus_units

            rows.append({
                "fips": fips,
                "county_name": county_name,
                "report_year": year,
                "report_quarter": quarter,
                "permitted_units": total_units,
                "data_source": "census_bps",
            })
    return rows


def _download_and_parse(year: int, quarter: int) -> list[dict[str, Any]]:
    """Download and parse annual building permits data."""
    url = _build_download_url(year)
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return _parse_bps_csv(tmp_path, year, quarter)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, quarter: int, catalog: str | None = None):
    """Ingest construction permit data into Bronze."""
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = _download_and_parse(year, quarter)
        if not rows:
            log_ingestion(spark, "hud_construction", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, quarter * 3, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.hud_construction")

        log_ingestion(spark, "hud_construction", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "hud_construction", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
