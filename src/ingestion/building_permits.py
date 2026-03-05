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
    yy = str(year % 100).zfill(2)
    mm = str(month).zfill(2)
    return f"https://www2.census.gov/econ/bps/County/co{yy}{mm}c.txt"


def parse_building_permits_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse a Census building permits CSV (two-header-row format)."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip category header row
        next(reader)  # skip sub-header row
        for fields in reader:
            if len(fields) < 17 or not fields[0].strip():
                continue  # skip blank lines
            survey_date = fields[0].strip()
            state_fips = fields[1].strip()
            county_fips = fields[2].strip().zfill(3)
            fips = normalize_fips(state_fips + county_fips)
            county_name = fields[5].strip()

            single_bldgs = int(fields[6].strip() or 0)
            single_units = int(fields[7].strip() or 0)
            two_units = int(fields[10].strip() or 0)
            three_four_units = int(fields[13].strip() or 0)
            five_plus_units = int(fields[16].strip() or 0)
            multi = two_units + three_four_units + five_plus_units
            total = single_units + multi

            rows.append({
                "fips": fips,
                "county_name": county_name,
                "survey_date": survey_date,
                "single_family_units": single_units,
                "multi_family_units": multi,
                "total_units": total,
                "single_family_bldgs": single_bldgs,
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


def ingest(spark, year: int, month: int, catalog: str | None = None):
    """Full ingestion: download, parse, write to Bronze Delta table."""
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
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
