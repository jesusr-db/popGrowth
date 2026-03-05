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
    return f"https://www2.census.gov/econ/bps/County/co{year}{mm}a.txt"


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
