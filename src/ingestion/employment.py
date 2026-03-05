"""Ingest BLS Quarterly Census of Employment and Wages data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int, quarter: int) -> str:
    """Build the BLS QCEW data download URL."""
    return f"https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_by_area.csv"


def parse_employment_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse employment CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "report_year": int(record["year"].strip()),
                "report_quarter": int(record["quarter"].strip()),
                "total_employment": int(record["total_employment"].strip()),
                "total_wages": int(record["total_wages"].strip()),
                "establishments": int(record["establishments"].strip()),
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
        return parse_employment_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, quarter: int, catalog: str | None = None):
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year, quarter)
        if not rows:
            log_ingestion(spark, "employment", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, quarter * 3, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.employment")

        log_ingestion(spark, "employment", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "employment", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
