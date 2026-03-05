"""Ingest NCES school enrollment data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int) -> str:
    """Build the NCES school enrollment data download URL."""
    return f"https://nces.ed.gov/ccd/data/csv/ccd_sch_enrollment_{year}.csv"


def parse_school_enrollment_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse school enrollment CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            rows.append({
                "fips": fips,
                "district_id": record["district_id"].strip(),
                "district_name": record["district_name"].strip(),
                "state": record["state"].strip(),
                "report_year": int(record["year"].strip()),
                "total_enrollment": int(record["total_enrollment"].strip()),
            })
    return rows


def download_and_parse(year: int) -> list[dict[str, Any]]:
    url = build_download_url(year)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return parse_school_enrollment_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, catalog: str = "store_siting"):
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year)
        if not rows:
            log_ingestion(spark, "school_enrollment", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, 1, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.school_enrollment")

        log_ingestion(spark, "school_enrollment", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "school_enrollment", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
