"""Ingest Census County Business Patterns data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int) -> str:
    """Build the Census CBP data download URL."""
    return f"https://www2.census.gov/programs-surveys/cbp/datasets/{year}/cbp{str(year)[2:]}co.txt"


def parse_business_patterns_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse business patterns CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "naics_code": record["naics_code"].strip(),
                "establishments": int(record["establishments"].strip()),
                "employees": int(record["employees"].strip()),
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
        return parse_business_patterns_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, catalog: str = "store_siting"):
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year)
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
