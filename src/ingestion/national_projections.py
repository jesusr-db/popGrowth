"""Ingest Census national population projections data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone
from typing import Any


def build_download_url(year: int = 2023) -> str:
    """Build the Census national projections data download URL."""
    return f"https://www2.census.gov/programs-surveys/popproj/datasets/{year}/np{year}_d1.csv"


def parse_national_projections_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse national projections CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            state_fips = record["state_fips"].strip().zfill(2)
            rows.append({
                "state_fips": state_fips,
                "state_name": record["state_name"].strip(),
                "projection_year": int(record["projection_year"].strip()),
                "projected_population": int(record["projected_population"].strip()),
            })
    return rows


def download_and_parse(year: int = 2023) -> list[dict[str, Any]]:
    url = build_download_url(year)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return parse_national_projections_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int = 2023, catalog: str = "store_siting"):
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year)
        if not rows:
            log_ingestion(spark, "national_projections", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = df.withColumn("ingested_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.national_projections")

        log_ingestion(spark, "national_projections", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "national_projections", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
