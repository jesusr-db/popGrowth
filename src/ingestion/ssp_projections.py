"""Ingest SSP population projection data into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(scenario: str = "SSP2") -> str:
    """Build the SSP projections data download URL."""
    return f"https://sedac.ciesin.columbia.edu/data/set/popdynamics-us-county-level-ssp/{scenario}.csv"


def parse_ssp_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse SSP projections CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "projection_year": int(record["projection_year"].strip()),
                "scenario": record["scenario"].strip(),
                "projected_population": int(record["projected_population"].strip()),
            })
    return rows


def download_and_parse(scenario: str = "SSP2") -> list[dict[str, Any]]:
    url = build_download_url(scenario)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(response.text)
        tmp_path = f.name

    try:
        return parse_ssp_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, scenario: str = "SSP2", catalog: str = "store_siting"):
    from pyspark.sql.functions import lit, current_timestamp
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(scenario)
        if not rows:
            log_ingestion(spark, "ssp_projections", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = df.withColumn("ingested_at", current_timestamp())
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.ssp_projections")

        log_ingestion(spark, "ssp_projections", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "ssp_projections", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
