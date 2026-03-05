"""Ingest ACS demographic data from Census Bureau into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int) -> str:
    """Build the Census ACS data download URL."""
    return f"https://api.census.gov/data/{year}/acs/acs5/profile"


def parse_acs_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse ACS demographics CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "population": int(record["population"].strip()),
                "median_income": int(record["median_income"].strip()),
                "median_age": float(record["median_age"].strip()),
                "households": int(record["households"].strip()),
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
        return parse_acs_csv(tmp_path)
    finally:
        os.unlink(tmp_path)


def ingest(spark, year: int, catalog: str | None = None):
    from pyspark.sql.functions import lit, current_timestamp
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        rows = download_and_parse(year)
        if not rows:
            log_ingestion(spark, "acs_demographics", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_year", lit(year))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.acs_demographics")

        log_ingestion(spark, "acs_demographics", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "acs_demographics", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
