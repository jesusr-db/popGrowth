"""Ingest USPS vacancy data via HUD into Bronze."""

import csv
import os
import requests
import tempfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips


def build_download_url(year: int, quarter: int) -> str:
    """Build the HUD vacancy data download URL."""
    return f"https://www.huduser.gov/portal/datasets/usps/USPS_Vacancy_{year}q{quarter}.csv"


def parse_vacancy_csv(filepath: str) -> list[dict[str, Any]]:
    """Parse vacancy CSV into list of dicts."""
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            fips = normalize_fips(record["county_fips"].strip())
            total = int(record["total_addresses"].strip())
            vacant = int(record["vacant_addresses"].strip())
            rate = round(vacant / total, 6) if total > 0 else 0.0
            rows.append({
                "fips": fips,
                "county_name": record["county_name"].strip(),
                "state": record["state"].strip(),
                "report_year": int(record["year"].strip()),
                "report_quarter": int(record["quarter"].strip()),
                "total_addresses": total,
                "vacant_addresses": vacant,
                "vacancy_rate": rate,
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
        return parse_vacancy_csv(tmp_path)
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
            log_ingestion(spark, "vacancy", "success", 0,
                         started_at=started_at, catalog=catalog)
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, quarter * 3, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.vacancy")

        log_ingestion(spark, "vacancy", "success", len(rows),
                     started_at=started_at, catalog=catalog)
    except Exception as e:
        log_ingestion(spark, "vacancy", "failure",
                     error_msg=str(e)[:500], started_at=started_at, catalog=catalog)
        raise
