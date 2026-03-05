"""Ingest BLS Quarterly Census of Employment and Wages data into Bronze.

Downloads county-level employment data from the BLS QCEW program.
The bulk download is a ZIP archive containing a single CSV with columns
documented at https://data.bls.gov/cew/doc/layouts/csv_quarterly_layout.htm

NOTE: The BLS ZIP file is approximately 300 MB. Downloads may take several
minutes depending on network speed.
"""

import csv
import logging
import os
import requests
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone, date
from typing import Any

from src.common.fips import normalize_fips, validate_fips

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def build_download_url(year: int) -> str:
    """Return the BLS QCEW quarterly single-file ZIP URL for *year*."""
    return (
        f"https://data.bls.gov/cew/data/files/"
        f"{year}/csv/{year}_qtrly_singlefile.zip"
    )


# ---------------------------------------------------------------------------
# Real-data parser
# ---------------------------------------------------------------------------

# We keep only "total, all industries" rows at the county level.
# own_code  = "0"  (all ownerships)
# industry_code = "10" (total, all industries)
# agglvl_code   = "70" (county, total)
_FILTER_OWN_CODE = "0"
_FILTER_INDUSTRY_CODE = "10"
_FILTER_AGGLVL_CODE = "70"


def parse_employment_csv(
    filepath: str, quarter: int
) -> list[dict[str, Any]]:
    """Parse the QCEW quarterly CSV into county-level employment dicts.

    Filters rows to the specified *quarter* (1-4).

    Expected BLS column names (quoted in CSV):
        area_fips, own_code, industry_code, agglvl_code, size_code,
        year, qtr, disclosure_code, qtrly_estabs,
        month1_emplvl, month2_emplvl, month3_emplvl,
        total_qtrly_wages, ...
    """
    rows: list[dict[str, Any]] = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for record in reader:
            # Filter to county-level totals
            own_code = record.get("own_code", "").strip().strip('"')
            industry_code = record.get("industry_code", "").strip().strip('"')
            agglvl_code = record.get("agglvl_code", "").strip().strip('"')

            if (own_code != _FILTER_OWN_CODE
                    or industry_code != _FILTER_INDUSTRY_CODE
                    or agglvl_code != _FILTER_AGGLVL_CODE):
                continue

            # Filter to the requested quarter
            qtr = record.get("qtr", "").strip().strip('"')
            if qtr != str(quarter):
                continue

            raw_fips = record["area_fips"].strip().strip('"')
            if not raw_fips.isdigit():
                continue
            fips = normalize_fips(raw_fips)
            if not validate_fips(fips):
                continue

            # Average employment across the three months of the quarter
            m1 = int(record["month1_emplvl"].strip().strip('"') or 0)
            m2 = int(record["month2_emplvl"].strip().strip('"') or 0)
            m3 = int(record["month3_emplvl"].strip().strip('"') or 0)
            total_employment = round((m1 + m2 + m3) / 3)

            total_wages = int(record["total_qtrly_wages"].strip().strip('"') or 0)
            establishments = int(record["qtrly_estabs"].strip().strip('"') or 0)

            rows.append({
                "fips": fips,
                "report_year": int(record["year"].strip().strip('"')),
                "report_quarter": int(qtr),
                "total_employment": total_employment,
                "total_wages": total_wages,
                "establishments": establishments,
                "data_source": "bls_qcew",
            })
    return rows


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_and_parse(year: int, quarter: int) -> list[dict[str, Any]]:
    """Download the QCEW ZIP, extract the CSV, parse county rows.

    The BLS ZIP file is ~300 MB; the download timeout is set to 300 seconds.
    """
    url = build_download_url(year)
    logger.info(
        "Downloading QCEW data from %s (file is ~300 MB, this may take a while)",
        url,
    )
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    tmp_dir = tempfile.mkdtemp(prefix="qcew_")
    zip_path = os.path.join(tmp_dir, "qcew.zip")
    try:
        with open(zip_path, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise ValueError("No CSV file found in QCEW ZIP archive")
            csv_name = csv_names[0]
            extracted_path = zf.extract(csv_name, tmp_dir)

        return parse_employment_csv(extracted_path, quarter)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def ingest(spark, year: int, quarter: int, catalog: str | None = None):
    """Ingest county employment data into ``<catalog>.bronze.employment``.

    Downloads real BLS QCEW data. If the download or parsing fails the
    error is propagated to the caller.
    """
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
