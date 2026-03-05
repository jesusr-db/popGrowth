"""Ingest IRS SOI Migration data into Bronze.

Downloads county-level migration inflow and outflow data from the IRS
Statistics of Income (SOI) program:
  https://www.irs.gov/statistics/soi-tax-stats-migration-data

CSV files follow the pattern:
  https://www.irs.gov/pub/irs-soi/countyinflow{yy}{yy2}.csv
  https://www.irs.gov/pub/irs-soi/countyoutflow{yy}{yy2}.csv

where yy is the 2-digit start year and yy2 = yy + 1.
"""

import io
from collections import defaultdict
from datetime import datetime, timezone, date
from typing import Any

import requests

from src.common.fips import normalize_fips


# ---------------------------------------------------------------------------
# IRS SOI download helpers
# ---------------------------------------------------------------------------

_IRS_BASE = "https://www.irs.gov/pub/irs-soi"

# State FIPS codes used for summary rows that should be skipped
_SUMMARY_STATE_FIPS = {"96", "97", "98"}


def _download_csv(url: str) -> str:
    """Download a CSV from the given URL and return its text content."""
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.text


def _parse_migration_csv(text: str) -> list[dict[str, str]]:
    """Parse an IRS SOI migration CSV into a list of row dicts."""
    import csv

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        # Normalize header keys to lowercase/stripped
        normed = {k.strip().lower(): v.strip() for k, v in row.items()}
        rows.append(normed)
    return rows


def _aggregate_flows(
    rows: list[dict[str, str]],
    fips_state_col: str,
    fips_county_col: str,
) -> dict[str, int]:
    """Aggregate n2 (exemptions / people) by county FIPS.

    Parameters
    ----------
    rows : parsed CSV rows
    fips_state_col : column name for the state FIPS to build the county key
    fips_county_col : column name for the county FIPS to build the county key

    Returns a dict mapping 5-digit FIPS -> total n2.
    """
    totals: dict[str, int] = defaultdict(int)
    for row in rows:
        state_fips = row.get(fips_state_col, "").strip()
        county_fips = row.get(fips_county_col, "").strip()

        # Skip summary rows
        if state_fips in _SUMMARY_STATE_FIPS:
            continue
        # Skip state-total rows (county code 000)
        if county_fips == "000":
            continue

        fips_raw = state_fips + county_fips
        try:
            fips = normalize_fips(fips_raw)
        except ValueError:
            continue

        n2_raw = row.get("n2", "0").strip()
        try:
            n2 = int(float(n2_raw))
        except (ValueError, TypeError):
            continue

        totals[fips] += n2

    return dict(totals)


def _build_migration_rows(
    inflow_by_fips: dict[str, int],
    outflow_by_fips: dict[str, int],
    year: int,
    quarter: int,
) -> list[dict[str, Any]]:
    """Combine inflow and outflow aggregates into output rows."""
    all_fips = sorted(set(inflow_by_fips) | set(outflow_by_fips))
    rows: list[dict[str, Any]] = []
    for fips in all_fips:
        inflow = inflow_by_fips.get(fips, 0)
        outflow = outflow_by_fips.get(fips, 0)
        rows.append({
            "fips": fips,
            "report_year": year,
            "report_quarter": quarter,
            "inflow": inflow,
            "outflow": outflow,
            "net_migration": inflow - outflow,
            "data_source": "irs_soi",
        })
    return rows


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def ingest(spark, year: int, quarter: int, catalog: str | None = None):
    """Download IRS SOI migration data and write to Bronze.

    Parameters
    ----------
    spark : SparkSession
    year : int – calendar year (e.g. 2023). The IRS file used will cover
        year-1 to year (e.g. countyinflow2223.csv).
    quarter : int – ignored for download but included in output rows.
    catalog : str | None – Unity Catalog name; defaults to config.CATALOG.
    """
    from pyspark.sql.functions import lit, current_timestamp

    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    try:
        yy_start = (year - 1) % 100
        yy_end = year % 100
        yy_start_str = f"{yy_start:02d}"
        yy_end_str = f"{yy_end:02d}"

        inflow_url = f"{_IRS_BASE}/countyinflow{yy_start_str}{yy_end_str}.csv"
        outflow_url = f"{_IRS_BASE}/countyoutflow{yy_start_str}{yy_end_str}.csv"

        inflow_text = _download_csv(inflow_url)
        outflow_text = _download_csv(outflow_url)

        inflow_rows = _parse_migration_csv(inflow_text)
        outflow_rows = _parse_migration_csv(outflow_text)

        # For inflow CSV: destination county is y2_statefips + y2_countyfips
        inflow_by_fips = _aggregate_flows(
            inflow_rows, "y2_statefips", "y2_countyfips"
        )
        # For outflow CSV: origin county is y1_statefips + y1_countyfips
        outflow_by_fips = _aggregate_flows(
            outflow_rows, "y1_statefips", "y1_countyfips"
        )

        rows = _build_migration_rows(inflow_by_fips, outflow_by_fips, year, quarter)

        if not rows:
            log_ingestion(
                spark, "migration", "success", 0,
                started_at=started_at, catalog=catalog,
            )
            return

        df = spark.createDataFrame(rows)
        df = (
            df.withColumn("source_date", lit(date(year, quarter * 3, 1)))
              .withColumn("ingested_at", current_timestamp())
        )
        df.write.mode("append").saveAsTable(f"{catalog}.bronze.migration")

        log_ingestion(
            spark, "migration", "success", len(rows),
            started_at=started_at, catalog=catalog,
        )
    except Exception as e:
        log_ingestion(
            spark, "migration", "failure",
            error_msg=str(e)[:500],
            started_at=started_at, catalog=catalog,
        )
        raise
