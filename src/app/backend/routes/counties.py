import os
import re
from fastapi import APIRouter, Query, HTTPException
from backend.db import execute_query
from backend.models.county import CountySummary, CountyDetail

router = APIRouter(prefix="/api")

# Simple validators to prevent SQL injection (catalog is from env, not user input)
_FIPS_RE = re.compile(r"^\d{5}$")
_STATE_RE = re.compile(r"^[A-Za-z]{2}$")


@router.get("/counties", response_model=list[CountySummary])
def list_counties(state: str | None = Query(None)):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"SELECT * FROM {catalog}.gold.gold_county_growth_score"
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += f" WHERE state = '{state}'"
    query += " ORDER BY composite_score DESC"
    return execute_query(query)


@router.get("/counties/top", response_model=list[CountySummary])
def top_counties(n: int = Query(25, ge=1, le=500), state: str | None = Query(None)):
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"SELECT * FROM {catalog}.gold.gold_county_growth_score"
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += f" WHERE state = '{state}'"
    query += f" ORDER BY composite_score DESC LIMIT {n}"
    return execute_query(query)


@router.get("/counties/{fips}")
def get_county(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    catalog = os.environ.get("CATALOG", "store_siting")
    # Get scored data
    score_rows = execute_query(
        f"SELECT * FROM {catalog}.gold.gold_county_growth_score WHERE fips = '{fips}'"
    )
    if not score_rows:
        raise HTTPException(status_code=404, detail=f"County {fips} not found")
    row = score_rows[0]

    # Get detail data
    detail_rows = execute_query(
        f"SELECT * FROM {catalog}.gold.gold_county_details WHERE fips = '{fips}'"
    )
    if detail_rows:
        row.update({k: v for k, v in detail_rows[0].items() if k != "fips" and v is not None})

    # Parse component_scores struct if it's a string
    cs = row.get("component_scores")
    if isinstance(cs, str):
        import json
        try:
            row["component_scores"] = json.loads(cs)
        except (json.JSONDecodeError, TypeError):
            row["component_scores"] = None

    return row


@router.get("/trends/{fips}")
def get_trends(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    # With single-year data per county, no time series to return yet
    return []
