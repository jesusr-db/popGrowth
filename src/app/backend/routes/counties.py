import os
import re
from fastapi import APIRouter, Query, HTTPException
from src.app.backend.db import execute_query
from src.app.backend.models.county import CountySummary, CountyDetail

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


@router.get("/counties/{fips}", response_model=CountyDetail)
def get_county(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"""
        SELECT s.*, d.*
        FROM {catalog}.gold.gold_county_growth_score s
        JOIN {catalog}.gold.gold_county_details d ON s.fips = d.fips
        WHERE s.fips = '{fips}'
    """
    rows = execute_query(query)
    if not rows:
        raise HTTPException(status_code=404, detail=f"County {fips} not found")
    return rows[0]


@router.get("/trends/{fips}")
def get_trends(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    catalog = os.environ.get("CATALOG", "store_siting")
    query = f"""
        SELECT report_year, report_quarter,
               permits_per_1k_pop, net_migration_rate,
               vacancy_rate_yoy_change, employment_growth_rate
        FROM {catalog}.gold.gold_county_details
        WHERE fips = '{fips}'
        ORDER BY report_year, report_quarter
    """
    return execute_query(query)
