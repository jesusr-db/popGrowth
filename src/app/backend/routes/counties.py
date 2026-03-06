import re
from fastapi import APIRouter, Query, HTTPException
from backend.db import execute_query
from backend.models.county import CountySummary

router = APIRouter(prefix="/api")

_FIPS_RE = re.compile(r"^\d{5}$")
_STATE_RE = re.compile(r"^[A-Za-z]{2}$")


@router.get("/counties", response_model=list[CountySummary])
def list_counties(state: str | None = Query(None)):
    query = "SELECT * FROM gold.synced_gold_county_growth_score"
    params = None
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += " WHERE state = %s"
        params = (state,)
    query += " ORDER BY composite_score DESC"
    return execute_query(query, params)


@router.get("/counties/top", response_model=list[CountySummary])
def top_counties(n: int = Query(25, ge=1, le=500), state: str | None = Query(None)):
    query = "SELECT * FROM gold.synced_gold_county_growth_score"
    params_list = []
    if state:
        if not _STATE_RE.match(state):
            raise HTTPException(400, "Invalid state code")
        query += " WHERE state = %s"
        params_list.append(state)
    query += " ORDER BY composite_score DESC LIMIT %s"
    params_list.append(n)
    return execute_query(query, tuple(params_list))


@router.get("/counties/{fips}")
def get_county(fips: str):
    if not _FIPS_RE.match(fips):
        raise HTTPException(400, "FIPS must be a 5-digit code")
    score_rows = execute_query(
        "SELECT * FROM gold.synced_gold_county_growth_score WHERE fips = %s", (fips,)
    )
    if not score_rows:
        raise HTTPException(status_code=404, detail=f"County {fips} not found")
    row = score_rows[0]

    detail_rows = execute_query(
        "SELECT * FROM gold.synced_gold_county_details WHERE fips = %s", (fips,)
    )
    if detail_rows:
        row.update({k: v for k, v in detail_rows[0].items() if k != "fips" and v is not None})

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
    return []
