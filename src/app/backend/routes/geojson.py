import json
import os
from fastapi import APIRouter
from src.app.backend.db import execute_query

router = APIRouter(prefix="/api")

_geojson_cache: dict | None = None


@router.get("/geojson")
def get_geojson():
    global _geojson_cache

    catalog = os.environ.get("CATALOG", "store_siting")
    scores = execute_query(
        f"SELECT fips, composite_score, score_tier, rank_national, population "
        f"FROM {catalog}.gold.gold_county_growth_score"
    )
    score_lookup = {r["fips"]: r for r in scores}

    if _geojson_cache is None:
        # Look in app/data first (DABs bundled), fall back to repo root
        geojson_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "us-counties.json"
        )
        if not os.path.exists(geojson_path):
            geojson_path = os.path.join(
                os.path.dirname(__file__), "..", "..", "..", "..", "data", "county_geojson", "us-counties.json"
            )
        with open(geojson_path) as f:
            _geojson_cache = json.load(f)

    for feature in _geojson_cache.get("features", []):
        fips = feature.get("properties", {}).get("GEOID", "")
        if fips in score_lookup:
            feature["properties"].update(score_lookup[fips])

    return _geojson_cache
