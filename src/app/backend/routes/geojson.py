import json
import os
from fastapi import APIRouter
from backend.db import execute_query

router = APIRouter(prefix="/api")

_geojson_cache: dict | None = None


@router.get("/geojson")
def get_geojson():
    global _geojson_cache

    scores = execute_query(
        """SELECT s.fips, s.composite_score, s.score_tier, s.rank_national,
                  s.population, s.median_income, s.ssp_growth_rate,
                  d.permits_per_1k_pop, d.net_migration_rate,
                  d.occupancy_rate, d.employment_per_capita
           FROM gold.synced_gold_county_growth_score s
           LEFT JOIN gold.synced_gold_county_details d ON s.fips = d.fips"""
    )
    score_lookup = {r["fips"]: r for r in scores}

    if _geojson_cache is None:
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
