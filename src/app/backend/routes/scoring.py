import os
from fastapi import APIRouter
from src.app.backend.db import execute_query
from src.app.backend.models.county import ScoringWeight, ScoringWeightsUpdate

router = APIRouter(prefix="/api")


@router.get("/scores/weights", response_model=list[ScoringWeight])
def get_weights():
    catalog = os.environ.get("CATALOG", "store_siting")
    return execute_query(f"SELECT * FROM {catalog}.gold.gold_scoring_config")


@router.put("/scores/weights")
def update_weights(payload: ScoringWeightsUpdate):
    catalog = os.environ.get("CATALOG", "store_siting")
    for w in payload.weights:
        execute_query(
            f"UPDATE {catalog}.gold.gold_scoring_config "
            f"SET weight = {w.weight} WHERE indicator = '{w.indicator}'"
        )
    return {"status": "updated", "message": "Re-run gold scoring job to apply new weights."}
