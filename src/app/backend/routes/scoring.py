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
    valid_indicators = {
        "building_permits", "net_migration", "vacancy_change",
        "employment_growth", "school_enrollment_growth",
        "ssp_projected_growth", "qsr_density_inv",
    }
    for w in payload.weights:
        if w.indicator not in valid_indicators:
            continue
        execute_query(
            f"UPDATE {catalog}.gold.gold_scoring_config "
            f"SET weight = {float(w.weight)} WHERE indicator = '{w.indicator}'"
        )
    return {"status": "updated", "message": "Re-run gold scoring job to apply new weights."}
