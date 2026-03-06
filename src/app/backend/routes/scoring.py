from fastapi import APIRouter
from backend.db import execute_query
from backend.models.county import ScoringWeight, ScoringWeightsUpdate

router = APIRouter(prefix="/api")


@router.get("/scores/weights", response_model=list[ScoringWeight])
def get_weights():
    return execute_query("SELECT * FROM gold.synced_gold_scoring_config")


@router.put("/scores/weights")
def update_weights(payload: ScoringWeightsUpdate):
    valid_indicators = {
        "building_permits", "net_migration", "vacancy_change",
        "employment_growth", "school_enrollment_growth",
        "ssp_projected_growth",
    }
    for w in payload.weights:
        if w.indicator not in valid_indicators:
            continue
        execute_query(
            "UPDATE gold.synced_gold_scoring_config SET weight = %s WHERE indicator = %s",
            (float(w.weight), w.indicator),
        )
    return {"status": "updated", "message": "Re-run gold scoring job to apply new weights."}
