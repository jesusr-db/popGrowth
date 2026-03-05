"""Gold layer scoring engine — weighted composite score per county."""

from src.common.config import DEFAULT_WEIGHTS, SCORE_TIERS


def min_max_normalize(values: list[float]) -> list[float]:
    """Min-max normalize a list of values to [0, 1]."""
    if len(values) <= 1:
        return [0.0] * len(values)
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.0] * len(values)
    return [(v - min_v) / (max_v - min_v) for v in values]


def compute_composite_score(
    indicators: dict[str, float],
    weights: dict[str, float] | None = None,
) -> float:
    """Compute weighted composite score from normalized indicator values."""
    w = weights or DEFAULT_WEIGHTS
    score = sum(indicators.get(k, 0.0) * w.get(k, 0.0) for k in w)
    return score * 100


def assign_tier(score: float) -> str:
    """Assign A/B/C/D/F tier based on score."""
    for tier, (low, high) in SCORE_TIERS.items():
        if low <= score <= high:
            return tier
    return "F"
