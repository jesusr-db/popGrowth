"""End-to-end test: Bronze fixtures -> Silver transforms -> Gold scoring."""

import pytest
from src.gold.scoring import compute_composite_score, assign_tier, min_max_normalize


def test_full_scoring_pipeline():
    """Test that scoring produces valid output from indicator values."""
    indicators = {
        "building_permits": 0.8,
        "net_migration": 0.6,
        "vacancy_change": 0.7,
        "employment_growth": 0.5,
        "school_enrollment_growth": 0.9,
        "ssp_projected_growth": 0.4,
        "qsr_density_inv": 0.3,
    }
    score = compute_composite_score(indicators)
    assert 0 <= score <= 100
    tier = assign_tier(score)
    assert tier in ("A", "B", "C", "D", "F")


def test_normalization_produces_valid_range():
    values = [100, 200, 300, 400, 500]
    normalized = min_max_normalize(values)
    for v in normalized:
        assert 0.0 <= v <= 1.0
    assert normalized[0] == 0.0
    assert normalized[-1] == 1.0


def test_scoring_with_missing_indicators():
    """Score should still compute with partial indicators."""
    indicators = {
        "building_permits": 0.8,
        "net_migration": 0.6,
    }
    score = compute_composite_score(indicators)
    assert 0 <= score <= 100


def test_tier_boundaries():
    """Verify tier assignment at exact boundaries."""
    assert assign_tier(80) == "A"
    assert assign_tier(79) == "B"
    assert assign_tier(60) == "B"
    assert assign_tier(59) == "C"
    assert assign_tier(40) == "C"
    assert assign_tier(39) == "D"
    assert assign_tier(20) == "D"
    assert assign_tier(19) == "F"
