import pytest
from src.gold.scoring import min_max_normalize, compute_composite_score, assign_tier


def test_min_max_normalize():
    values = [10, 20, 30, 40, 50]
    result = min_max_normalize(values)
    assert result == [0.0, 0.25, 0.5, 0.75, 1.0]


def test_min_max_normalize_single_value():
    result = min_max_normalize([42])
    assert result == [0.0]


def test_min_max_normalize_identical_values():
    result = min_max_normalize([5, 5, 5])
    assert result == [0.0, 0.0, 0.0]


def test_compute_composite_score():
    indicators = {
        "building_permits": 0.8,
        "net_migration": 0.6,
        "vacancy_change": 0.7,
        "employment_growth": 0.5,
        "school_enrollment_growth": 0.9,
        "ssp_projected_growth": 0.4,
        "qsr_density_inv": 0.3,
    }
    weights = {
        "building_permits": 0.25,
        "net_migration": 0.20,
        "vacancy_change": 0.15,
        "employment_growth": 0.15,
        "school_enrollment_growth": 0.10,
        "ssp_projected_growth": 0.10,
        "qsr_density_inv": 0.05,
    }
    score = compute_composite_score(indicators, weights)
    expected = (0.8*0.25 + 0.6*0.20 + 0.7*0.15 + 0.5*0.15 + 0.9*0.10 + 0.4*0.10 + 0.3*0.05) * 100
    assert score == pytest.approx(expected, rel=0.01)


def test_assign_tier():
    assert assign_tier(95) == "A"
    assert assign_tier(80) == "A"
    assert assign_tier(79) == "B"
    assert assign_tier(60) == "B"
    assert assign_tier(59) == "C"
    assert assign_tier(40) == "C"
    assert assign_tier(39) == "D"
    assert assign_tier(20) == "D"
    assert assign_tier(19) == "F"
    assert assign_tier(0) == "F"
