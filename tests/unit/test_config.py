from src.common.config import DEFAULT_WEIGHTS, SCORE_TIERS, get_table_name


def test_weights_sum_to_one():
    assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9


def test_tiers_cover_full_range():
    all_values = set()
    for low, high in SCORE_TIERS.values():
        all_values.update(range(low, high + 1))
    assert all_values == set(range(0, 101))


def test_get_table_name():
    assert get_table_name("bronze", "permits") == "store_siting.bronze.permits"
