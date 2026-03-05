import pytest
from src.common.fips import normalize_fips, validate_fips


def test_normalize_fips_pads_short_codes():
    assert normalize_fips("1001") == "01001"
    assert normalize_fips("6037") == "06037"


def test_normalize_fips_preserves_five_digit():
    assert normalize_fips("36061") == "36061"


def test_normalize_fips_rejects_invalid():
    with pytest.raises(ValueError):
        normalize_fips("abc")
    with pytest.raises(ValueError):
        normalize_fips("")
    with pytest.raises(ValueError):
        normalize_fips("123456")


def test_validate_fips_valid():
    assert validate_fips("01001") is True
    assert validate_fips("36061") is True


def test_validate_fips_invalid():
    assert validate_fips("00000") is False
    assert validate_fips("abc") is False
    assert validate_fips("") is False
