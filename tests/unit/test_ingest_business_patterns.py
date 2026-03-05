import os
from src.ingestion.business_patterns import parse_business_patterns_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2022)
    assert "census.gov" in url
    assert "22" in url


def test_parse_business_patterns_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "business_patterns_sample.csv"
    )
    rows = parse_business_patterns_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["naics_code"] == "44-45"
    assert row["establishments"] == 12500
    assert row["employees"] == 185000


def test_parse_business_patterns_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "business_patterns_sample.csv"
    )
    rows = parse_business_patterns_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
