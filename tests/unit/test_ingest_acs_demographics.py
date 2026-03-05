import os
from src.ingestion.acs_demographics import parse_acs_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2023)
    assert "census.gov" in url
    assert "2023" in url


def test_parse_acs_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "acs_demographics_sample.csv"
    )
    rows = parse_acs_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["population"] == 2750000
    assert row["median_income"] == 52000
    assert row["median_age"] == 39.5
    assert row["households"] == 950000


def test_parse_acs_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "acs_demographics_sample.csv"
    )
    rows = parse_acs_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
