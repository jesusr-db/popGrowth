import os
from src.ingestion.employment import parse_employment_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2024)
    assert "bls.gov" in url
    assert "2024" in url


def test_parse_employment_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "employment_sample.csv"
    )
    rows = parse_employment_csv(fixture_path, quarter=1)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["total_employment"] > 0
    assert row["total_wages"] == 18500000000
    assert row["establishments"] == 95000


def test_parse_employment_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "employment_sample.csv"
    )
    rows = parse_employment_csv(fixture_path, quarter=1)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
