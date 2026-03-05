import os
from src.ingestion.national_projections import parse_national_projections_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2023)
    assert "census.gov" in url
    assert "2023" in url


def test_parse_national_projections_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "national_projections_sample.csv"
    )
    rows = parse_national_projections_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["state_fips"] == "12"
    assert row["state_name"] == "Florida"
    assert row["projection_year"] == 2030
    assert row["projected_population"] == 24500000


def test_parse_national_projections_csv_state_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "national_projections_sample.csv"
    )
    rows = parse_national_projections_csv(fixture_path)
    for row in rows:
        assert len(row["state_fips"]) == 2
        assert row["state_fips"].isdigit()
