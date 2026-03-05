import os
from src.ingestion.ssp_projections import parse_ssp_csv, build_download_url


def test_build_download_url():
    url = build_download_url("SSP2")
    assert "SSP2" in url


def test_parse_ssp_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "ssp_projections_sample.csv"
    )
    rows = parse_ssp_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["projection_year"] == 2030
    assert row["scenario"] == "SSP2"
    assert row["projected_population"] == 2950000


def test_parse_ssp_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "ssp_projections_sample.csv"
    )
    rows = parse_ssp_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
