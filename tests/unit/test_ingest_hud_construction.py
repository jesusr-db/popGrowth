import os
from src.ingestion.hud_construction import parse_hud_construction_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2025, 1)
    assert "huduser.gov" in url
    assert "2025" in url


def test_parse_hud_construction_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "hud_construction_sample.csv"
    )
    rows = parse_hud_construction_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["county_name"] == "Miami-Dade"
    assert row["permitted_units"] == 580
    assert row["structure_type"] == "single-family"


def test_parse_hud_construction_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "hud_construction_sample.csv"
    )
    rows = parse_hud_construction_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
