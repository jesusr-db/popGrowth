import os
from src.ingestion.school_enrollment import parse_school_enrollment_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2025)
    assert "nces.ed.gov" in url
    assert "2025" in url


def test_parse_school_enrollment_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "school_enrollment_sample.csv"
    )
    rows = parse_school_enrollment_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["total_enrollment"] == 345000
    assert row["district_name"] == "Miami-Dade County Public Schools"


def test_parse_school_enrollment_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "school_enrollment_sample.csv"
    )
    rows = parse_school_enrollment_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
