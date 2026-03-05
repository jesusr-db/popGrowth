import pytest
import os
from unittest.mock import patch, MagicMock
from src.ingestion.building_permits import parse_building_permits_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2025, 1)
    assert "census.gov" in url
    assert "2025" in url


def test_parse_building_permits_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "building_permits_sample.csv"
    )
    rows = parse_building_permits_csv(fixture_path)
    assert len(rows) == 3
    row = rows[0]
    assert row["fips"] == "12086"
    assert row["county_name"] == "Miami-Dade County"
    assert row["single_family_units"] == 450
    assert row["multi_family_units"] == 330
    assert row["total_units"] == 780


def test_parse_building_permits_csv_computes_total():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "building_permits_sample.csv"
    )
    rows = parse_building_permits_csv(fixture_path)
    for row in rows:
        assert row["total_units"] == row["single_family_units"] + row["multi_family_units"]
