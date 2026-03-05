import os
from src.ingestion.migration import parse_migration_csv


def test_parse_migration_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "migration_sample.csv"
    )
    rows = parse_migration_csv(fixture_path)
    assert len(rows) == 3

    miami = rows[0]
    assert miami["fips"] == "12086"
    assert miami["inflow"] == 8500
    assert miami["outflow"] == 6200
    assert miami["net_migration"] == 2300

    ny = rows[1]
    assert ny["net_migration"] == -3600


def test_parse_migration_csv_all_have_fips():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "migration_sample.csv"
    )
    rows = parse_migration_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
