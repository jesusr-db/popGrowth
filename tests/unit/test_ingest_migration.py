from unittest.mock import patch, MagicMock

from src.ingestion.migration import (
    _parse_migration_csv,
    _aggregate_flows,
    _build_migration_rows,
)


def test_parse_migration_csv():
    csv_text = "y1_statefips,y1_countyfips,y2_statefips,y2_countyfips,n1,n2\n12,086,48,453,100,250\n48,453,12,086,80,200\n"
    rows = _parse_migration_csv(csv_text)
    assert len(rows) == 2
    assert rows[0]["y1_statefips"] == "12"
    assert rows[0]["n2"] == "250"


def test_aggregate_flows_skips_summary_rows():
    rows = [
        {"y2_statefips": "12", "y2_countyfips": "086", "n2": "100"},
        {"y2_statefips": "96", "y2_countyfips": "000", "n2": "999"},  # summary row
        {"y2_statefips": "12", "y2_countyfips": "086", "n2": "50"},
        {"y2_statefips": "12", "y2_countyfips": "000", "n2": "300"},  # county 000 = state total
    ]
    totals = _aggregate_flows(rows, "y2_statefips", "y2_countyfips")
    assert totals["12086"] == 150
    assert "96000" not in totals
    assert "12000" not in totals


def test_build_migration_rows():
    inflow = {"12086": 500, "48453": 300}
    outflow = {"12086": 200, "48453": 400}
    rows = _build_migration_rows(inflow, outflow, 2023, 1)
    assert len(rows) == 2

    by_fips = {r["fips"]: r for r in rows}
    assert by_fips["12086"]["net_migration"] == 300
    assert by_fips["48453"]["net_migration"] == -100
    assert by_fips["12086"]["data_source"] == "irs_soi"
    assert by_fips["12086"]["report_year"] == 2023
