import os
from src.ingestion.vacancy import parse_vacancy_csv, build_download_url


def test_build_download_url():
    url = build_download_url(2025, 1)
    assert "huduser.gov" in url
    assert "2025" in url


def test_parse_vacancy_csv():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "vacancy_sample.csv"
    )
    rows = parse_vacancy_csv(fixture_path)
    assert len(rows) == 3

    row = rows[0]
    assert row["fips"] == "12086"
    assert row["total_addresses"] == 1050000
    assert row["vacant_addresses"] == 84000
    assert row["vacancy_rate"] == round(84000 / 1050000, 6)


def test_parse_vacancy_csv_rate_computed():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "vacancy_sample.csv"
    )
    rows = parse_vacancy_csv(fixture_path)
    for row in rows:
        expected_rate = round(row["vacant_addresses"] / row["total_addresses"], 6)
        assert row["vacancy_rate"] == expected_rate


def test_parse_vacancy_csv_fips_format():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", "vacancy_sample.csv"
    )
    rows = parse_vacancy_csv(fixture_path)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
