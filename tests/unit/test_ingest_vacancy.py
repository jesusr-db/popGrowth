from unittest.mock import patch, MagicMock

from src.ingestion.vacancy import download_and_parse, CENSUS_ACS_URL


SAMPLE_RESPONSE = [
    ["B25002_001E", "B25002_003E", "NAME", "state", "county"],
    ["1050000", "84000", "Miami-Dade County, Florida", "12", "086"],
    ["500000", "35000", "Travis County, Texas", "48", "453"],
]


@patch("src.ingestion.vacancy.requests.get")
def test_download_and_parse_parses_rows(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = download_and_parse(2023, 1)
    assert len(rows) == 2

    assert rows[0]["fips"] == "12086"
    assert rows[0]["total_addresses"] == 1050000
    assert rows[0]["vacant_addresses"] == 84000
    assert rows[0]["data_source"] == "census_acs"
    assert rows[0]["report_year"] == 2023
    assert rows[0]["report_quarter"] == 1


@patch("src.ingestion.vacancy.requests.get")
def test_download_and_parse_uses_previous_year(mock_get):
    """ACS data lags by one year, so year=2023 should query for 2022."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = [["B25002_001E", "B25002_003E", "NAME", "state", "county"]]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    download_and_parse(2023, 2)
    url = mock_get.call_args[0][0]
    assert "2022" in url
    assert "api.census.gov" in url


@patch("src.ingestion.vacancy.requests.get")
def test_download_and_parse_fips_format(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = download_and_parse(2023, 1)
    for row in rows:
        assert len(row["fips"]) == 5
        assert row["fips"].isdigit()
