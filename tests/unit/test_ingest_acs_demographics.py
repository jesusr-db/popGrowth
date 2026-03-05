from unittest.mock import patch, MagicMock

from src.ingestion.acs_demographics import _fetch_demographics


SAMPLE_RESPONSE = [
    ["B01003_001E", "B19013_001E", "B01002_001E", "B11001_001E", "NAME", "state", "county"],
    ["2750000", "52000", "39.5", "950000", "Miami-Dade County, Florida", "12", "086"],
    ["1400000", "61000", "36.2", "520000", "Travis County, Texas", "48", "453"],
]


@patch("src.ingestion.acs_demographics.requests.get")
def test_fetch_demographics_parses_rows(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_demographics(2022)
    assert len(rows) == 2

    assert rows[0]["fips"] == "12086"
    assert rows[0]["population"] == 2750000
    assert rows[0]["median_income"] == 52000
    assert rows[0]["median_age"] == 39.5
    assert rows[0]["households"] == 950000
    assert rows[0]["data_source"] == "census_acs"
    assert rows[0]["report_year"] == 2022
    assert rows[0]["county_name"] == "Miami-Dade County, Florida"


@patch("src.ingestion.acs_demographics.requests.get")
def test_fetch_demographics_url_construction(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [["B01003_001E", "B19013_001E", "B01002_001E", "B11001_001E", "NAME", "state", "county"]]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    _fetch_demographics(2021)
    url = mock_get.call_args[0][0]
    assert "2021" in url
    assert "api.census.gov" in url
    assert "acs5" in url


@patch("src.ingestion.acs_demographics.requests.get")
def test_fetch_demographics_skips_missing_data(mock_get):
    data = [
        ["B01003_001E", "B19013_001E", "B01002_001E", "B11001_001E", "NAME", "state", "county"],
        ["100", "-", "30.0", "50", "Test County, State", "01", "001"],
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = data
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_demographics(2022)
    assert len(rows) == 1
    assert rows[0]["median_income"] is None
