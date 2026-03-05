from unittest.mock import patch, MagicMock

from src.ingestion.business_patterns import _fetch_business_patterns


SAMPLE_RESPONSE = [
    ["ESTAB", "EMP", "NAICS2017", "state", "county"],
    ["12500", "185000", "44-45", "12", "086"],
    ["8200", "95000", "72", "48", "453"],
]


@patch("src.ingestion.business_patterns.requests.get")
def test_fetch_business_patterns_parses_rows(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_business_patterns(2021)
    assert len(rows) == 2

    assert rows[0]["fips"] == "12086"
    assert rows[0]["naics_code"] == "44-45"
    assert rows[0]["establishments"] == 12500
    assert rows[0]["employees"] == 185000
    assert rows[0]["data_source"] == "census_cbp"
    assert rows[0]["report_year"] == 2021


@patch("src.ingestion.business_patterns.requests.get")
def test_fetch_business_patterns_url_construction(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [["ESTAB", "EMP", "NAICS2017", "state", "county"]]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    _fetch_business_patterns(2020)
    url = mock_get.call_args[0][0]
    assert "2020" in url
    assert "api.census.gov" in url
    assert "cbp" in url


@patch("src.ingestion.business_patterns.requests.get")
def test_fetch_business_patterns_handles_missing_emp(mock_get):
    data = [
        ["ESTAB", "EMP", "NAICS2017", "state", "county"],
        ["100", "-", "11", "01", "001"],
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = data
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_business_patterns(2021)
    assert len(rows) == 1
    assert rows[0]["employees"] == 0
