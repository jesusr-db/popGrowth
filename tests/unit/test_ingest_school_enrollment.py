from unittest.mock import patch, MagicMock

from src.ingestion.school_enrollment import _fetch_enrollment


SAMPLE_RESPONSE = [
    ["B14001_002E", "NAME", "state", "county"],
    ["345000", "Miami-Dade County, Florida", "12", "086"],
    ["120000", "Travis County, Texas", "48", "453"],
]


@patch("src.ingestion.school_enrollment.requests.get")
def test_fetch_enrollment_parses_rows(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_enrollment(2022)
    assert len(rows) == 2

    assert rows[0]["fips"] == "12086"
    assert rows[0]["total_enrollment"] == 345000
    assert rows[0]["data_source"] == "census_acs"
    assert rows[0]["report_year"] == 2022


@patch("src.ingestion.school_enrollment.requests.get")
def test_fetch_enrollment_url_construction(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [["B14001_002E", "NAME", "state", "county"]]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    _fetch_enrollment(2021)
    url = mock_get.call_args[0][0]
    assert "2021" in url
    assert "api.census.gov" in url
    assert "B14001_002E" in url


@patch("src.ingestion.school_enrollment.requests.get")
def test_fetch_enrollment_skips_missing_values(mock_get):
    data = [
        ["B14001_002E", "NAME", "state", "county"],
        ["-", "Empty County, State", "01", "001"],
        ["50000", "Real County, State", "01", "003"],
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = data
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_enrollment(2022)
    assert len(rows) == 1
    assert rows[0]["fips"] == "01003"
