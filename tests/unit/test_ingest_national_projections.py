import pytest
from unittest.mock import patch, MagicMock

import requests

from src.ingestion.national_projections import _fetch_projections


SAMPLE_RESPONSE = [
    ["POP", "YEAR", "SEX", "state"],
    ["22000000", "2030", "0", "12"],
    ["11000000", "2030", "1", "12"],  # male only — should be filtered
    ["30000000", "2040", "0", "48"],
]


@patch("src.ingestion.national_projections.requests.get")
def test_fetch_projections_parses_rows(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _fetch_projections(2017)
    # Only SEX=0 rows kept, so 2 out of 3
    assert len(rows) == 2
    assert rows[0]["state_fips"] == "12"
    assert rows[0]["projection_year"] == 2030
    assert rows[0]["projected_population"] == 22000000
    assert rows[0]["data_source"] == "census_popproj"


@patch("src.ingestion.national_projections.requests.get")
def test_fetch_projections_url_construction(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [["POP", "YEAR", "SEX", "state"]]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    _fetch_projections(2017)
    url = mock_get.call_args[0][0]
    assert "2017" in url
    assert "popproj" in url
    assert "api.census.gov" in url


@patch("src.ingestion.national_projections.requests.get")
def test_fetch_projections_raises_not_implemented_on_http_error(mock_get):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.RequestException("404")
    mock_get.return_value = mock_resp

    with pytest.raises(NotImplementedError, match="not available for vintage"):
        _fetch_projections(2099)
