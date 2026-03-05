from unittest.mock import patch, MagicMock
import tempfile
import os

from src.ingestion.hud_construction import _build_download_url, _parse_bps_csv


def test_build_download_url():
    url = _build_download_url(2023)
    assert url == "https://www2.census.gov/econ/bps/County/co2312c.txt"

    url2 = _build_download_url(2005)
    assert url2 == "https://www2.census.gov/econ/bps/County/co0512c.txt"


def test_parse_bps_csv():
    # Two header rows, then data rows matching the expected CSV format
    # Fields: [0]=survey_date, [1]=state_fips, [2]=county_fips, [3]=region, [4]=division,
    #          [5]=county_name, [6]=?, [7]=single_units, ...,[10]=two_units,...[13]=3-4units,...[16]=5+units
    # Fields indexed by _parse_bps_csv:
    # [1]=state, [2]=county, [5]=name, [7]=single, [10]=two, [13]=3-4, [16]=5+
    csv_content = (
        "Category Header Row\n"
        "Sub Header Row\n"
        "202312,12,086,3,6,Miami-Dade County,x,500,x,x,30,x,x,20,x,x,50\n"
        "202312,48,453,3,7,Travis County,x,300,x,x,10,x,x,5,x,x,15\n"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        rows = _parse_bps_csv(tmp_path, 2023, 4)
        assert len(rows) == 2

        assert rows[0]["fips"] == "12086"
        assert rows[0]["county_name"] == "Miami-Dade County"
        assert rows[0]["report_year"] == 2023
        assert rows[0]["report_quarter"] == 4
        # total = single(500) + two(30) + three_four(20) + five_plus(50) = 600
        assert rows[0]["permitted_units"] == 600
        assert rows[0]["data_source"] == "census_bps"
    finally:
        os.unlink(tmp_path)


@patch("src.ingestion.hud_construction.requests.get")
def test_download_and_parse_mocks_http(mock_get):
    from src.ingestion.hud_construction import _download_and_parse

    csv_text = (
        "Category Header Row\n"
        "Sub Header Row\n"
        "202312,12,086,3,6,Miami-Dade County,x,400,x,x,20,x,x,10,x,x,30\n"
    )
    mock_resp = MagicMock()
    mock_resp.text = csv_text
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    rows = _download_and_parse(2023, 4)
    assert len(rows) == 1
    assert rows[0]["fips"] == "12086"
    url = mock_get.call_args[0][0]
    assert "co2312c.txt" in url
