from unittest.mock import patch
from fastapi.testclient import TestClient
from src.app.backend.main import app

client = TestClient(app)


@patch("src.app.backend.routes.counties.execute_query")
def test_list_counties(mock_query):
    mock_query.return_value = [
        {"fips": "12086", "county_name": "Miami-Dade", "state": "FL",
         "composite_score": 85.5, "score_tier": "A", "rank_national": 12,
         "population": 2800000, "median_income": 55000.0}
    ]
    response = client.get("/api/counties")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["fips"] == "12086"


@patch("src.app.backend.routes.counties.execute_query")
def test_top_counties_default_25(mock_query):
    mock_query.return_value = []
    response = client.get("/api/counties/top")
    assert response.status_code == 200
    call_args = mock_query.call_args[0][0]
    assert "LIMIT 25" in call_args


@patch("src.app.backend.routes.counties.execute_query")
def test_get_county_not_found(mock_query):
    mock_query.return_value = []
    response = client.get("/api/counties/99999")
    assert response.status_code == 404
