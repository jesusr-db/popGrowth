import pytest

from src.ingestion.ssp_projections import ingest


def test_ingest_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="SSP projection data requires manual download"):
        ingest(spark=None, scenario="SSP2")


def test_ingest_includes_scenario_in_message():
    with pytest.raises(NotImplementedError, match="SSP3"):
        ingest(spark=None, scenario="SSP3")


def test_ingest_default_scenario():
    with pytest.raises(NotImplementedError, match="SSP2"):
        ingest(spark=None)
