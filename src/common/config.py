"""Scoring weights and pipeline configuration."""

DEFAULT_WEIGHTS = {
    "building_permits": 0.25,
    "net_migration": 0.20,
    "vacancy_change": 0.15,
    "employment_growth": 0.15,
    "school_enrollment_growth": 0.10,
    "ssp_projected_growth": 0.15,
}

SCORE_TIERS = {
    "A": (80, 100),
    "B": (60, 80),
    "C": (40, 60),
    "D": (20, 40),
    "F": (0, 20),
}

import os
CATALOG = os.environ.get("CATALOG", "vdm_classic_rikfy0_catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

MIN_SOURCES_FOR_SCORE = 5
TOTAL_SOURCES = 10


def get_table_name(schema: str, table: str) -> str:
    """Return fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{table}"
