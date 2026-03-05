"""Scoring weights and pipeline configuration."""

DEFAULT_WEIGHTS = {
    "building_permits": 0.25,
    "net_migration": 0.20,
    "vacancy_change": 0.15,
    "employment_growth": 0.15,
    "school_enrollment_growth": 0.10,
    "ssp_projected_growth": 0.10,
    "qsr_density_inv": 0.05,
}

SCORE_TIERS = {
    "A": (80, 100),
    "B": (60, 79),
    "C": (40, 59),
    "D": (20, 39),
    "F": (0, 19),
}

CATALOG = "store_siting"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

MIN_SOURCES_FOR_SCORE = 5
TOTAL_SOURCES = 10


def get_table_name(schema: str, table: str) -> str:
    """Return fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{table}"
