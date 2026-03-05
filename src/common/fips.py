"""FIPS county code normalization and validation utilities."""

# Valid state FIPS codes (01-56, excluding gaps)
VALID_STATE_FIPS = {
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56",
}


def normalize_fips(raw: str) -> str:
    """Normalize a FIPS code to 5-digit zero-padded string."""
    cleaned = str(raw).strip()
    if not cleaned.isdigit():
        raise ValueError(f"FIPS code must be numeric, got: {raw!r}")
    if len(cleaned) > 5 or len(cleaned) < 1:
        raise ValueError(f"FIPS code must be 1-5 digits, got: {raw!r}")
    return cleaned.zfill(5)


def validate_fips(fips: str) -> bool:
    """Check if a 5-digit FIPS code has a valid state prefix."""
    if not fips or not fips.isdigit() or len(fips) != 5:
        return False
    state = fips[:2]
    return state in VALID_STATE_FIPS


def state_fips_from_county(county_fips: str) -> str:
    """Extract 2-digit state FIPS from 5-digit county FIPS."""
    normalized = normalize_fips(county_fips)
    return normalized[:2]
