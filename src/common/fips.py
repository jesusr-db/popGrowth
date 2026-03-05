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


STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}


def fips_to_state_abbr(fips: str) -> str:
    """Convert 5-digit county FIPS to 2-letter state abbreviation."""
    return STATE_FIPS_TO_ABBR.get(fips[:2], "??")
