"""Transform Bronze Overture Places to Silver — QSR-relevant POI restaurants.

Filters Overture Places to restaurant and fast-food categories,
extracts key fields from nested structs, and assigns approximate
county FIPS codes based on lat/lng state bounding boxes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, coalesce, lit, lower, udf,
)
from pyspark.sql.types import StringType


# Overture category values that map to QSR-relevant restaurants
QSR_CATEGORIES = {
    "restaurant",
    "fast_food",
    "fast_food_restaurant",
    "quick_service_restaurant",
    "pizza_restaurant",
    "burger_restaurant",
    "sandwich_shop",
    "coffee_shop",
    "ice_cream_shop",
    "bakery",
    "deli",
    "food_court",
    "drive_in_restaurant",
    "cafe",
}

# Approximate state bounding boxes for FIPS assignment:
# (min_lat, max_lat, min_lon, max_lon, state_fips)
# Provides a rough state-level FIPS prefix. Full county-level FIPS
# requires a spatial join with county polygons (future enrichment step).
_STATE_BBOXES = [
    (30.22, 35.01, -88.47, -84.89, "01"),  # AL
    (54.56, 71.39, -179.15, -129.98, "02"),  # AK
    (31.33, 37.00, -114.81, -109.04, "04"),  # AZ
    (33.00, 36.50, -94.62, -89.64, "05"),  # AR
    (32.53, 42.01, -124.41, -114.13, "06"),  # CA
    (36.99, 41.00, -109.06, -102.04, "08"),  # CO
    (40.95, 42.05, -73.73, -71.79, "09"),  # CT
    (38.45, 39.84, -75.79, -75.05, "10"),  # DE
    (38.79, 39.00, -77.12, -76.91, "11"),  # DC
    (24.40, 31.00, -87.63, -80.03, "12"),  # FL
    (30.36, 35.00, -85.61, -80.84, "13"),  # GA
    (18.91, 22.24, -160.25, -154.81, "15"),  # HI
    (41.99, 49.00, -117.24, -111.04, "16"),  # ID
    (36.97, 42.51, -91.51, -87.02, "17"),  # IL
    (37.77, 41.76, -88.10, -84.78, "18"),  # IN
    (40.38, 43.50, -96.64, -90.14, "19"),  # IA
    (36.99, 40.00, -102.05, -94.59, "20"),  # KS
    (36.50, 39.15, -89.57, -81.96, "21"),  # KY
    (28.93, 33.02, -94.04, -88.82, "22"),  # LA
    (43.06, 47.46, -71.08, -66.95, "23"),  # ME
    (37.91, 39.72, -79.49, -75.05, "24"),  # MD
    (41.24, 42.89, -73.51, -69.93, "25"),  # MA
    (41.70, 48.31, -90.42, -82.12, "26"),  # MI
    (43.50, 49.38, -97.24, -89.49, "27"),  # MN
    (30.17, 34.99, -91.66, -88.10, "28"),  # MS
    (35.99, 40.61, -95.77, -89.10, "29"),  # MO
    (44.36, 49.00, -116.05, -104.04, "30"),  # MT
    (39.99, 43.00, -104.05, -95.31, "31"),  # NE
    (35.00, 42.00, -120.01, -114.04, "32"),  # NV
    (42.70, 45.31, -72.56, -70.70, "33"),  # NH
    (38.93, 41.36, -75.56, -73.89, "34"),  # NJ
    (31.33, 37.00, -109.05, -103.00, "35"),  # NM
    (40.50, 45.02, -79.76, -71.86, "36"),  # NY
    (33.84, 36.59, -84.32, -75.46, "37"),  # NC
    (45.94, 49.00, -104.05, -96.55, "38"),  # ND
    (38.40, 41.98, -84.82, -80.52, "39"),  # OH
    (33.62, 37.00, -103.00, -94.43, "40"),  # OK
    (41.99, 46.29, -124.57, -116.46, "41"),  # OR
    (39.72, 42.27, -80.52, -74.69, "42"),  # PA
    (41.15, 42.02, -71.86, -71.12, "44"),  # RI
    (32.03, 35.22, -83.35, -78.54, "45"),  # SC
    (42.48, 45.94, -104.06, -96.44, "46"),  # SD
    (34.98, 36.68, -90.31, -81.65, "47"),  # TN
    (25.84, 36.50, -106.65, -93.51, "48"),  # TX
    (36.99, 42.00, -114.05, -109.04, "49"),  # UT
    (42.73, 45.02, -73.44, -71.47, "50"),  # VT
    (36.54, 39.47, -83.68, -75.24, "51"),  # VA
    (45.54, 49.00, -124.85, -116.92, "53"),  # WA
    (37.20, 40.64, -82.64, -77.72, "54"),  # WV
    (42.49, 47.08, -92.89, -86.25, "55"),  # WI
    (40.99, 45.01, -111.06, -104.05, "56"),  # WY
]


def _assign_state_fips_udf():
    """Return a UDF that assigns a state-level FIPS from lat/lng."""
    bboxes = _STATE_BBOXES

    def assign(lat, lng):
        if lat is None or lng is None:
            return None
        for min_lat, max_lat, min_lon, max_lon, state_fips in bboxes:
            if min_lat <= lat <= max_lat and min_lon <= lng <= max_lon:
                return state_fips + "000"  # state FIPS + placeholder county
        return None

    return udf(assign, StringType())


def transform_overture_poi(bronze_df: DataFrame) -> DataFrame:
    """Transform bronze.overture_places into silver.silver_poi_restaurants.

    Filters to QSR-relevant categories and extracts:
    - id, lat, lng, name, brand, category, fips (approximate state-level)

    Overture nested struct access:
    - names.primary -> place name
    - categories.primary -> category string
    - brand.names.primary -> brand name
    - bbox.xmin/ymin -> approximate lng/lat (centroid of bbox)
    """
    # Extract lat/lng from bbox centroid (more reliable than parsing WKB geometry)
    with_coords = bronze_df.withColumn(
        "lat", (col("bbox.ymin") + col("bbox.ymax")) / 2
    ).withColumn(
        "lng", (col("bbox.xmin") + col("bbox.xmax")) / 2
    )

    # Extract category from nested struct
    with_category = with_coords.withColumn(
        "category_value",
        coalesce(col("categories.primary"), lit("unknown"))
    )

    # Filter to QSR-relevant categories
    category_conditions = None
    for cat in QSR_CATEGORIES:
        condition = lower(col("category_value")).contains(cat)
        if category_conditions is None:
            category_conditions = condition
        else:
            category_conditions = category_conditions | condition

    filtered = with_category.filter(category_conditions)

    # Extract name and brand from nested structs
    with_fields = filtered.withColumn(
        "name", col("names.primary")
    ).withColumn(
        "brand",
        coalesce(col("brand.names.primary"), lit(None).cast("string"))
    ).withColumn(
        "category", col("category_value")
    )

    # Assign approximate FIPS (state-level + placeholder county code)
    assign_fips = _assign_state_fips_udf()
    with_fips = with_fields.withColumn("fips", assign_fips(col("lat"), col("lng")))

    # Select final columns
    result = with_fips.select(
        col("id"),
        col("lat"),
        col("lng"),
        col("name"),
        col("brand"),
        col("category"),
        col("fips"),
    )

    return result
