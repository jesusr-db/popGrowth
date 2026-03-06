import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)
from src.silver.overture_poi import transform_overture_poi, QSR_CATEGORIES


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def _make_bronze_places_df(spark, rows):
    """Create a DataFrame mimicking Overture Places bronze schema.

    Each row is a tuple of:
    (id, names_primary, categories_primary, brand_names_primary,
     bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax)
    """
    schema = StructType([
        StructField("id", StringType()),
        StructField("names", StructType([
            StructField("primary", StringType()),
        ])),
        StructField("categories", StructType([
            StructField("primary", StringType()),
        ])),
        StructField("brand", StructType([
            StructField("names", StructType([
                StructField("primary", StringType()),
            ])),
        ])),
        StructField("bbox", StructType([
            StructField("xmin", DoubleType()),
            StructField("xmax", DoubleType()),
            StructField("ymin", DoubleType()),
            StructField("ymax", DoubleType()),
        ])),
    ])
    data = []
    for r in rows:
        data.append((
            r[0],
            {"primary": r[1]},
            {"primary": r[2]},
            {"names": {"primary": r[3]}},
            {"xmin": r[4], "xmax": r[5], "ymin": r[6], "ymax": r[7]},
        ))
    return spark.createDataFrame(data, schema)


def test_filters_to_restaurant_categories(spark):
    rows = [
        ("p1", "McDonald's", "fast_food", "McDonald's",
         -80.20, -80.18, 25.78, 25.80),
        ("p2", "Gas Station", "gas_station", None,
         -80.30, -80.28, 25.78, 25.80),
        ("p3", "Subway", "sandwich_shop", "Subway",
         -80.25, -80.23, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)
    result_rows = result.collect()

    # gas_station should be filtered out
    ids = {r["id"] for r in result_rows}
    assert "p1" in ids
    assert "p3" in ids
    assert "p2" not in ids


def test_extracts_lat_lng_from_bbox(spark):
    rows = [
        ("p1", "Wendy's", "restaurant", "Wendy's",
         -84.40, -84.38, 33.76, 33.78),
    ]
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)
    row = result.collect()[0]

    assert abs(row["lat"] - 33.77) < 0.01
    assert abs(row["lng"] - (-84.39)) < 0.01


def test_assigns_state_fips(spark):
    # Coordinates in Miami-Dade County, FL (state FIPS 12)
    rows = [
        ("p1", "Burger King", "fast_food", "Burger King",
         -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)
    row = result.collect()[0]

    assert row["fips"] is not None
    assert row["fips"].startswith("12")  # Florida state FIPS


def test_has_required_columns(spark):
    rows = [
        ("p1", "Taco Bell", "fast_food", "Taco Bell",
         -87.65, -87.63, 41.88, 41.90),
    ]
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)

    required = {"id", "lat", "lng", "name", "brand", "category", "fips"}
    assert required.issubset(set(result.columns))


def test_handles_null_brand(spark):
    rows = [
        ("p1", "Local Restaurant", "restaurant", None,
         -73.99, -73.97, 40.74, 40.76),
    ]
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)
    row = result.collect()[0]

    assert row["name"] == "Local Restaurant"
    assert row["brand"] is None


def test_empty_input_returns_empty(spark):
    rows = []
    bronze_df = _make_bronze_places_df(spark, rows)
    result = transform_overture_poi(bronze_df)
    assert result.count() == 0
