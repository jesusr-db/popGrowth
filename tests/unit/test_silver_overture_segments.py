import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BinaryType,
)
from src.silver.overture_segments import (
    transform_overture_segments, TRAFFIC_CLASS_MAP,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def _make_bronze_segments_df(spark, rows):
    """Create a DataFrame mimicking Overture Segments bronze schema.

    Each row is a tuple of:
    (id, road_class, bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax)
    """
    schema = StructType([
        StructField("id", StringType()),
        StructField("class", StringType()),
        StructField("geometry", BinaryType()),
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
            r[1],
            b"\x00",  # placeholder WKB geometry
            {"xmin": r[2], "xmax": r[3], "ymin": r[4], "ymax": r[5]},
        ))
    return spark.createDataFrame(data, schema)


def test_motorway_gets_traffic_class_5(spark):
    rows = [
        ("s1", "motorway", -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 5
    assert row["functional_class"] == "motorway"


def test_residential_gets_traffic_class_1(spark):
    rows = [
        ("s1", "residential", -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 1


def test_trunk_gets_traffic_class_4(spark):
    rows = [
        ("s1", "trunk", -87.65, -87.63, 41.88, 41.90),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 4


def test_primary_gets_traffic_class_3(spark):
    rows = [
        ("s1", "primary", -73.99, -73.97, 40.74, 40.76),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 3


def test_secondary_gets_traffic_class_2(spark):
    rows = [
        ("s1", "secondary", -122.42, -122.40, 37.77, 37.79),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 2


def test_null_class_defaults_to_1(spark):
    rows = [
        ("s1", None, -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["traffic_class"] == 1
    assert row["functional_class"] == "unclassified"


def test_assigns_state_fips(spark):
    # Coordinates in Florida
    rows = [
        ("s1", "primary", -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    row = result.collect()[0]

    assert row["fips"] is not None
    assert row["fips"].startswith("12")  # Florida


def test_has_required_columns(spark):
    rows = [
        ("s1", "motorway", -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)

    required = {"id", "geometry", "functional_class", "traffic_class", "fips"}
    assert required.issubset(set(result.columns))


def test_empty_input_returns_empty(spark):
    rows = []
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    assert result.count() == 0


def test_multiple_road_classes(spark):
    rows = [
        ("s1", "motorway", -80.20, -80.18, 25.78, 25.80),
        ("s2", "trunk", -80.20, -80.18, 25.78, 25.80),
        ("s3", "primary", -80.20, -80.18, 25.78, 25.80),
        ("s4", "secondary", -80.20, -80.18, 25.78, 25.80),
        ("s5", "residential", -80.20, -80.18, 25.78, 25.80),
    ]
    bronze_df = _make_bronze_segments_df(spark, rows)
    result = transform_overture_segments(bronze_df)
    result_rows = {r["id"]: r["traffic_class"] for r in result.collect()}

    assert result_rows["s1"] == 5
    assert result_rows["s2"] == 4
    assert result_rows["s3"] == 3
    assert result_rows["s4"] == 2
    assert result_rows["s5"] == 1
