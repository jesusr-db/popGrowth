import pytest
from pyspark.sql import SparkSession
from src.silver.building_permits import transform_building_permits


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_building_permits_aggregates_monthly_to_quarterly(spark):
    data = [
        ("12086", "Miami-Dade County", "01/2025", 450, 330, 780),
        ("12086", "Miami-Dade County", "02/2025", 400, 280, 680),
        ("12086", "Miami-Dade County", "03/2025", 500, 350, 850),
    ]
    columns = ["fips", "county_name", "survey_date", "single_family_units",
               "multi_family_units", "total_units"]
    bronze_df = spark.createDataFrame(data, columns)

    result = transform_building_permits(bronze_df)
    rows = result.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["fips"] == "12086"
    assert row["report_year"] == 2025
    assert row["report_quarter"] == 1
    assert row["total_units_permitted"] == 780 + 680 + 850


def test_transform_has_required_columns(spark):
    data = [("12086", "Miami-Dade County", "01/2025", 450, 330, 780)]
    columns = ["fips", "county_name", "survey_date", "single_family_units",
               "multi_family_units", "total_units"]
    bronze_df = spark.createDataFrame(data, columns)

    result = transform_building_permits(bronze_df)
    required = {"fips", "report_year", "report_quarter", "total_units_permitted",
                "single_family_units", "multi_family_units"}
    assert required.issubset(set(result.columns))
