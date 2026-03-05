import pytest
from pyspark.sql import SparkSession
from src.silver.migration import transform_migration


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_migration_computes_rate(spark):
    data = [
        ("12086", "Miami-Dade", "FL", 2025, 1, 8500, 6200, 2300),
        ("36061", "New York", "NY", 2025, 1, 4200, 7800, -3600),
    ]
    columns = ["fips", "county_name", "state", "report_year", "report_quarter",
               "inflow", "outflow", "net_migration"]
    bronze_df = spark.createDataFrame(data, columns)

    pop_data = [("12086", 2800000), ("36061", 1630000)]
    pop_df = spark.createDataFrame(pop_data, ["fips", "population"])

    result = transform_migration(bronze_df, pop_df)
    rows = {r["fips"]: r for r in result.collect()}

    assert rows["12086"]["net_migration_rate"] == pytest.approx(2300 / 2800000 * 1000, rel=0.01)
    assert rows["36061"]["net_migration_rate"] < 0
