"""Transform Bronze migration data to Silver with net migration rates."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_migration(bronze_df: DataFrame, population_df: DataFrame) -> DataFrame:
    """Join migration with population to compute per-capita migration rate."""
    joined = bronze_df.join(population_df, on="fips", how="left")
    result = joined.withColumn(
        "net_migration_rate",
        (col("net_migration") / col("population")) * 1000
    )
    return result.select(
        "fips", "report_year", "report_quarter",
        "inflow", "outflow", "net_migration", "net_migration_rate"
    )
