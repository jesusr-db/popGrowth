"""Transform Bronze national projections to Silver — state-level passthrough."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_national_projections(bronze_df: DataFrame) -> DataFrame:
    """State-level context table passthrough."""
    return bronze_df.select(
        col("state_fips"),
        col("projection_year"),
        col("projected_population").cast("long"),
    )
