"""Transform Bronze ACS demographics to Silver — passthrough with type enforcement."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_acs_demographics(bronze_df: DataFrame) -> DataFrame:
    """Passthrough — already at county level. Ensure proper types."""
    return bronze_df.select(
        col("fips"),
        col("report_year"),
        col("population").cast("int"),
        col("median_income").cast("double"),
        col("median_age").cast("double"),
        col("households").cast("int"),
    )
