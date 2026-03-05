"""Transform Bronze SSP projections to Silver — compute projected growth rate."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first
from pyspark.sql.window import Window


def transform_ssp_projections(bronze_df: DataFrame) -> DataFrame:
    """Passthrough with scenario column. Compute projected_growth_rate from base year."""
    window = Window.partitionBy("fips", "scenario").orderBy("projection_year")
    df = bronze_df.withColumn(
        "base_pop", first("projected_population").over(window)
    ).withColumn(
        "projected_growth_rate",
        (col("projected_population") - col("base_pop")) / col("base_pop")
    )
    return df.select(
        "fips", "projection_year", "scenario",
        "projected_population", "projected_growth_rate"
    )
