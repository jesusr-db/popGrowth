"""Transform Bronze BLS employment data to Silver with growth rate."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, lit
from pyspark.sql.window import Window


def transform_employment(bronze_df: DataFrame) -> DataFrame:
    """Compute employment growth rate and average weekly wage."""
    window = Window.partitionBy("fips").orderBy("report_year", "report_quarter")
    df = bronze_df.withColumn(
        "prev_employment", lag("total_employment").over(window)
    ).withColumn(
        "employment_growth_rate",
        (col("total_employment") - col("prev_employment")) / col("prev_employment")
    ).withColumn(
        "avg_weekly_wage",
        col("total_wages") / (col("total_employment") * lit(13))
    )
    return df.select(
        "fips", "report_year", "report_quarter",
        "total_employment", "employment_growth_rate", "avg_weekly_wage"
    )
