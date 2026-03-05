"""Transform Bronze school enrollment to Silver — aggregate to county, compute growth."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lag
from pyspark.sql.window import Window


def transform_school_enrollment(bronze_df: DataFrame) -> DataFrame:
    """Aggregate district-level enrollment to county and compute YoY growth."""
    county_agg = (
        bronze_df
        .groupBy("fips", "report_year")
        .agg(spark_sum("total_enrollment").alias("total_enrollment"))
    )
    window = Window.partitionBy("fips").orderBy("report_year")
    df = county_agg.withColumn(
        "prev_enrollment", lag("total_enrollment").over(window)
    ).withColumn(
        "enrollment_growth_rate",
        (col("total_enrollment") - col("prev_enrollment")) / col("prev_enrollment")
    )
    return df.select("fips", "report_year", "total_enrollment", "enrollment_growth_rate")
