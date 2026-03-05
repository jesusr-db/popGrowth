"""Transform Bronze vacancy data to Silver with YoY change."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window


def transform_vacancy(bronze_df: DataFrame) -> DataFrame:
    """Compute vacancy rate and year-over-year change."""
    df = bronze_df.withColumn(
        "vacancy_rate",
        col("vacant_addresses") / col("total_addresses")
    )
    window = Window.partitionBy("fips").orderBy("report_year", "report_quarter")
    df = df.withColumn(
        "prev_vacancy_rate", lag("vacancy_rate").over(window)
    ).withColumn(
        "vacancy_rate_yoy_change",
        col("vacancy_rate") - col("prev_vacancy_rate")
    )
    return df.select(
        "fips", "report_year", "report_quarter",
        "vacancy_rate", "vacancy_rate_yoy_change"
    )
