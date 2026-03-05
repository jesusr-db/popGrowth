"""Transform Bronze building permits to Silver — aggregate monthly to quarterly at county level."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, quarter as spark_quarter,
    year as spark_year, to_date, concat, lit,
)


def transform_building_permits(bronze_df: DataFrame) -> DataFrame:
    """Aggregate monthly building permits to quarterly at county FIPS level."""
    # Census survey_date format is "YYYYMM" (e.g., "202401")
    parsed = bronze_df.withColumn(
        "parsed_date", to_date(col("survey_date"), "yyyyMM")
    ).withColumn(
        "report_year", spark_year("parsed_date")
    ).withColumn(
        "report_quarter", spark_quarter("parsed_date")
    )

    aggregated = (
        parsed
        .groupBy("fips", "county_name", "report_year", "report_quarter")
        .agg(
            spark_sum("total_units").alias("total_units_permitted"),
            spark_sum("single_family_units").alias("single_family_units"),
            spark_sum("multi_family_units").alias("multi_family_units"),
        )
    )
    return aggregated
