"""Transform Bronze business patterns to Silver — filter QSR, compute density."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, lit


def transform_business_patterns(bronze_df: DataFrame, population_df: DataFrame) -> DataFrame:
    """Filter NAICS 7222 (QSR), compute QSR establishments and retail density."""
    qsr = bronze_df.filter(col("naics_code") == "7222")
    qsr_agg = (
        qsr.groupBy("fips", "report_year")
        .agg(
            spark_sum("establishments").alias("qsr_establishments"),
            spark_sum("employees").alias("qsr_employees"),
        )
    )
    total_agg = (
        bronze_df.groupBy("fips", "report_year")
        .agg(spark_sum("establishments").alias("total_establishments"))
    )
    joined = total_agg.join(qsr_agg, on=["fips", "report_year"], how="left")
    joined = joined.join(population_df, on="fips", how="left")
    joined = joined.withColumn(
        "retail_density",
        col("total_establishments") / col("population") * 1000
    )
    return joined.select(
        "fips", "report_year", "total_establishments",
        "qsr_establishments", "retail_density"
    )
