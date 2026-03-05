"""Transform Bronze HUD construction permits to Silver with growth rate."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window


def transform_hud_construction(bronze_df: DataFrame) -> DataFrame:
    """Compute QoQ construction growth rate."""
    window = Window.partitionBy("fips").orderBy("report_year", "report_quarter")
    df = bronze_df.withColumn(
        "prev_permitted", lag("permitted_units").over(window)
    ).withColumn(
        "construction_growth_rate",
        (col("permitted_units") - col("prev_permitted")) / col("prev_permitted")
    )
    return df.select(
        "fips", "report_year", "report_quarter",
        "permitted_units", "construction_growth_rate"
    )
