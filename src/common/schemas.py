"""Shared PySpark schemas for Bronze and Silver tables."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType,
)


def bronze_metadata_fields():
    """Common metadata fields appended to all Bronze tables."""
    return [
        StructField("source_date", DateType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
