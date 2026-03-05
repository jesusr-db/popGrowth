"""Log ingestion runs to a tracking table for observability."""

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


INGESTION_LOG_SCHEMA = StructType([
    StructField("source_name", StringType(), False),
    StructField("status", StringType(), False),  # "success" | "failure"
    StructField("rows_loaded", IntegerType(), True),
    StructField("error_msg", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), False),
])


def log_ingestion(
    spark: SparkSession,
    source_name: str,
    status: str,
    rows_loaded: int | None = None,
    error_msg: str | None = None,
    started_at: datetime | None = None,
    catalog: str | None = None,
):
    """Write a row to the ingestion log table."""
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    now = datetime.now(timezone.utc)
    row = [(
        source_name,
        status,
        rows_loaded,
        error_msg,
        started_at or now,
        now,
    )]
    df = spark.createDataFrame(row, schema=INGESTION_LOG_SCHEMA)
    df.write.mode("append").saveAsTable(f"{catalog}.bronze._ingestion_log")
