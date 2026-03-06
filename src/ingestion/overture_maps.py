"""Ingest Overture Maps places and transportation segments into Bronze.

Overture Maps Foundation publishes open GeoParquet data on S3 and Azure.
This module reads the places and transportation/segment themes directly
from cloud storage and writes filtered US data to Bronze Delta tables.

Data paths are configurable via the OVERTURE_RELEASE environment variable.
Default: 2026-02-18.0 (latest as of March 2026).

See https://docs.overturemaps.org/getting-data/ for release info.
"""

import os
from datetime import datetime, timezone

# Default Overture release — override via env var if a newer release is available
OVERTURE_RELEASE = os.environ.get("OVERTURE_RELEASE", "2026-02-18.0")

# S3 base path for Overture Maps data
OVERTURE_S3_BASE = os.environ.get(
    "OVERTURE_S3_BASE",
    f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}",
)

# Azure Blob base path (alternative)
OVERTURE_AZURE_BASE = os.environ.get(
    "OVERTURE_AZURE_BASE",
    f"wasbs://release@overturemapswestus2.blob.core.windows.net/{OVERTURE_RELEASE}",
)

# Theme paths relative to base
PLACES_PATH = "theme=places/type=place"
SEGMENTS_PATH = "theme=transportation/type=segment"

# US bounding box (continental US + Alaska + Hawaii generous bounds)
US_BBOX = {
    "min_lon": -180.0,  # Alaska extends past -180
    "max_lon": -66.0,
    "min_lat": 17.5,    # Includes Puerto Rico, US Virgin Islands
    "max_lat": 72.0,    # Northern Alaska
}


def _get_data_path(theme_path: str, storage: str = "s3") -> str:
    """Build the full data path for an Overture theme."""
    if storage == "azure":
        return f"{OVERTURE_AZURE_BASE}/{theme_path}"
    return f"{OVERTURE_S3_BASE}/{theme_path}"


def ingest_places(spark, catalog: str | None = None, storage: str = "s3"):
    """Read Overture Places GeoParquet from cloud storage, filter to US, write to Bronze.

    Overture Places schema (key fields):
    - id: string (unique identifier)
    - geometry: binary (WKB point)
    - names.primary: string (place name)
    - categories.primary: string (place category)
    - categories.alternate: array<string>
    - brand.names.primary: string (brand name)
    - addresses[0].country: string (ISO country code)
    - bbox: struct (xmin, xmax, ymin, ymax)
    """
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from pyspark.sql.functions import col, current_timestamp, lit
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    path = _get_data_path(PLACES_PATH, storage)

    try:
        df = spark.read.parquet(path)

        # Filter to US using bbox coordinates
        # Overture provides bbox.xmin, bbox.xmax, bbox.ymin, bbox.ymax at top level
        us_df = df.filter(
            (col("bbox.xmin") >= US_BBOX["min_lon"])
            & (col("bbox.xmax") <= US_BBOX["max_lon"])
            & (col("bbox.ymin") >= US_BBOX["min_lat"])
            & (col("bbox.ymax") <= US_BBOX["max_lat"])
        )

        # Add ingestion metadata
        us_df = (
            us_df
            .withColumn("overture_release", lit(OVERTURE_RELEASE))
            .withColumn("ingested_at", current_timestamp())
        )

        table = f"{catalog}.bronze.overture_places"
        us_df.write.mode("overwrite").saveAsTable(table)

        count = us_df.count()
        log_ingestion(
            spark, "overture_places", "success", count,
            started_at=started_at, catalog=catalog,
        )
    except Exception as e:
        log_ingestion(
            spark, "overture_places", "failure",
            error_msg=str(e)[:500], started_at=started_at, catalog=catalog,
        )
        raise


def ingest_segments(spark, catalog: str | None = None, storage: str = "s3"):
    """Read Overture Transportation Segments from cloud storage, filter to US, write to Bronze.

    Overture Segments schema (key fields):
    - id: string (unique identifier)
    - geometry: binary (WKB linestring)
    - class: string (functional class: motorway, trunk, primary, secondary, etc.)
    - subclass: string (more specific road type)
    - names.primary: string (road name)
    - bbox: struct (xmin, xmax, ymin, ymax)
    """
    if catalog is None:
        from src.common.config import CATALOG
        catalog = CATALOG
    from pyspark.sql.functions import col, current_timestamp, lit
    from src.common.ingestion_logger import log_ingestion

    started_at = datetime.now(timezone.utc)
    path = _get_data_path(SEGMENTS_PATH, storage)

    try:
        df = spark.read.parquet(path)

        # Filter to US using bbox coordinates
        us_df = df.filter(
            (col("bbox.xmin") >= US_BBOX["min_lon"])
            & (col("bbox.xmax") <= US_BBOX["max_lon"])
            & (col("bbox.ymin") >= US_BBOX["min_lat"])
            & (col("bbox.ymax") <= US_BBOX["max_lat"])
        )

        # Add ingestion metadata
        us_df = (
            us_df
            .withColumn("overture_release", lit(OVERTURE_RELEASE))
            .withColumn("ingested_at", current_timestamp())
        )

        table = f"{catalog}.bronze.overture_segments"
        us_df.write.mode("overwrite").saveAsTable(table)

        count = us_df.count()
        log_ingestion(
            spark, "overture_segments", "success", count,
            started_at=started_at, catalog=catalog,
        )
    except Exception as e:
        log_ingestion(
            spark, "overture_segments", "failure",
            error_msg=str(e)[:500], started_at=started_at, catalog=catalog,
        )
        raise


def ingest(spark, catalog: str | None = None, storage: str = "s3"):
    """Full ingestion: read both places and segments from Overture Maps."""
    ingest_places(spark, catalog=catalog, storage=storage)
    ingest_segments(spark, catalog=catalog, storage=storage)
