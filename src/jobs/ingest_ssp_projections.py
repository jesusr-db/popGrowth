"""Entry point for SSP projections ingestion job."""
from src.ingestion.ssp_projections import ingest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest-ssp-projections").getOrCreate()

ingest(spark)
