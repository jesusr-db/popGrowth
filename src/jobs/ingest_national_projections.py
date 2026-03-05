"""Entry point for national projections ingestion job."""
from src.ingestion.national_projections import ingest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest-national-projections").getOrCreate()

ingest(spark)
