"""Entry point for building permits ingestion job."""
from src.ingestion.building_permits import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-building-permits").getOrCreate()

# Ingest current month
now = datetime.now()
ingest(spark, now.year, now.month)
