"""Entry point for business patterns ingestion job."""
from src.ingestion.business_patterns import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-business-patterns").getOrCreate()

ingest(spark, datetime.now().year)
