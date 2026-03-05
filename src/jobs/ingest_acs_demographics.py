"""Entry point for ACS demographics ingestion job."""
from src.ingestion.acs_demographics import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-acs-demographics").getOrCreate()

ingest(spark, datetime.now().year)
