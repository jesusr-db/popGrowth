"""Entry point for school enrollment ingestion job."""
from src.ingestion.school_enrollment import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-school-enrollment").getOrCreate()

ingest(spark, datetime.now().year)
