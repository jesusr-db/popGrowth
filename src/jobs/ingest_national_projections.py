"""Entry point for national projections ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.national_projections import ingest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest-national-projections").getOrCreate()

ingest(spark)
