"""Entry point for building permits ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.building_permits import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-building-permits").getOrCreate()

# Ingest current month
now = datetime.now()
ingest(spark, now.year, now.month)
