"""Entry point for vacancy ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.vacancy import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-vacancy").getOrCreate()

now = datetime.now()
quarter = (now.month - 1) // 3 + 1
ingest(spark, now.year, quarter)
