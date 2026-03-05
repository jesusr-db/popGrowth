"""Entry point for employment ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.employment import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-employment").getOrCreate()

now = datetime.now()
quarter = (now.month - 1) // 3 + 1
ingest(spark, now.year, quarter)
