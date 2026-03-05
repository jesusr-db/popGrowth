"""Entry point for school enrollment ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.school_enrollment import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-school-enrollment").getOrCreate()

ingest(spark, datetime.now().year)
