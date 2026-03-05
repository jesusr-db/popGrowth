"""Entry point for ACS demographics ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.acs_demographics import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-acs-demographics").getOrCreate()

ingest(spark, datetime.now().year)
