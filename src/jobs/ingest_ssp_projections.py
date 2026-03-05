"""Entry point for SSP projections ingestion job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.ingestion.ssp_projections import ingest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest-ssp-projections").getOrCreate()

ingest(spark)
