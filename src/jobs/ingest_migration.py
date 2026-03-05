"""Entry point for migration ingestion job."""
import sys, os
# On Databricks, __file__ may not be defined; use the workspace files path
try:
    _dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _dir = os.getcwd()
# Walk up to find the bundle files root (contains src/)
_root = _dir
for _ in range(5):
    if os.path.isdir(os.path.join(_root, 'src', 'common')):
        break
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)

from src.ingestion.migration import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-migration").getOrCreate()

# IRS SOI migration data lags ~3 years (latest is typically 2021-2022)
ingest(spark, 2022, 4)
