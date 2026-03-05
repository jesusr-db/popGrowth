"""Entry point for building permits ingestion job."""
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

from src.ingestion.building_permits import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-building-permits").getOrCreate()

# Census data lags ~2 months; ingest previous month
now = datetime.now()
month = now.month - 2 if now.month > 2 else now.month + 10
year = now.year if now.month > 2 else now.year - 1
ingest(spark, year, month)
