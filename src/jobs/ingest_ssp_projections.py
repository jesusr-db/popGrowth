"""Entry point for SSP projections ingestion job."""
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

from src.ingestion.ssp_projections import ingest
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest-ssp-projections").getOrCreate()

ingest(spark)
