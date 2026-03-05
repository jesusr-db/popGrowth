"""Entry point for Silver transforms job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.silver.transforms import run_all_silver_transforms
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver-transforms").getOrCreate()

run_all_silver_transforms(spark)
