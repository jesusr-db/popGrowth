"""Entry point for Gold scoring job."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src.gold.compute_scores import run_gold_scoring
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold-scoring").getOrCreate()

run_gold_scoring(spark)
