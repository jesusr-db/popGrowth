"""Entry point for Gold scoring job."""
from src.gold.compute_scores import run_gold_scoring
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold-scoring").getOrCreate()

run_gold_scoring(spark)
