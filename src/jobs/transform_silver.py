"""Entry point for Silver transforms job."""
from src.silver.transforms import run_all_silver_transforms
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver-transforms").getOrCreate()

run_all_silver_transforms(spark)
