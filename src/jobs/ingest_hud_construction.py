"""Entry point for HUD construction ingestion job."""
from src.ingestion.hud_construction import ingest
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-hud-construction").getOrCreate()

now = datetime.now()
quarter = (now.month - 1) // 3 + 1
ingest(spark, now.year, quarter)
