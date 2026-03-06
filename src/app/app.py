"""Databricks App entry point — runs uvicorn."""
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("popGrowth")

# Ensure the app directory is on sys.path so 'backend' is importable
_app_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _app_dir)

logger.info("App dir: %s", _app_dir)
logger.info("DATABRICKS_HOST: %s", os.environ.get("DATABRICKS_HOST", "<NOT SET>"))
logger.info("DATABRICKS_HTTP_PATH: %s", os.environ.get("DATABRICKS_HTTP_PATH", "<NOT SET>"))
logger.info("CATALOG: %s", os.environ.get("CATALOG", "<NOT SET>"))

import uvicorn

uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, log_level="debug")
