"""Databricks App entry point — runs uvicorn."""
import sys
import os

# Ensure the app directory is on sys.path so 'backend' is importable
_app_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _app_dir)

import uvicorn

uvicorn.run("backend.main:app", host="0.0.0.0", port=8000)
