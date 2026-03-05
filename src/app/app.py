"""Databricks App entry point — sets up sys.path and runs uvicorn."""
import sys
import os

# The app runs from src/app/ but needs src/ on the path for imports like
# src.common.config. Add the parent of src/ (the bundle root) to sys.path.
_app_dir = os.path.dirname(os.path.abspath(__file__))
_src_dir = os.path.dirname(_app_dir)
_root_dir = os.path.dirname(_src_dir)
sys.path.insert(0, _root_dir)

import uvicorn

if __name__ == "__main__":
    uvicorn.run("src.app.backend.main:app", host="0.0.0.0", port=8000)
