"""Databricks SQL Connector for the FastAPI app."""

import os
import logging
from databricks import sql as databricks_sql
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def get_connection_params():
    host = os.environ.get("DATABRICKS_HOST", "")
    # Strip protocol prefix if present — connector expects just the hostname
    host = host.replace("https://", "").replace("http://", "").rstrip("/")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")

    logger.debug("Connecting to %s with http_path=%s", host, http_path)

    params = {
        "server_hostname": host,
        "http_path": http_path,
    }

    # Databricks Apps inject DATABRICKS_TOKEN or use default credentials
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if token:
        params["access_token"] = token
    else:
        # Use Databricks SDK credentials provider (works in Databricks Apps)
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            params["credentials_provider"] = w.config.authenticate
            logger.debug("Using SDK credentials provider")
        except Exception as e:
            logger.warning("Could not initialize SDK credentials: %s", e)

    return params


@contextmanager
def get_cursor():
    params = get_connection_params()
    conn = databricks_sql.connect(**params)
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute_query(query: str, params: dict | None = None) -> list[dict]:
    with get_cursor() as cursor:
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
