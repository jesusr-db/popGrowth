"""Databricks SQL Connector for the FastAPI app."""

import os
from databricks import sql as databricks_sql
from contextlib import contextmanager


def get_connection_params():
    return {
        "server_hostname": os.environ["DATABRICKS_HOST"],
        "http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "access_token": os.environ.get("DATABRICKS_TOKEN", ""),
    }


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
