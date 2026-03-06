"""Lakebase PostgreSQL connection for the FastAPI app."""

import os
import logging
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_cached_credential: dict | None = None
_credential_expiry: float = 0


def _get_lakebase_credential() -> dict:
    """Get Lakebase database credential via Databricks SDK.

    Uses generate_database_credential() for a short-lived OAuth token.
    Falls back to the SP's workspace token if the database API is unavailable.
    Credentials are cached for 50 minutes (tokens expire at 60 min).
    """
    global _cached_credential, _credential_expiry

    if _cached_credential and time.time() < _credential_expiry:
        return _cached_credential

    instance_name = os.environ.get("LAKEBASE_INSTANCE", "store-siting-app")

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        cred = w.database.generate_database_credential(instance_names=[instance_name])
        user = os.environ.get("PGUSER") or w.current_user.me().user_name

        _cached_credential = {
            "user": user,
            "password": cred.token,
        }
        _credential_expiry = time.time() + 3000
        logger.debug("Generated Lakebase credential for user=%s", user)
        return _cached_credential

    except Exception as e:
        logger.warning("SDK credential generation failed: %s, falling back to workspace token", e)
        try:
            from databricks.sdk import WorkspaceClient
            w2 = WorkspaceClient()
            token = w2.config.token
            user = os.environ.get("PGUSER") or w2.current_user.me().user_name
            if token:
                logger.debug("Using workspace OAuth token for user=%s", user)
                return {"user": user, "password": token}
        except Exception as e2:
            logger.warning("Workspace token fallback also failed: %s", e2)
        token = os.environ.get("DATABRICKS_TOKEN", "")
        user = os.environ.get("PGUSER", "token")
        return {"user": user, "password": token}


@contextmanager
def get_cursor():
    host = os.environ.get("PGHOST") or os.environ.get("LAKEBASE_HOST", "")
    port = int(os.environ.get("PGPORT", "5432"))
    database = os.environ.get("PGDATABASE") or os.environ.get("LAKEBASE_DATABASE", "store_siting_app")
    cred = _get_lakebase_credential()

    logger.debug("Connecting to Lakebase at %s db=%s user=%s", host, database, cred["user"])

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=cred["user"],
        password=cred["password"],
        sslmode="require",
    )
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_query(query: str, params: tuple | None = None) -> list[dict]:
    with get_cursor() as cursor:
        cursor.execute(query, params)
        if cursor.description is None:
            return []
        return [dict(row) for row in cursor.fetchall()]
