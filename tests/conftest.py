"""Shared PyTest fixtures for the test suite."""

import os
from collections.abc import Generator
from datetime import date

import psycopg
import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from nse_pipeline.load.db import (
    apply_sql_migrations,
    get_db_config,
    populate_dim_date,
    seed_dim_symbols,
)
from nse_pipeline.utils.spark import get_spark_session, stop_spark_session

load_dotenv()

_TEST_DB_NAME = "nse_warehouse_test"


def _postgres_reachable() -> bool:
    """Check if PostgreSQL is reachable (needed to decide whether to create the test DB)."""
    try:
        cfg = get_db_config()
        # Connect to the 'postgres' maintenance database to check connectivity
        conn = psycopg.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname="postgres",
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=3,
            autocommit=True,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Provides a single session-scoped PySpark SparkSession for fast test execution."""
    spark = get_spark_session(
        app_name="nse-test-suite",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "2"},
    )
    yield spark
    stop_spark_session()


@pytest.fixture(scope="session")
def postgres_test_db() -> Generator[dict, None, None]:
    """Session-scoped fixture that creates an isolated nse_warehouse_test database.

    Safety guard: raises ValueError if the computed test DB name does not end in '_test'.
    This prevents accidentally running destructive operations against production data.
    """
    if not _postgres_reachable():
        pytest.skip("PostgreSQL is not reachable (start with: docker compose up -d)")

    test_db_name = _TEST_DB_NAME
    if not test_db_name.endswith("_test"):
        raise ValueError(
            f"Test database name '{test_db_name}' must end in '_test' to prevent "
            "accidental pollution of production data."
        )

    cfg = get_db_config()

    # Connect to the maintenance 'postgres' database to CREATE / DROP the test DB
    admin_conn = psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname="postgres",
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=3,
        autocommit=True,
    )

    with admin_conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name};")
        cur.execute(f"CREATE DATABASE {test_db_name};")

    # Apply migrations and seed dimensions in the test DB
    test_cfg = {**cfg, "dbname": test_db_name}
    test_conn = psycopg.connect(**test_cfg, autocommit=True)
    apply_sql_migrations(test_conn, sql_dir="sql")
    seed_dim_symbols(test_conn, sectors_path="config/sectors.csv")
    populate_dim_date(test_conn, start_date=date(2020, 1, 1), end_date=date(2030, 12, 31))
    test_conn.close()

    # Override the env var so all get_db_connection() calls inside tests use the test DB
    old_db_env = os.environ.get("POSTGRES_DB")
    os.environ["POSTGRES_DB"] = test_db_name

    yield test_cfg

    # Teardown: restore env and drop the test database
    if old_db_env is not None:
        os.environ["POSTGRES_DB"] = old_db_env
    else:
        del os.environ["POSTGRES_DB"]

    with admin_conn.cursor() as cur:
        # Terminate any lingering connections to the test DB before dropping
        cur.execute(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{test_db_name}';"
        )
        cur.execute(f"DROP DATABASE IF EXISTS {test_db_name};")

    admin_conn.close()


def pytest_configure(config):
    """Register the 'postgres_integration' custom mark."""
    config.addinivalue_line(
        "markers",
        "postgres_integration: tests that require a live PostgreSQL container",
    )
