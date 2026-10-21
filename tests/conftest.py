"""Shared PyTest fixtures for the test suite."""

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from nse_pipeline.utils.spark import get_spark_session, stop_spark_session


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
