"""Unit tests for Spark session helper and environment validation."""

from pathlib import Path
from unittest.mock import patch

import pytest

from nse_pipeline.utils.spark import (
    check_python_spark_compatibility,
    check_windows_hadoop_environment,
    get_spark_session,
)


def test_windows_hadoop_environment_missing_home():
    """Verify check_windows_hadoop_environment fails when HADOOP_HOME is unset."""
    with patch("sys.platform", "win32"), patch.dict("os.environ", {}, clear=True):
        with pytest.raises(RuntimeError, match="HADOOP_HOME environment variable is not set"):
            check_windows_hadoop_environment()


def test_windows_hadoop_environment_missing_winutils(tmp_path: Path):
    """Verify check_windows_hadoop_environment fails when winutils.exe does not exist."""
    fake_hadoop = tmp_path / "hadoop"
    fake_hadoop.mkdir()

    with (
        patch("sys.platform", "win32"),
        patch.dict("os.environ", {"HADOOP_HOME": str(fake_hadoop)}),
    ):
        with pytest.raises(RuntimeError, match="winutils.exe was not found"):
            check_windows_hadoop_environment()


def test_windows_hadoop_environment_success(tmp_path: Path):
    """Verify check_windows_hadoop_environment passes when winutils.exe exists."""
    fake_hadoop = tmp_path / "hadoop"
    bin_dir = fake_hadoop / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "winutils.exe").write_text("dummy binary", encoding="utf-8")

    with (
        patch("sys.platform", "win32"),
        patch.dict("os.environ", {"HADOOP_HOME": str(fake_hadoop)}),
    ):
        # Should not raise
        check_windows_hadoop_environment()


def test_python_spark_compatibility_fails_on_spark3_python313():
    """Verify compatibility check catches PySpark < 4 on Python 3.13+."""
    with patch("sys.version_info", (3, 13, 0)), patch("pyspark.__version__", "3.5.4"):
        with pytest.raises(RuntimeError, match="requires PySpark 4.0.0+"):
            check_python_spark_compatibility()


def test_python_spark_compatibility_passes_on_spark4():
    """Verify compatibility check passes when PySpark >= 4 is used."""
    with patch("sys.version_info", (3, 13, 0)), patch("pyspark.__version__", "4.2.0"):
        # Should not raise
        check_python_spark_compatibility()


def test_get_spark_session_singleton(spark_session):
    """Verify get_spark_session returns an active SparkSession and reuses it."""
    s1 = get_spark_session()
    assert s1 is not None
    assert s1.version.startswith("4.")
    df = s1.createDataFrame([(1, "ok")], ["id", "status"])
    assert df.count() == 1
