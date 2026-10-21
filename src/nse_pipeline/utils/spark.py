"""PySpark session initialization and environment management.

Why this design:
PySpark on Windows requires specific environment setup:
1. PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON must explicitly point to sys.executable
   to prevent workers from invoking a different Python binary or failing to spawn.
2. Hadoop winutils.exe must be present under HADOOP_HOME\\bin on Windows to support
   local filesystem operations (POSIX permission emulation and CRC file creation).
3. Python 3.13 requires PySpark 4.0.0+; earlier PySpark 3.5.x versions crash their Python worker
   processes due to internal changes in Python 3.13's C-API and threading models.
"""

import os
import sys
from pathlib import Path
from typing import Any

import pyspark
from pyspark.sql import SparkSession

from nse_pipeline.utils.logging import logger

_SPARK_INSTANCE: SparkSession | None = None


def check_windows_hadoop_environment() -> None:
    """Verifies that HADOOP_HOME and winutils.exe are configured on Windows."""
    if sys.platform != "win32":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        raise RuntimeError(
            "HADOOP_HOME environment variable is not set.\n"
            "On Windows, PySpark requires Hadoop winutils.exe to operate with local filesystems.\n"
            "Action required:\n"
            "1. Download Hadoop 3.4.0 winutils from https://github.com/steveloughran/winutils\n"
            "2. Extract to C:\\hadoop (so that C:\\hadoop\\bin\\winutils.exe exists)\n"
            "3. Set system environment variable HADOOP_HOME=C:\\hadoop\n"
            "4. Add %HADOOP_HOME%\\bin to your system PATH."
        )

    winutils_path = Path(hadoop_home) / "bin" / "winutils.exe"
    if not winutils_path.is_file():
        raise RuntimeError(
            f"winutils.exe was not found at expected path: {winutils_path}\n"
            "Action required: Ensure winutils.exe is placed inside %HADOOP_HOME%\\bin."
        )


def check_python_spark_compatibility() -> None:
    """Verifies PySpark version compatibility with the active Python runtime."""
    py_major, py_minor = sys.version_info[:2]
    spark_major = int(pyspark.__version__.split(".")[0])

    if py_major == 3 and py_minor >= 13 and spark_major < 4:
        raise RuntimeError(
            f"Incompatible environment: Python {py_major}.{py_minor} requires PySpark 4.0.0+.\n"
            f"Current PySpark version is {pyspark.__version__}.\n"
            "PySpark 3.5.x worker processes fail under Python 3.13 due to changes in interpreter internals.\n"
            "Action required: Upgrade to pyspark>=4.0.0 (e.g. pip install pyspark==4.2.0)."
        )


def get_spark_session(
    app_name: str = "nse-pipeline",
    master: str = "local[*]",
    extra_configs: dict[str, Any] | None = None,
) -> SparkSession:
    """Initializes or retrieves a singleton PySpark SparkSession configured for local batch pipeline work.

    Args:
        app_name: Name of the Spark application.
        master: Spark master URL (defaults to local[*]).
        extra_configs: Optional key-value dictionary of extra Spark configuration parameters.

    Returns:
        Configured SparkSession instance.
    """
    global _SPARK_INSTANCE

    if _SPARK_INSTANCE is not None:
        try:
            # Check if active
            if not _SPARK_INSTANCE._jsc.sc().isStopped():
                return _SPARK_INSTANCE
        except Exception:
            _SPARK_INSTANCE = None

    # 1. Environment checks
    check_python_spark_compatibility()
    check_windows_hadoop_environment()

    # 2. Worker Python executable alignment
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # 3. Local scratch and warehouse directories inside project area
    base_dir = Path(".").resolve()
    temp_dir = base_dir / "data" / "tmp" / "spark"
    temp_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir = base_dir / "data" / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    # 4. Configure Spark Session Builder
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse_dir.as_uri())
        .config("spark.local.dir", str(temp_dir))
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, str(v))

    logger.debug("Creating new SparkSession for %s (master: %s)...", app_name, master)
    _SPARK_INSTANCE = builder.getOrCreate()
    _SPARK_INSTANCE.sparkContext.setLogLevel("WARN")

    return _SPARK_INSTANCE


def stop_spark_session() -> None:
    """Stops the active SparkSession and resets the singleton instance."""
    global _SPARK_INSTANCE
    if _SPARK_INSTANCE is not None:
        try:
            _SPARK_INSTANCE.stop()
        except Exception as e:
            logger.debug("Error stopping SparkSession: %s", e)
        finally:
            _SPARK_INSTANCE = None
