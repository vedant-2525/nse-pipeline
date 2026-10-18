"""Logging configuration for the pipeline.

We configure standard library logging to provide structured, consistent console
output across local development, Airflow execution, and container environments.
"""

import logging
import os
import sys
from pathlib import Path

_INITIALIZED = False


def setup_logger(
    name: str = "nse_pipeline",
    log_level: str | None = None,
    log_file: Path | str | None = None,
) -> logging.Logger:
    """Configures and returns a named logger.

    Why this design:
    Data pipelines require timestamps and module origins in every log line to
    diagnose long-running batch failures and backfill issues.
    """
    global _INITIALIZED

    level_str = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if logger was already initialized
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Standard out handler for Docker/Airflow container log capture
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # Optional file handler for persistent local logs
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


# Default application logger instance
logger = setup_logger()
