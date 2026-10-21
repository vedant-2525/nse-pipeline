"""Data quality validation gates that enforce hard invariants on Silver output.

Why this design:
A data pipeline should fail fast and loudly when corrupted records slip through
cleaning rules. If a hard quality invariant fails, the transformation run raises
a DataQualityError and halts execution before corrupted data can reach the warehouse.
Every check logs its status (passed or failed) for auditability.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nse_pipeline.utils.logging import logger


class DataQualityError(Exception):
    """Raised when a critical Data Quality invariant fails."""

    pass


def validate_silver_quality(
    df: DataFrame,
    expected_date: date,
    min_row_count: int = 40,
    max_row_count: int = 3500,
) -> None:
    """Runs data quality gates on the clean Silver DataFrame.

    Raises:
        DataQualityError: If any critical check is violated.
    """
    total_rows = df.count()
    logger.info("Starting Data Quality gate validation on %d records...", total_rows)

    # 1. Row count bounds check
    if not (min_row_count <= total_rows <= max_row_count):
        msg = (
            f"DQ FAIL [Row Count Bounds]: Record count {total_rows} is outside plausible range "
            f"[{min_row_count}, {max_row_count}]."
        )
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info(
        "DQ PASS: Row count within bounds (%d in [%d, %d]).",
        total_rows,
        min_row_count,
        max_row_count,
    )

    # 2. Partition Date Matching
    distinct_dates = [r[0] for r in df.select("trade_date").distinct().collect()]
    if len(distinct_dates) != 1 or distinct_dates[0] != expected_date:
        msg = (
            f"DQ FAIL [Partition Date Mismatch]: Expected all records to have trade_date={expected_date}, "
            f"but found dates: {distinct_dates}."
        )
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info("DQ PASS: All records match requested partition date: %s.", expected_date)

    # 3. No Duplicate Keys on (trade_date, symbol, series)
    distinct_keys = df.select("trade_date", "symbol", "series").distinct().count()
    if distinct_keys != total_rows:
        duplicates = total_rows - distinct_keys
        msg = f"DQ FAIL [Duplicate Keys]: Found {duplicates} duplicate key records on (trade_date, symbol, series)."
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info("DQ PASS: No duplicate keys detected (%d unique keys).", distinct_keys)

    # 4. Inverted High / Low check: high >= low
    inverted_count = df.filter(F.col("high") < F.col("low")).count()
    if inverted_count > 0:
        msg = f"DQ FAIL [Inverted High-Low]: Found {inverted_count} records where high < low."
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info("DQ PASS: High >= Low invariant satisfied across all records.")

    # 5. Price bounds check: open, close, last within [low, high]
    out_of_bounds = df.filter(
        (F.col("open") < F.col("low"))
        | (F.col("open") > F.col("high"))
        | (F.col("close") < F.col("low"))
        | (F.col("close") > F.col("high"))
        | (F.col("last") < F.col("low"))
        | (F.col("last") > F.col("high"))
    ).count()

    if out_of_bounds > 0:
        msg = f"DQ FAIL [Price Outside Bounds]: Found {out_of_bounds} records with open/close/last outside [low, high]."
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info(
        "DQ PASS: Open, Close, and Last prices strictly within [Low, High] for all records."
    )

    # 6. Positive Prices check: prices > 0
    non_positive = df.filter(
        (F.col("open") <= 0)
        | (F.col("high") <= 0)
        | (F.col("low") <= 0)
        | (F.col("close") <= 0)
        | (F.col("last") <= 0)
        | (F.col("prev_close") <= 0)
    ).count()

    if non_positive > 0:
        msg = f"DQ FAIL [Non-Positive Price]: Found {non_positive} records with price <= 0."
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info("DQ PASS: All prices strictly positive (> 0).")

    # 7. Non-negative volume check: volume >= 0
    neg_vol = df.filter(F.col("volume") < 0).count()
    if neg_vol > 0:
        msg = f"DQ FAIL [Negative Volume]: Found {neg_vol} records with volume < 0."
        logger.error(msg)
        raise DataQualityError(msg)
    logger.info("DQ PASS: All volume metrics non-negative (>= 0).")

    logger.info("ALL DATA QUALITY GATES PASSED for %s.", expected_date)
