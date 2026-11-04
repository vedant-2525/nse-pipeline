"""Idempotent warehouse loader that transfers clean Silver Parquet data into PostgreSQL Gold.

Why this design:
1. PyArrow In-Memory Reader: Bypasses PySpark and JVM overhead for fast, lightweight loading.
2. Staging Table + Atomic Upsert: Uses COPY into a temporary table followed by an atomic INSERT ... ON CONFLICT
   DO UPDATE in a single database transaction. Mid-load failures roll back completely without partial state.
3. Strict Governance & Pre-Load Gating: Refuses unvalidated, imbalanced, or fallback partitions unless explicitly permitted.
4. Independent Ops Logging: Pipeline execution logs and data quality results are committed on an isolated connection
   so execution failures are reliably recorded even when the core data transaction rolls back.
"""

import json
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import pyarrow.parquet as pq

from nse_pipeline.load.db import get_db_connection
from nse_pipeline.utils.logging import logger


class PartitionValidationError(Exception):
    """Raised when a Silver partition fails pre-load metadata validation."""

    pass


def validate_partition_metadata(
    partition_dir: Path,
    trade_date: date,
    allow_fallback: bool = False,
    include_all_symbols: bool = False,
) -> dict[str, Any]:
    """Inspects and validates _SUCCESS.meta.json before loading into warehouse.

    Raises:
        FileNotFoundError: If partition directory or _SUCCESS.meta.json is missing.
        PermissionError: If partition source is fallback and allow_fallback=False, or if
                         universe is 'all_eq' and include_all_symbols=False.
        PartitionValidationError: If reconciliation fields or universe are missing or imbalanced.
    """
    if not partition_dir.is_dir():
        raise FileNotFoundError(f"Silver partition directory not found at: {partition_dir}")

    meta_file = partition_dir / "_SUCCESS.meta.json"
    if not meta_file.is_file():
        raise FileNotFoundError(
            f"Missing partition audit file: {meta_file}. Partition may be incomplete or corrupted."
        )

    with open(meta_file, encoding="utf-8") as f:
        meta = json.load(f)

    # 1. Source Governance Check
    source_type = meta.get("source_type", "unknown")
    if source_type != "official" and not allow_fallback:
        raise PermissionError(
            f"Silver partition for {trade_date} has source_type='{source_type}'.\n"
            "By default, the warehouse refuses fallback data to maintain reporting integrity.\n"
            "To explicitly load fallback data, pass the flag: --allow-fallback"
        )

    # 2. Universe Guard Check
    if "universe" not in meta:
        date_str = trade_date.strftime("%Y-%m-%d")
        raise PartitionValidationError(
            f"Silver partition metadata for {trade_date} is missing the 'universe' field (older schema layout). "
            f"Please re-run the silver transformation: python -m nse_pipeline.cli transform --date {date_str}"
        )

    universe = meta.get("universe")
    symbol_count = meta.get("symbol_count", "unknown")
    logger.info(
        "Silver partition universe: '%s' (symbols: %s) for date %s",
        universe,
        symbol_count,
        trade_date,
    )

    if universe == "all_eq" and not include_all_symbols:
        raise PermissionError(
            f"Silver partition for {trade_date} contains full equity universe ('all_eq', {symbol_count} symbols).\n"
            "By default, the warehouse refuses full market loads to maintain curated index consistency.\n"
            "To explicitly load the full universe, re-run with the flag: --include-all-symbols"
        )

    # 3. Required Reconciliation Fields Check
    required_fields = [
        "read_count",
        "non_equity_filtered",
        "universe_filtered",
        "rejected",
        "deduped",
        "row_count",
    ]
    missing = [f for f in required_fields if f not in meta]
    if missing:
        raise PartitionValidationError(
            f"Partition metadata missing reconciliation fields: {missing}."
        )

    # 3. Reconciliation Balance Invariant
    read_c = meta["read_count"]
    accounted_c = (
        meta["non_equity_filtered"]
        + meta["universe_filtered"]
        + meta["rejected"]
        + meta["deduped"]
        + meta["row_count"]
    )
    if read_c != accounted_c:
        raise PartitionValidationError(
            f"Partition row-count reconciliation is imbalanced: read={read_c} vs accounted={accounted_c}."
        )

    return meta


def log_pipeline_run(
    run_id: uuid.UUID,
    stage: str,
    trade_date: date,
    status: str,
    rows_in: int | None,
    rows_out: int | None,
    error: str | None,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Logs pipeline execution outcome to ops.pipeline_run_log using an isolated connection."""
    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.pipeline_run_log (
                        run_id, stage, trade_date, status, rows_in, rows_out, error, started_at, finished_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        run_id,
                        stage,
                        trade_date,
                        status,
                        rows_in,
                        rows_out,
                        error,
                        started_at,
                        finished_at,
                    ),
                )
    except Exception as e:
        logger.error("Failed to write to ops.pipeline_run_log: %s", e)


def log_dq_result(
    run_id: uuid.UUID,
    stage: str,
    check_name: str,
    status: str,
    details: str,
) -> None:
    """Records a data quality gate outcome into ops.dq_results."""
    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ops.dq_results (run_id, stage, check_name, status, details)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (run_id, stage, check_name, status, details),
                )
    except Exception as e:
        logger.error("Failed to write to ops.dq_results: %s", e)


def log_dq_results(
    run_id: uuid.UUID,
    results: list[dict[str, Any]],
    default_stage: str = "silver",
) -> None:
    """Records multiple data quality check outcomes into ops.dq_results."""
    if not results:
        return
    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                for r in results:
                    stage = r.get("stage", default_stage)
                    name = r.get("name", r.get("check_name", "unknown"))
                    status = (
                        "PASS"
                        if r.get("passed", True) and r.get("status", "PASS") == "PASS"
                        else r.get("status", "FAIL")
                    )
                    details = r.get("details", "")
                    cur.execute(
                        """
                        INSERT INTO ops.dq_results (run_id, stage, check_name, status, details)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (run_id, stage, name, status, details),
                    )
    except Exception as e:
        logger.error("Failed to write batch to ops.dq_results: %s", e)


def load_silver_partition(
    trade_date: date,
    silver_base_dir: Path | str = "data/silver/equity_prices",
    allow_fallback: bool = False,
    include_all_symbols: bool = False,
    conn: psycopg.Connection | None = None,
) -> dict[str, Any]:
    """Loads a single Silver date partition into PostgreSQL Gold Fact Table atomically.

    Returns:
        dict containing run execution metrics (run_id, rows_in, rows_out, status, duration_seconds)
    """
    run_id = uuid.uuid4()
    started_at = datetime.now(UTC)
    date_str = trade_date.strftime("%Y-%m-%d")
    partition_dir = Path(silver_base_dir) / f"trade_date={date_str}"

    logger.info("====================================================================")
    logger.info("Starting Warehouse Load for trade date: %s", date_str)
    logger.info("====================================================================")

    # 1. Validate partition metadata before attempting database operations
    try:
        meta = validate_partition_metadata(
            partition_dir,
            trade_date,
            allow_fallback=allow_fallback,
            include_all_symbols=include_all_symbols,
        )
        source_type = meta["source_type"]
        log_dq_result(
            run_id=run_id,
            stage="load",
            check_name="partition_metadata_validation",
            status="PASS",
            details=f"Validated _SUCCESS.meta.json (source_type={source_type}, read={meta['read_count']})",
        )

        # Record Silver quality check outcomes into ops.dq_results
        silver_checks = meta.get("checks", [])
        t_dq_start = time.time()
        if silver_checks:
            log_dq_results(run_id=run_id, results=silver_checks, default_stage="silver")
        t_dq_duration = time.time() - t_dq_start
    except Exception as e:
        finished_at = datetime.now(UTC)
        log_pipeline_run(
            run_id=run_id,
            stage="load",
            trade_date=trade_date,
            status="FAILED",
            rows_in=0,
            rows_out=0,
            error=str(e),
            started_at=started_at,
            finished_at=finished_at,
        )
        log_dq_result(
            run_id=run_id,
            stage="load",
            check_name="partition_metadata_validation",
            status="FAIL",
            details=str(e),
        )
        raise

    # 2. Read Parquet files via PyArrow
    t_read_start = time.time()
    table = pq.read_table(partition_dir)
    rows_in = table.num_rows
    if rows_in == 0:
        err = f"Parquet partition at {partition_dir} contains 0 rows."
        finished_at = datetime.now(UTC)
        log_pipeline_run(
            run_id=run_id,
            stage="load",
            trade_date=trade_date,
            status="FAILED",
            rows_in=0,
            rows_out=0,
            error=err,
            started_at=started_at,
            finished_at=finished_at,
        )
        raise ValueError(err)

    df = table.to_pandas()
    if "trade_date" not in df.columns or df["trade_date"].isnull().all():
        df["trade_date"] = trade_date

    # Prepare tuples for staging table COPY
    staging_rows = []
    for _, r in df.iterrows():
        staging_rows.append(
            (
                str(r["symbol"]).strip().upper(),
                str(r.get("series", "EQ")).strip().upper(),
                str(r["isin"]).strip()
                if pd.notnull(r.get("isin")) and str(r["isin"]).strip()
                else None,
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["last"]),
                float(r["prev_close"]),
                int(r["volume"]),
                float(r["turnover"]),
                int(r["trades"]) if pd.notnull(r.get("trades")) else None,
                str(r.get("source_format", "unknown")),
                str(r.get("source_file", "unknown")),
                trade_date,
            )
        )

    # 3. Execute atomic load in ONE transaction
    t_read_duration = time.time() - t_read_start
    should_close_conn = False
    active_conn = conn
    t_conn_start = time.time()
    if active_conn is None:
        active_conn = get_db_connection()
        should_close_conn = True
    t_conn_duration = time.time() - t_conn_start

    try:
        with active_conn.transaction():
            with active_conn.cursor() as cur:
                t_staging_start = time.time()
                # 3a. Create temporary staging table
                cur.execute(
                    """
                    CREATE TEMP TABLE staging_price (
                        symbol VARCHAR(30),
                        series VARCHAR(10),
                        isin VARCHAR(20),
                        open NUMERIC(18, 4),
                        high NUMERIC(18, 4),
                        low NUMERIC(18, 4),
                        close NUMERIC(18, 4),
                        last NUMERIC(18, 4),
                        prev_close NUMERIC(18, 4),
                        volume BIGINT,
                        turnover NUMERIC(22, 2),
                        trades BIGINT,
                        source_format VARCHAR(20),
                        source_file VARCHAR(150),
                        trade_date DATE
                    ) ON COMMIT DROP;
                    """
                )

                # 3b. High-performance COPY into staging table
                copy_sql = """
                    COPY staging_price (
                        symbol, series, isin, open, high, low, close, last, prev_close,
                        volume, turnover, trades, source_format, source_file, trade_date
                    ) FROM STDIN;
                """
                with cur.copy(copy_sql) as copy:
                    for row in staging_rows:
                        copy.write_row(row)
                t_copy_duration = time.time() - t_staging_start

                # 3c. Upsert any new/unseen symbols into gold.dim_symbol
                cur.execute(
                    """
                    INSERT INTO gold.dim_symbol (symbol, isin)
                    SELECT DISTINCT s.symbol, s.isin
                    FROM staging_price s
                    WHERE s.symbol IS NOT NULL
                    ON CONFLICT (symbol) DO UPDATE SET
                        isin = EXCLUDED.isin,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE (gold.dim_symbol.isin IS NULL OR gold.dim_symbol.isin = '')
                      AND EXCLUDED.isin IS NOT NULL AND EXCLUDED.isin != '';
                    """
                )

                # 3d. Atomic upsert into gold.fact_daily_price
                t_upsert_start = time.time()
                upsert_fact_sql = """
                    INSERT INTO gold.fact_daily_price (
                        symbol_id, date_id, open, high, low, close, last, prev_close,
                        volume, turnover, trades, source_format, source_type, loaded_at
                    )
                    SELECT
                        sym.symbol_id,
                        d.date_id,
                        s.open,
                        s.high,
                        s.low,
                        s.close,
                        s.last,
                        s.prev_close,
                        s.volume,
                        s.turnover,
                        s.trades,
                        s.source_format,
                        %(source_type)s,
                        CURRENT_TIMESTAMP
                    FROM staging_price s
                    JOIN gold.dim_symbol sym ON s.symbol = sym.symbol
                    JOIN gold.dim_date d ON s.trade_date = d.date
                    ON CONFLICT (symbol_id, date_id) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        last = EXCLUDED.last,
                        prev_close = EXCLUDED.prev_close,
                        volume = EXCLUDED.volume,
                        turnover = EXCLUDED.turnover,
                        trades = EXCLUDED.trades,
                        source_format = EXCLUDED.source_format,
                        source_type = EXCLUDED.source_type,
                        loaded_at = CURRENT_TIMESTAMP;
                """
                cur.execute(upsert_fact_sql, {"source_type": source_type})
                rows_out = cur.rowcount
                t_upsert_duration = time.time() - t_upsert_start

        finished_at = datetime.now(UTC)
        duration = round((finished_at - started_at).total_seconds(), 2)

        # 4. Log successful execution
        t_runlog_start = time.time()
        log_pipeline_run(
            run_id=run_id,
            stage="load",
            trade_date=trade_date,
            status="SUCCESS",
            rows_in=rows_in,
            rows_out=rows_out,
            error=None,
            started_at=started_at,
            finished_at=finished_at,
        )
        log_dq_result(
            run_id=run_id,
            stage="load",
            check_name="fact_upsert_check",
            status="PASS",
            details=f"Upserted {rows_out} rows into gold.fact_daily_price (rows_in={rows_in})",
        )
        t_runlog_duration = time.time() - t_runlog_start

        logger.info(
            "[PROFILE] Load breakdown for %s: conn=%.3fs, pyarrow_read=%.3fs, staging_copy=%.3fs, fact_upsert=%.3fs, dq_results=%.3fs, run_log=%.3fs",
            date_str,
            t_conn_duration,
            t_read_duration,
            t_copy_duration,
            t_upsert_duration,
            t_dq_duration,
            t_runlog_duration,
        )

        logger.info(
            "SUCCESS: Loaded %d rows into gold.fact_daily_price for trade date %s (duration: %.2fs)",
            rows_out,
            date_str,
            duration,
        )

        return {
            "run_id": str(run_id),
            "trade_date": trade_date,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "status": "SUCCESS",
            "duration_seconds": duration,
        }

    except Exception as e:
        finished_at = datetime.now(UTC)
        duration = round((finished_at - started_at).total_seconds(), 2)
        logger.error("Failed to load partition %s into warehouse: %s", date_str, e)
        log_pipeline_run(
            run_id=run_id,
            stage="load",
            trade_date=trade_date,
            status="FAILED",
            rows_in=rows_in,
            rows_out=0,
            error=str(e),
            started_at=started_at,
            finished_at=finished_at,
        )
        log_dq_result(
            run_id=run_id,
            stage="load",
            check_name="fact_upsert_check",
            status="FAIL",
            details=f"Transaction rolled back: {e}",
        )
        raise

    finally:
        if should_close_conn and active_conn:
            active_conn.close()
