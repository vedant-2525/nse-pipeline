"""Historical backfill orchestrator for the NSE Market Data Pipeline.

Why this design:
1. Unified Pipeline Execution: Orchestrates Ingest -> Transform -> Load across a date range.
2. Single Spark Session Reuse: Minimizes JVM startup overhead by maintaining a shared Spark session.
3. Strict Governance & Provenance: Rejects fallback sources (yfinance) completely during backfills.
4. Fail-Fast on Rate Limits: Halts immediately upon receiving HTTP 403/429 to protect exchange access,
   marking all subsequent days as 'not_attempted'.
5. Fault Isolation: Non-rate-limit errors (e.g. missing daily archive) record a per-day failure
   and allow processing of remaining dates.
6. Complete Idempotency & Resumability: Inspects Bronze, Silver, and Gold warehouse states before
   running, safely skipping already processed days.
7. Audit Reporting: Generates structured JSON backfill reports and prints a clean console summary.
"""

import json
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession

from nse_pipeline.ingest.downloader import BhavcopyDownloader
from nse_pipeline.ingest.metadata import read_file_metadata
from nse_pipeline.ingest.nse_source import NseSource
from nse_pipeline.load.db import get_db_connection, is_postgres_reachable
from nse_pipeline.load.loader import load_silver_partition
from nse_pipeline.transform.run_silver import run_silver_transform
from nse_pipeline.utils.calendar import (
    check_holiday_coverage,
    is_trading_day,
    is_weekend,
)
from nse_pipeline.utils.logging import logger
from nse_pipeline.utils.spark import get_spark_session, stop_spark_session


def load_universe_symbols(symbols_path: str | Path = "config/symbols.yaml") -> list[str]:
    """Loads configured equity symbols for the target universe (e.g. NIFTY 50)."""
    p = Path(symbols_path)
    if p.is_file():
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get("symbols", [])
    return []


def is_already_fully_done(
    trade_date: date,
    bronze_dir: Path,
    silver_dir: Path,
) -> bool:
    """Checks whether a trade date has already been completely ingested, transformed, and loaded.

    Conditions for skipping:
    1. Official Bronze raw file exists in data/bronze/trade_date=YYYY-MM-DD/
    2. Valid Silver Parquet partition exists with _SUCCESS.meta.json having universe='default_list'
    3. Gold warehouse fact_daily_price contains rows for date_id = YYYYMMDD
    """
    date_str = trade_date.strftime("%Y-%m-%d")
    date_id = int(trade_date.strftime("%Y%m%d"))

    # 1. Bronze check
    bronze_partition = bronze_dir / f"trade_date={date_str}"
    if not bronze_partition.is_dir():
        return False
    csv_files = [f for f in bronze_partition.glob("*.csv") if f.stat().st_size > 0]
    if not csv_files:
        return False
    has_official = False
    for f in csv_files:
        meta = read_file_metadata(f)
        src_name = meta.get("source_name", "") if meta else ""
        if "yfinance" not in src_name and "fallback" not in f.name.lower():
            has_official = True
            break
    if not has_official:
        return False

    # 2. Silver check
    silver_partition = silver_dir / f"trade_date={date_str}"
    meta_file = silver_partition / "_SUCCESS.meta.json"
    if not meta_file.is_file():
        return False
    try:
        with open(meta_file, encoding="utf-8") as mf:
            silver_meta = json.load(mf)
        if silver_meta.get("universe") != "default_list":
            return False
        if silver_meta.get("source_type") != "official":
            return False
        if silver_meta.get("row_count", 0) <= 0:
            return False
    except Exception:
        return False

    # 3. Gold warehouse check
    if not is_postgres_reachable():
        return False

    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM gold.fact_daily_price WHERE date_id = %s LIMIT 1;",
                    (date_id,),
                )
                row = cur.fetchone()
                if not row or row[0] != 1:
                    return False
    except Exception:
        return False

    return True


def run_backfill(
    from_date: date,
    to_date: date,
    force: bool = False,
    bronze_dir: Path | str = "data/bronze",
    silver_dir: Path | str = "data/silver/equity_prices",
    quarantine_dir: Path | str = "data/quarantine/equity_prices",
    reports_dir: Path | str = "data/reports",
    symbols_config: str = "config/symbols.yaml",
    holidays_path: str = "config/holidays.csv",
    spark: SparkSession | None = None,
) -> tuple[int, dict[str, Any]]:
    """Executes an end-to-end backfill across a date range.

    Returns:
        tuple (exit_code, report_dict)
        exit_code is 0 if all trading days succeeded or were cleanly skipped; 1 if any failed.
    """
    if from_date > to_date:
        raise ValueError(f"--from date ({from_date}) cannot be later than --to date ({to_date}).")

    start_time = time.time()
    started_at = datetime.now(UTC)

    b_dir = Path(bronze_dir)
    s_dir = Path(silver_dir)
    q_dir = Path(quarantine_dir)
    rep_dir = Path(reports_dir)
    rep_dir.mkdir(parents=True, exist_ok=True)

    symbols = load_universe_symbols(symbols_config)

    # Check holiday coverage warning for all years in range
    years_in_range = sorted({from_date.year, to_date.year})
    check_holiday_coverage(years_in_range, holidays_path=holidays_path)

    # Enumerate all calendar days in range
    calendar_dates: list[date] = []
    curr = from_date
    while curr <= to_date:
        calendar_dates.append(curr)
        curr += timedelta(days=1)

    # 0. Preflight check: Verify PostgreSQL warehouse is reachable before starting execution
    if not is_postgres_reachable():
        err_msg = (
            "Error: PostgreSQL warehouse is not reachable.\n"
            "Action required: Ensure Docker Desktop is running and start the container:\n"
            "  docker compose up -d\n"
            "Then re-run: python -m nse_pipeline.cli db init"
        )
        print(err_msg, file=sys.stderr)
        logger.error("Backfill preflight failed: PostgreSQL warehouse is not reachable.")
        # Mark all dates in range as not_attempted in audit report
        preflight_results = [
            {
                "date": d.strftime("%Y-%m-%d"),
                "is_trading_day": is_trading_day(d, holidays_path),
                "status": "not_attempted",
                "step": None,
                "reason": "PostgreSQL warehouse is not reachable (preflight failure)",
                "duration_seconds": 0.0,
            }
            for d in calendar_dates
        ]
        finished_at = datetime.now(UTC)
        total_duration = round(time.time() - start_time, 2)
        total_days = len(calendar_dates)
        trading_days = sum(1 for d in preflight_results if d.get("is_trading_day", True))
        report = {
            "from_date": from_date.strftime("%Y-%m-%d"),
            "to_date": to_date.strftime("%Y-%m-%d"),
            "force": force,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": total_duration,
            "summary": {
                "total_calendar_days": total_days,
                "trading_days": trading_days,
                "ok": 0,
                "skipped_already_loaded": 0,
                "skipped_non_trading_day": 0,
                "failed": 0,
                "not_attempted": total_days,
            },
            "days": preflight_results,
        }
        report_filename = f"backfill_report_{started_at.strftime('%Y%m%d_%H%M%S')}.json"
        report_path = rep_dir / report_filename
        with open(report_path, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)
        return 1, report

    # Manage shared Spark session across dates
    spark_session = spark or get_spark_session(app_name="nse-pipeline-backfill")
    should_stop_spark = spark is None

    day_results: list[dict[str, Any]] = []
    rate_limited = False
    rate_limit_reason = ""

    print("\n" + "=" * 80)
    print(f"Starting Historical Backfill: {from_date} to {to_date} (force={force})")
    print("=" * 80 + "\n")

    try:
        downloader = BhavcopyDownloader(
            output_base_dir=b_dir,
            primary_source=NseSource(),
            fallback_source=None,  # NEVER permit yfinance fallback during backfill
        )

        for _idx, dt in enumerate(calendar_dates):
            date_str = dt.strftime("%Y-%m-%d")

            # Check if previous date triggered a rate-limit or systemic failure halt
            if rate_limited:
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": is_trading_day(dt, holidays_path),
                        "status": "not_attempted",
                        "step": None,
                        "reason": f"Halted after failure on previous date: {rate_limit_reason}",
                        "duration_seconds": 0.0,
                    }
                )
                continue

            # 1. Non-trading day check (weekend / holiday)
            if not is_trading_day(dt, holidays_path):
                reason = "Weekend" if is_weekend(dt) else "NSE Holiday"
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": False,
                        "status": "skipped_non_trading_day",
                        "step": None,
                        "reason": reason,
                        "duration_seconds": 0.0,
                    }
                )
                logger.info("Skipped non-trading day %s (%s)", date_str, reason)
                continue

            # 2. Already completed idempotency check
            if not force and is_already_fully_done(dt, b_dir, s_dir):
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "skipped_already_loaded",
                        "step": None,
                        "reason": "Already fully ingested, transformed (default_list), and loaded",
                        "duration_seconds": 0.0,
                    }
                )
                logger.info("Skipped %s: already fully loaded with default universe.", date_str)
                continue

            # 3. Execute Day Pipeline: Ingest -> Transform -> Load
            day_start = time.time()
            logger.info("Processing trading day: %s", date_str)

            # Step A: Ingest
            try:
                downloader.download(trade_date=dt, force=force, use_fallback=False)
            except PermissionError as pe:
                err_msg = str(pe)
                if "403" in err_msg or "429" in err_msg or "rate limit" in err_msg.lower():
                    logger.error(
                        "RATE LIMIT DETECTED on %s: %s. Halting backfill immediately.", date_str, pe
                    )
                    rate_limited = True
                    rate_limit_reason = f"HTTP 403/429 Rate Limit: {pe}"
                    day_results.append(
                        {
                            "date": date_str,
                            "is_trading_day": True,
                            "status": "failed",
                            "step": "ingest",
                            "reason": rate_limit_reason,
                            "duration_seconds": round(time.time() - day_start, 2),
                        }
                    )
                    continue
                else:
                    day_results.append(
                        {
                            "date": date_str,
                            "is_trading_day": True,
                            "status": "failed",
                            "step": "ingest",
                            "reason": str(pe),
                            "duration_seconds": round(time.time() - day_start, 2),
                        }
                    )
                    continue
            except FileNotFoundError:
                msg = "no official file"
                logger.warning("No official bhavcopy archive available from NSE for %s", date_str)
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "failed",
                        "step": "ingest",
                        "reason": msg,
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
                continue
            except Exception as e:
                logger.error("Ingest failed for %s: %s", date_str, e)
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "failed",
                        "step": "ingest",
                        "reason": str(e),
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
                continue

            # Step B: Transform (Silver)
            try:
                run_silver_transform(
                    trade_date=dt,
                    bronze_dir=b_dir,
                    silver_dir=s_dir,
                    quarantine_dir=q_dir,
                    symbol_universe=symbols,
                    allow_fallback=False,
                    spark=spark_session,
                )
            except Exception as e:
                logger.error("Transform failed for %s: %s", date_str, e)
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "failed",
                        "step": "transform",
                        "reason": str(e),
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
                continue

            # Step C: Load (Gold)
            # Before load, check database reachability to fail fast on DB outage
            if not is_postgres_reachable():
                logger.error(
                    "DATABASE OUTAGE DETECTED on %s: PostgreSQL warehouse is not reachable. Halting backfill immediately.",
                    date_str,
                )
                rate_limited = True
                rate_limit_reason = "PostgreSQL warehouse became unreachable (systemic outage)"
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "failed",
                        "step": "load",
                        "reason": rate_limit_reason,
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
                continue

            try:
                load_metrics = load_silver_partition(
                    trade_date=dt,
                    silver_base_dir=s_dir,
                    allow_fallback=False,
                    include_all_symbols=False,
                )
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "ok",
                        "step": None,
                        "reason": f"Rows loaded: {load_metrics['rows_out']}",
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
            except Exception as e:
                err_str = str(e).lower()
                is_connection_error = (
                    any(
                        x in err_str
                        for x in [
                            "connection",
                            "timeout",
                            "not reachable",
                            "could not connect",
                            "failed to connect",
                            "refused",
                        ]
                    )
                    or not is_postgres_reachable()
                )

                if is_connection_error:
                    logger.error(
                        "DATABASE OUTAGE DETECTED during load for %s: %s. Halting backfill immediately.",
                        date_str,
                        e,
                    )
                    rate_limited = True
                    rate_limit_reason = f"PostgreSQL connection failure: {e}"

                logger.error("Load failed for %s: %s", date_str, e)
                day_results.append(
                    {
                        "date": date_str,
                        "is_trading_day": True,
                        "status": "failed",
                        "step": "load",
                        "reason": str(e),
                        "duration_seconds": round(time.time() - day_start, 2),
                    }
                )
                continue

    finally:
        if should_stop_spark:
            stop_spark_session()

    finished_at = datetime.now(UTC)
    total_duration = round(time.time() - start_time, 2)

    # Compute Summary
    total_days = len(calendar_dates)
    trading_days = sum(1 for d in day_results if d.get("is_trading_day", True))
    ok_count = sum(1 for d in day_results if d["status"] == "ok")
    skipped_loaded = sum(1 for d in day_results if d["status"] == "skipped_already_loaded")
    skipped_non_trading = sum(1 for d in day_results if d["status"] == "skipped_non_trading_day")
    failed_count = sum(1 for d in day_results if d["status"] == "failed")
    not_attempted_count = sum(1 for d in day_results if d["status"] == "not_attempted")

    summary = {
        "total_calendar_days": total_days,
        "trading_days": trading_days,
        "ok": ok_count,
        "skipped_already_loaded": skipped_loaded,
        "skipped_non_trading_day": skipped_non_trading,
        "failed": failed_count,
        "not_attempted": not_attempted_count,
    }

    report = {
        "from_date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d"),
        "force": force,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": total_duration,
        "summary": summary,
        "days": day_results,
    }

    # Save JSON report
    report_filename = f"backfill_report_{started_at.strftime('%Y%m%d_%H%M%S')}.json"
    report_path = rep_dir / report_filename
    with open(report_path, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2)

    # Print Summary Table
    print("\n" + "=" * 90)
    print(f"BACKFILL SUMMARY TABLE ({from_date} to {to_date})")
    print("=" * 90)
    print(f"{'Date':<12} | {'Day Type':<16} | {'Status':<24} | {'Step':<10} | {'Details / Reason'}")
    print("-" * 90)
    for r in day_results:
        day_type = "Trading Day" if r.get("is_trading_day", True) else "Non-Trading"
        step = r.get("step") or "-"
        reason = r.get("reason") or ""
        print(f"{r['date']:<12} | {day_type:<16} | {r['status']:<24} | {step:<10} | {reason}")
    print("-" * 90)
    print(
        f"Total Days: {total_days} | Trading: {trading_days} | OK: {ok_count} | "
        f"Skipped (Done): {skipped_loaded} | Skipped (Off): {skipped_non_trading} | "
        f"Failed: {failed_count} | Not Attempted: {not_attempted_count}"
    )
    print(f"Execution Duration: {total_duration}s | Audit Report: {report_path}")
    print("=" * 90 + "\n")

    exit_code = 0 if (failed_count == 0 and not_attempted_count == 0) else 1
    return exit_code, report
