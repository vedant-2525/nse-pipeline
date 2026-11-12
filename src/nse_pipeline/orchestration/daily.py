"""Daily batch pipeline orchestrator for the NSE Market Data Pipeline.

Runs a single trading day end-to-end:
Preflight -> Ingest (NSE official only) -> Transform (Silver) -> Load (Gold).
Reuses historical backfill orchestration logic for guaranteed consistency and idempotency.
"""

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from nse_pipeline.orchestration.backfill import is_already_fully_done, run_backfill
from nse_pipeline.utils.calendar import (
    check_holiday_coverage,
    get_previous_trading_day,
    is_trading_day,
)
from nse_pipeline.utils.logging import logger

DEFAULT_CUTOFF_HOUR = 19
DEFAULT_CUTOFF_MINUTE = 0
DEFAULT_TIMEZONE = "Asia/Kolkata"


def resolve_default_date(
    now: datetime | None = None,
    cutoff_hour: int = DEFAULT_CUTOFF_HOUR,
    cutoff_minute: int = DEFAULT_CUTOFF_MINUTE,
    tz_name: str = DEFAULT_TIMEZONE,
    holidays_path: str = "config/holidays.csv",
) -> date:
    """Computes the target trading date when no explicit --date is provided.

    NSE Bhavcopy files are typically published after ~18:30 IST on trading days.
    - If current IST time is before cutoff (default 19:00 IST):
      The current trading day's file is not yet available, so return the PREVIOUS trading day.
    - If current IST time is at or after cutoff:
      If today is an active trading day, return today.
      If today is a weekend or holiday, return the most recent previous trading day.
    """
    if now is None:
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)
    elif now.tzinfo is None:
        tz = ZoneInfo(tz_name)
        now = now.replace(tzinfo=tz)

    today = now.date()

    # Before cutoff: today's file is not guaranteed to exist yet
    if (now.hour, now.minute) < (cutoff_hour, cutoff_minute):
        return get_previous_trading_day(today, holidays_path=holidays_path)

    # At or after cutoff
    if is_trading_day(today, holidays_path=holidays_path):
        return today

    return get_previous_trading_day(today, holidays_path=holidays_path)


def run_daily_pipeline(
    target_date: date | None = None,
    cutoff_hour: int = DEFAULT_CUTOFF_HOUR,
    cutoff_minute: int = DEFAULT_CUTOFF_MINUTE,
    force: bool = False,
    bronze_dir: Path | str = "data/bronze",
    silver_dir: Path | str = "data/silver/equity_prices",
    quarantine_dir: Path | str = "data/quarantine/equity_prices",
    reports_dir: Path | str = "data/reports",
    symbols_config: str = "config/symbols.yaml",
    holidays_path: str = "config/holidays.csv",
) -> tuple[int, dict[str, Any]]:
    """Executes the daily pipeline for a single trading day.

    Returns:
        tuple (exit_code, report_dict)
    """
    # 1. Resolve date if not supplied
    if target_date is None:
        target_date = resolve_default_date(
            cutoff_hour=cutoff_hour,
            cutoff_minute=cutoff_minute,
            holidays_path=holidays_path,
        )

    # 2. Check holiday data coverage warning for the date's year
    check_holiday_coverage(target_date.year, holidays_path=holidays_path)

    date_str = target_date.strftime("%Y-%m-%d")
    logger.info("Daily pipeline invoked for date: %s (force=%s)", date_str, force)

    # 3. Non-trading day guard
    if not is_trading_day(target_date, holidays_path=holidays_path):
        logger.info("Daily run skipped: %s is not an active NSE trading day.", date_str)
        print(f"Skipped: {date_str} is not an official NSE trading day (weekend/holiday).")
        report = {
            "date": date_str,
            "status": "skipped_non_trading_day",
            "reason": "Non-trading day (weekend or NSE holiday)",
            "rows_loaded": 0,
        }
        return 0, report

    # 4. Check if already fully done
    b_dir = Path(bronze_dir)
    s_dir = Path(silver_dir)
    if not force and is_already_fully_done(target_date, b_dir, s_dir):
        logger.info("Daily run skipped: %s is already fully loaded in warehouse.", date_str)
        print(f"Already done: {date_str} has already been ingested, transformed, and loaded.")
        print(f"DAILY SUMMARY: date={date_str} status=SKIPPED_ALREADY_DONE rows=0")
        report = {
            "date": date_str,
            "status": "skipped_already_loaded",
            "reason": "Already fully ingested, transformed, and loaded in warehouse",
            "rows_loaded": 0,
        }
        return 0, report

    # 5. Reuse backfill orchestration for the single day (from_date == to_date)
    exit_code, bf_report = run_backfill(
        from_date=target_date,
        to_date=target_date,
        force=force,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        reports_dir=reports_dir,
        symbols_config=symbols_config,
        holidays_path=holidays_path,
    )

    day_info = bf_report["days"][0] if bf_report.get("days") else {}
    status = day_info.get("status", "unknown").upper()
    reason = day_info.get("reason", "")

    # Clean one-line summary
    print(f"DAILY SUMMARY: date={date_str} status={status} reason='{reason}' exit_code={exit_code}")

    return exit_code, bf_report
