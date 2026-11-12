"""Offline unit tests for daily command and cutoff-based date selection."""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from nse_pipeline.orchestration.daily import (
    resolve_default_date,
    run_daily_pipeline,
)


def test_resolve_default_date_before_cutoff():
    """Verify that before 19:00 IST on a weekday, the previous trading day is selected."""
    # Wednesday at 14:30 IST -> should return Tuesday 2024-10-15
    ist_tz = ZoneInfo("Asia/Kolkata")
    dt = datetime(2024, 10, 16, 14, 30, tzinfo=ist_tz)
    resolved = resolve_default_date(now=dt, cutoff_hour=19, cutoff_minute=0)
    assert resolved == date(2024, 10, 15)


def test_resolve_default_date_after_cutoff():
    """Verify that at or after 19:00 IST on a weekday, today is selected."""
    # Wednesday at 19:15 IST -> should return Wednesday 2024-10-16
    ist_tz = ZoneInfo("Asia/Kolkata")
    dt = datetime(2024, 10, 16, 19, 15, tzinfo=ist_tz)
    resolved = resolve_default_date(now=dt, cutoff_hour=19, cutoff_minute=0)
    assert resolved == date(2024, 10, 16)


def test_resolve_default_date_on_weekend():
    """Verify that on Saturday or Sunday, the previous Friday trading day is selected."""
    # Sunday at 20:00 IST -> should return Friday 2024-10-18
    ist_tz = ZoneInfo("Asia/Kolkata")
    dt = datetime(2024, 10, 20, 20, 0, tzinfo=ist_tz)  # Sunday
    resolved = resolve_default_date(now=dt, cutoff_hour=19, cutoff_minute=0)
    assert resolved == date(2024, 10, 18)


def test_resolve_default_date_on_monday_before_cutoff():
    """Verify that on Monday morning before cutoff, Friday is selected."""
    ist_tz = ZoneInfo("Asia/Kolkata")
    dt = datetime(2024, 10, 21, 10, 0, tzinfo=ist_tz)  # Monday morning
    resolved = resolve_default_date(now=dt, cutoff_hour=19, cutoff_minute=0)
    assert resolved == date(2024, 10, 18)


def test_daily_pipeline_skips_non_trading_day(tmp_path: Path):
    """Verify that passing a weekend/holiday date skips cleanly with code 0."""
    sunday = date(2024, 10, 20)
    exit_code, report = run_daily_pipeline(
        target_date=sunday,
        reports_dir=tmp_path / "reports",
    )
    assert exit_code == 0
    assert report["status"] == "skipped_non_trading_day"


@patch("nse_pipeline.orchestration.daily.is_already_fully_done", return_value=True)
def test_daily_pipeline_skips_when_already_done(mock_done, tmp_path: Path):
    """Verify that an already processed date skips with exit code 0."""
    t_date = date(2024, 10, 15)
    exit_code, report = run_daily_pipeline(
        target_date=t_date,
        force=False,
        reports_dir=tmp_path / "reports",
    )
    assert exit_code == 0
    assert report["status"] == "skipped_already_loaded"


@patch("nse_pipeline.orchestration.daily.is_already_fully_done", return_value=False)
@patch("nse_pipeline.orchestration.daily.run_backfill")
def test_daily_pipeline_executes_backfill_for_single_day(mock_backfill, mock_done, tmp_path: Path):
    """Verify that daily pipeline calls run_backfill with from_date == to_date."""
    mock_backfill.return_value = (
        0,
        {"days": [{"status": "ok", "reason": "Rows loaded: 51"}]},
    )
    t_date = date(2024, 10, 15)

    exit_code, report = run_daily_pipeline(
        target_date=t_date,
        force=False,
        reports_dir=tmp_path / "reports",
    )
    assert exit_code == 0
    mock_backfill.assert_called_once()
    assert mock_backfill.call_args.kwargs["from_date"] == t_date
    assert mock_backfill.call_args.kwargs["to_date"] == t_date


@patch("nse_pipeline.orchestration.daily.is_already_fully_done", return_value=False)
@patch("nse_pipeline.orchestration.daily.run_backfill")
def test_daily_pipeline_returns_nonzero_on_failure(mock_backfill, mock_done, tmp_path: Path):
    """Verify that failure in run_backfill propagates as non-zero exit code."""
    mock_backfill.return_value = (
        1,
        {"days": [{"status": "failed", "reason": "HTTP 429 Rate Limit"}]},
    )
    t_date = date(2024, 10, 15)

    exit_code, report = run_daily_pipeline(
        target_date=t_date,
        force=False,
        reports_dir=tmp_path / "reports",
    )
    assert exit_code == 1
