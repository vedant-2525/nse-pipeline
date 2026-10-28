"""Offline unit tests for historical backfill orchestration logic."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from nse_pipeline.orchestration.backfill import (
    is_already_fully_done,
    run_backfill,
)


def test_is_already_fully_done_requires_all_three_layers(tmp_path: Path):
    """Verify is_already_fully_done returns True only when bronze, silver, and gold exist."""
    trade_date = date(2024, 10, 11)
    date_str = "2024-10-11"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    # Initially nothing exists
    assert is_already_fully_done(trade_date, bronze_dir, silver_dir) is False

    # 1. Create Bronze
    b_part = bronze_dir / f"trade_date={date_str}"
    b_part.mkdir(parents=True)
    raw_file = b_part / "bhavcopy.csv"
    raw_file.write_text("SYMBOL,OPEN\nRELIANCE,2500\n", encoding="utf-8")
    meta_bronze = b_part / "bhavcopy.csv.meta.json"
    meta_bronze.write_text(json.dumps({"source_name": "nse_official"}), encoding="utf-8")
    assert is_already_fully_done(trade_date, bronze_dir, silver_dir) is False

    # 2. Create Silver with default_list
    s_part = silver_dir / f"trade_date={date_str}"
    s_part.mkdir(parents=True)
    s_meta = {
        "trade_date": date_str,
        "universe": "default_list",
        "source_type": "official",
        "row_count": 51,
    }
    (s_part / "_SUCCESS.meta.json").write_text(json.dumps(s_meta), encoding="utf-8")
    assert is_already_fully_done(trade_date, bronze_dir, silver_dir) is False

    # 3. Mock PostgreSQL check returning row in gold
    with (
        patch("nse_pipeline.orchestration.backfill.is_postgres_reachable", return_value=True),
        patch("nse_pipeline.orchestration.backfill.get_db_connection") as mock_conn_fn,
    ):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (1,)
        mock_conn.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn_fn.return_value = mock_conn

        assert is_already_fully_done(trade_date, bronze_dir, silver_dir) is True


def test_backfill_skips_non_trading_days(tmp_path: Path):
    """Verify weekends and holidays are marked as skipped_non_trading_day."""
    # 2024-10-12 is Saturday, 2024-10-13 is Sunday
    from_date = date(2024, 10, 12)
    to_date = date(2024, 10, 13)

    exit_code, report = run_backfill(
        from_date=from_date,
        to_date=to_date,
        reports_dir=tmp_path / "reports",
        spark=MagicMock(),
    )

    assert exit_code == 0
    assert report["summary"]["total_calendar_days"] == 2
    assert report["summary"]["skipped_non_trading_day"] == 2
    assert report["summary"]["ok"] == 0
    assert report["summary"]["failed"] == 0


@patch("nse_pipeline.orchestration.backfill.is_already_fully_done", return_value=True)
def test_backfill_idempotent_skips_already_loaded(mock_done, tmp_path: Path):
    """Verify days already completed are marked as skipped_already_loaded."""
    # 2024-10-14 is Monday (trading day)
    from_date = date(2024, 10, 14)
    to_date = date(2024, 10, 14)

    exit_code, report = run_backfill(
        from_date=from_date,
        to_date=to_date,
        reports_dir=tmp_path / "reports",
        spark=MagicMock(),
    )

    assert exit_code == 0
    assert report["summary"]["ok"] == 0
    assert report["summary"]["skipped_already_loaded"] == 1
    assert report["days"][0]["status"] == "skipped_already_loaded"


@patch("nse_pipeline.orchestration.backfill.is_already_fully_done", return_value=False)
@patch("nse_pipeline.orchestration.backfill.load_silver_partition", return_value={"rows_out": 51})
@patch("nse_pipeline.orchestration.backfill.run_silver_transform", return_value={"kept_count": 51})
@patch("nse_pipeline.orchestration.backfill.BhavcopyDownloader")
def test_backfill_success_across_range(
    mock_downloader_cls, mock_transform, mock_load, mock_done, tmp_path: Path
):
    """Verify successful end-to-end backfill marks trading days ok and creates JSON report."""
    mock_downloader = MagicMock()
    mock_downloader_cls.return_value = mock_downloader

    # 2024-10-14 (Mon) to 2024-10-15 (Tue)
    from_date = date(2024, 10, 14)
    to_date = date(2024, 10, 15)
    rep_dir = tmp_path / "reports"

    exit_code, report = run_backfill(
        from_date=from_date,
        to_date=to_date,
        reports_dir=rep_dir,
        spark=MagicMock(),
    )

    assert exit_code == 0
    assert report["summary"]["total_calendar_days"] == 2
    assert report["summary"]["trading_days"] == 2
    assert report["summary"]["ok"] == 2
    assert report["summary"]["failed"] == 0

    # Verify JSON report was written to disk
    report_files = list(rep_dir.glob("backfill_report_*.json"))
    assert len(report_files) == 1
    saved_report = json.loads(report_files[0].read_text(encoding="utf-8"))
    assert saved_report["summary"]["ok"] == 2


@patch("nse_pipeline.orchestration.backfill.is_already_fully_done", return_value=False)
@patch("nse_pipeline.orchestration.backfill.BhavcopyDownloader")
def test_backfill_stops_immediately_on_rate_limit(
    mock_downloader_cls, mock_done, tmp_path: Path
):
    """Verify HTTP 429/430 halts execution and marks subsequent dates as not_attempted."""
    mock_downloader = MagicMock()
    # First day raises 429 rate limit
    mock_downloader.download.side_effect = PermissionError("HTTP 429: Too Many Requests from NSE")
    mock_downloader_cls.return_value = mock_downloader

    # 2024-10-14 (Mon) to 2024-10-16 (Wed) - 3 trading days
    from_date = date(2024, 10, 14)
    to_date = date(2024, 10, 16)

    exit_code, report = run_backfill(
        from_date=from_date,
        to_date=to_date,
        reports_dir=tmp_path / "reports",
        spark=MagicMock(),
    )

    assert exit_code == 1
    assert report["summary"]["failed"] == 1
    assert report["summary"]["not_attempted"] == 2
    assert report["days"][0]["status"] == "failed"
    assert report["days"][0]["step"] == "ingest"
    assert "429" in report["days"][0]["reason"]
    assert report["days"][1]["status"] == "not_attempted"
    assert report["days"][2]["status"] == "not_attempted"


@patch("nse_pipeline.orchestration.backfill.is_already_fully_done", return_value=False)
@patch("nse_pipeline.orchestration.backfill.load_silver_partition", return_value={"rows_out": 51})
@patch("nse_pipeline.orchestration.backfill.run_silver_transform", return_value={"kept_count": 51})
@patch("nse_pipeline.orchestration.backfill.BhavcopyDownloader")
def test_backfill_continues_past_non_rate_limit_failure(
    mock_downloader_cls, mock_transform, mock_load, mock_done, tmp_path: Path
):
    """Verify non-rate-limit failures (e.g. missing official file) record failure and continue."""
    mock_downloader = MagicMock()
    # First day has no official file, second day succeeds
    mock_downloader.download.side_effect = [
        FileNotFoundError("No bhavcopy found"),
        Path("data/bronze/file.csv"),
    ]
    mock_downloader_cls.return_value = mock_downloader

    # 2024-10-14 (Mon) to 2024-10-15 (Tue)
    from_date = date(2024, 10, 14)
    to_date = date(2024, 10, 15)

    exit_code, report = run_backfill(
        from_date=from_date,
        to_date=to_date,
        reports_dir=tmp_path / "reports",
        spark=MagicMock(),
    )

    assert exit_code == 1  # Exits non-zero because 1 day failed
    assert report["summary"]["failed"] == 1
    assert report["summary"]["ok"] == 1
    assert report["summary"]["not_attempted"] == 0
    assert report["days"][0]["status"] == "failed"
    assert report["days"][0]["reason"] == "no official file"
    assert report["days"][1]["status"] == "ok"
