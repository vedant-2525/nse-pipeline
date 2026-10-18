"""Unit tests for Bhavcopy ingestion, sources, and downloader.

All tests run completely offline using fixtures and mocked responses,
adhering to the rule that unit tests never rely on external network calls.
"""

import io
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from nse_pipeline.ingest.base import BhavcopySource
from nse_pipeline.ingest.downloader import BhavcopyDownloader
from nse_pipeline.ingest.nse_source import NseSource
from nse_pipeline.ingest.yfinance_source import YFinanceSource


def test_bhavcopy_source_subclass():
    """Verify concrete sources properly inherit from BhavcopySource ABC."""
    assert issubclass(NseSource, BhavcopySource)
    assert issubclass(YFinanceSource, BhavcopySource)


def test_nse_source_candidate_urls():
    """Verify URL generator selects modern UDiFF format vs legacy format based on transition date."""
    nse = NseSource()

    # Modern date: post July 8, 2024
    modern_date = date(2024, 10, 11)
    modern_urls = nse.get_candidate_urls(modern_date)
    assert any("BhavCopy_NSE_CM_0_0_0_20241011_F_0000.csv.zip" in u for u in modern_urls)

    # Legacy date: pre July 8, 2024
    legacy_date = date(2024, 1, 15)
    legacy_urls = nse.get_candidate_urls(legacy_date)
    assert any("cm15JAN2024bhav.csv.zip" in u for u in legacy_urls)


def test_nse_source_fetch_zip_success(tmp_path: Path):
    """Verify NseSource extracts CSV from a downloaded zip archive."""
    # Create an in-memory zip archive using the real fixture sample
    fixture_csv = Path("tests/fixtures/bhavcopy_new_sample.csv")
    csv_bytes = fixture_csv.read_bytes()

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("BhavCopy_NSE_CM_0_0_0_20241011_F_0000.csv", csv_bytes)

    # Mock session
    mock_session = MagicMock()
    mock_session.headers = {}
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.content = zip_buffer.getvalue()
    mock_session.get.return_value = mock_response

    nse = NseSource(session=mock_session)
    dest_dir = tmp_path / "bronze"

    saved_file = nse.fetch(date(2024, 10, 11), dest_dir)
    assert saved_file.is_file()
    assert saved_file.name == "BhavCopy_NSE_CM_0_0_0_20241011_F_0000.csv"
    assert saved_file.stat().st_size > 0


def test_nse_source_fetch_404_raises_filenotfound(tmp_path: Path):
    """Verify HTTP 404 (file not found/holiday) raises FileNotFoundError."""
    mock_session = MagicMock()
    mock_session.headers = {}
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 404
    mock_response.content = b""
    mock_session.get.return_value = mock_response

    nse = NseSource(session=mock_session)
    dest_dir = tmp_path / "bronze"

    with pytest.raises(FileNotFoundError, match="NSE Bhavcopy not found"):
        nse.fetch(date(2024, 10, 12), dest_dir)


def test_nse_source_fetch_429_raises_permission_error(tmp_path: Path):
    """Verify HTTP 429 / 403 (rate limiting) raises PermissionError immediately."""
    mock_session = MagicMock()
    mock_session.headers = {}
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 429
    mock_session.get.return_value = mock_response

    nse = NseSource(session=mock_session)
    dest_dir = tmp_path / "bronze"

    with pytest.raises(PermissionError, match="blocked access"):
        nse.fetch(date(2024, 10, 11), dest_dir)


def test_yfinance_source_fetch_mocked(tmp_path: Path):
    """Verify YFinanceSource parses history data and writes canonical CSV."""
    symbols_file = tmp_path / "symbols.yaml"
    symbols_file.write_text("symbols:\n  - RELIANCE\n  - TCS\n", encoding="utf-8")

    yf_source = YFinanceSource(symbols_path=symbols_file)
    dest_dir = tmp_path / "bronze"

    # Mock yf.Ticker
    mock_df = pd.DataFrame(
        {
            "Open": [2500.0],
            "High": [2550.0],
            "Low": [2490.0],
            "Close": [2540.0],
            "Volume": [1000000],
        }
    )

    with patch("yfinance.Ticker") as mock_ticker_cls:
        mock_instance = MagicMock()
        mock_instance.history.return_value = mock_df
        mock_ticker_cls.return_value = mock_instance

        saved_file = yf_source.fetch(date(2024, 10, 11), dest_dir)

    assert saved_file.is_file()
    assert saved_file.name == "yfinance_fallback_20241011.csv"

    # Check content of generated CSV
    df = pd.read_csv(saved_file)
    assert len(df) == 2
    assert set(df["SYMBOL"]) == {"RELIANCE", "TCS"}
    assert "CLOSE" in df.columns
    assert "OPEN" in df.columns


def test_downloader_idempotent(tmp_path: Path):
    """Verify downloader returns existing file from bronze without triggering fetch."""
    bronze_dir = tmp_path / "bronze"
    partition_dir = bronze_dir / "trade_date=2024-10-11"
    partition_dir.mkdir(parents=True)
    existing_file = partition_dir / "test_bhav.csv"
    existing_file.write_text("SYMBOL,CLOSE\nRELIANCE,2500\n", encoding="utf-8")

    mock_primary = MagicMock(spec=BhavcopySource)
    downloader = BhavcopyDownloader(
        output_base_dir=bronze_dir,
        primary_source=mock_primary,
        delay_seconds=0.0,
    )

    result = downloader.download(date(2024, 10, 11), force=False)
    assert result == existing_file
    # Primary source fetch must NOT have been called
    mock_primary.fetch.assert_not_called()


def test_downloader_weekend_rejection(tmp_path: Path):
    """Verify downloader rejects non-trading days unless force=True."""
    bronze_dir = tmp_path / "bronze"
    downloader = BhavcopyDownloader(
        output_base_dir=bronze_dir,
        holidays_path="config/holidays.csv",
        delay_seconds=0.0,
    )

    sunday = date(2024, 10, 13)
    with pytest.raises(ValueError, match="not an official NSE trading day"):
        downloader.download(sunday, force=False)


def test_downloader_triggers_fallback_on_primary_failure(tmp_path: Path):
    """Verify downloader seamlessly activates fallback when primary source fails with FileNotFoundError."""
    bronze_dir = tmp_path / "bronze"
    trade_date = date(2024, 10, 11)  # A valid trading Friday

    mock_primary = MagicMock(spec=BhavcopySource)
    mock_primary.source_name = "nse_official"
    mock_primary.fetch.side_effect = FileNotFoundError("Bhavcopy not found on server")

    mock_fallback = MagicMock(spec=BhavcopySource)
    mock_fallback.source_name = "yfinance_fallback"

    def fake_fallback_fetch(dt, output_dir):
        f = output_dir / "fallback.csv"
        f.write_text("SYMBOL,CLOSE\n", encoding="utf-8")
        return f

    mock_fallback.fetch.side_effect = fake_fallback_fetch

    downloader = BhavcopyDownloader(
        output_base_dir=bronze_dir,
        primary_source=mock_primary,
        fallback_source=mock_fallback,
        delay_seconds=0.0,
        max_retries=2,
    )

    result = downloader.download(trade_date)
    assert result.name == "fallback.csv"
    assert result.is_file()
    mock_primary.fetch.assert_called_once()
    mock_fallback.fetch.assert_called_once()


def test_downloader_stops_immediately_on_rate_limit(tmp_path: Path):
    """Verify downloader does not repeatedly hammer the server when 403/429 occurs."""
    bronze_dir = tmp_path / "bronze"
    trade_date = date(2024, 10, 11)

    mock_primary = MagicMock(spec=BhavcopySource)
    mock_primary.source_name = "nse_official"
    mock_primary.fetch.side_effect = PermissionError("HTTP 403 Forbidden")

    mock_fallback = MagicMock(spec=BhavcopySource)
    mock_fallback.source_name = "yfinance_fallback"

    def fake_fallback_fetch(dt, output_dir):
        f = output_dir / "fallback.csv"
        f.write_text("SYMBOL,CLOSE\n", encoding="utf-8")
        return f

    mock_fallback.fetch.side_effect = fake_fallback_fetch

    downloader = BhavcopyDownloader(
        output_base_dir=bronze_dir,
        primary_source=mock_primary,
        fallback_source=mock_fallback,
        delay_seconds=0.0,
        max_retries=5,
    )

    result = downloader.download(trade_date)
    assert result.name == "fallback.csv"
    assert result.is_file()
    # Verify primary fetch was called only ONCE and did not loop through all 5 retries
    assert mock_primary.fetch.call_count == 1
