"""Bhavcopy Downloader orchestrating retries, polite delays, and bronze lake storage.

Why this design:
Automated scrapers can harm public servers or trigger IP bans if they behave aggressively.
This module enforces:
1. Polite throttling (1-3 second mandatory pauses between requests).
2. Idempotent storage: if a date partition already exists in Bronze, it avoids re-downloading.
3. Exponential backoff for transient network issues.
4. Immediate abort on rate-limiting (HTTP 403/429) to avoid worsening server penalties.
5. Automatic fallback to secondary provider (yfinance) when primary official source fails.
"""

import time
from datetime import date
from pathlib import Path

from nse_pipeline.ingest.base import BhavcopySource
from nse_pipeline.ingest.nse_source import NseSource
from nse_pipeline.ingest.yfinance_source import YFinanceSource
from nse_pipeline.utils.calendar import is_trading_day
from nse_pipeline.utils.logging import logger


class BhavcopyDownloader:
    """Manages the download lifecycle for raw market bhavcopy files into data/bronze."""

    def __init__(
        self,
        output_base_dir: Path | str = "data/bronze",
        primary_source: BhavcopySource | None = None,
        fallback_source: BhavcopySource | None = None,
        holidays_path: Path | str = "config/holidays.csv",
        delay_seconds: float = 2.0,
        max_retries: int = 5,
        backoff_factor: float = 2.0,
    ):
        self.output_base_dir = Path(output_base_dir)
        self.primary_source = primary_source or NseSource()
        self.fallback_source = fallback_source or YFinanceSource()
        self.holidays_path = Path(holidays_path)
        self.delay_seconds = delay_seconds
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def _get_partition_dir(self, trade_date: date) -> Path:
        """Returns Hive-style partition directory path: data/bronze/trade_date=YYYY-MM-DD."""
        return self.output_base_dir / f"trade_date={trade_date.strftime('%Y-%m-%d')}"

    def get_existing_file(self, trade_date: date) -> Path | None:
        """Returns the path to an already downloaded raw file if present in bronze."""
        partition_dir = self._get_partition_dir(trade_date)
        if not partition_dir.is_dir():
            return None

        # Look for non-empty CSV files in the partition
        for file in partition_dir.glob("*.csv"):
            if file.stat().st_size > 0:
                return file
        return None

    def download(
        self,
        trade_date: date,
        force: bool = False,
        use_fallback: bool = True,
    ) -> Path:
        """Downloads the bhavcopy for trade_date into bronze partition storage.

        Args:
            trade_date: Target trading session date.
            force: When True, overwrites existing downloaded file.
            use_fallback: When True, tries fallback source if primary fails.

        Returns:
            Path to the saved raw file in bronze.
        """
        # 1. Idempotency check: avoid redundant network traffic
        if not force:
            existing = self.get_existing_file(trade_date)
            if existing:
                logger.info(
                    "Idempotency hit: Raw file already exists in bronze at %s. Skipping download.",
                    existing,
                )
                return existing

        # 2. Schedule validation: verify trading day
        if not force and not is_trading_day(trade_date, self.holidays_path):
            raise ValueError(
                f"{trade_date} is not an official NSE trading day (weekend or market holiday). "
                "Use force=True if you need to fetch anyway."
            )

        partition_dir = self._get_partition_dir(trade_date)
        partition_dir.mkdir(parents=True, exist_ok=True)

        # 3. Polite pause before making request
        if self.delay_seconds > 0:
            logger.debug(
                "Applying polite delay of %.2f seconds before ingest...", self.delay_seconds
            )
            time.sleep(self.delay_seconds)

        # 4. Attempt primary source with exponential backoff
        primary_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "Downloading via %s (attempt %d/%d) for date %s...",
                    self.primary_source.source_name,
                    attempt,
                    self.max_retries,
                    trade_date,
                )
                return self.primary_source.fetch(trade_date, partition_dir)

            except PermissionError as e:
                # 403 or 429 received: STOP hitting the server immediately to prevent blacklisting
                logger.error("Rate-limit or access forbidden on primary source: %s", e)
                primary_error = e
                break

            except FileNotFoundError as e:
                # File not available (market closed or file not ready)
                logger.warning("Primary source returned file not found: %s", e)
                primary_error = e
                break

            except Exception as e:
                primary_error = e
                if attempt == self.max_retries:
                    logger.error("Primary source exhausted all %d retries: %s", self.max_retries, e)
                    break

                sleep_time = self.delay_seconds * (self.backoff_factor ** (attempt - 1))
                logger.warning(
                    "Attempt %d failed (%s). Retrying in %.2f seconds...",
                    attempt,
                    e,
                    sleep_time,
                )
                time.sleep(sleep_time)

        # 5. Try fallback source if primary failed and fallback is enabled
        if use_fallback and self.fallback_source:
            logger.info(
                "Primary source (%s) failed; triggering fallback source (%s)...",
                self.primary_source.source_name,
                self.fallback_source.source_name,
            )
            try:
                return self.fallback_source.fetch(trade_date, partition_dir)
            except Exception as fallback_err:
                logger.error("Fallback source also failed: %s", fallback_err)
                raise RuntimeError(
                    f"Both primary ({primary_error}) and fallback ({fallback_err}) sources failed for {trade_date}."
                ) from fallback_err

        raise RuntimeError(f"Ingestion failed for {trade_date}: {primary_error}") from primary_error
