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
from nse_pipeline.ingest.metadata import read_file_metadata, write_file_metadata
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

    def get_existing_file(
        self, trade_date: date, required_source: str | None = None
    ) -> Path | None:
        """Returns the path to an already downloaded raw file if present in bronze.

        Why this design:
        A fallback (yfinance) file must never block a subsequent official NSE fetch.
        When required_source is 'nse_official', only official NSE files (not labeled
        as fallback) count as an idempotency hit.
        """
        partition_dir = self._get_partition_dir(trade_date)
        if not partition_dir.is_dir():
            return None

        # Look for non-empty CSV files in the partition
        csv_files = [f for f in partition_dir.glob("*.csv") if f.stat().st_size > 0]
        if not csv_files:
            return None

        official_files = [
            f
            for f in csv_files
            if not f.name.startswith("yfinance_fallback_") and not f.name.startswith("fallback")
        ]
        fallback_files = [
            f
            for f in csv_files
            if f.name.startswith("yfinance_fallback_") or f.name.startswith("fallback")
        ]

        if required_source == "nse_official":
            return official_files[0] if official_files else None
        elif required_source == "yfinance_fallback":
            return fallback_files[0] if fallback_files else None
        else:
            return (
                official_files[0]
                if official_files
                else (fallback_files[0] if fallback_files else None)
            )

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
        # 1. Idempotency check: consider which source produced the file
        if not force:
            req_source = self.primary_source.source_name if self.primary_source else None
            existing = self.get_existing_file(trade_date, required_source=req_source)
            if existing:
                logger.info(
                    "Idempotency hit: Raw file (%s) already exists in bronze at %s. Skipping download.",
                    req_source or "any",
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
                res_file = self.primary_source.fetch(trade_date, partition_dir)
                if read_file_metadata(res_file) is None:
                    write_file_metadata(
                        res_file,
                        source_name=self.primary_source.source_name,
                        url="unknown",
                    )
                return res_file

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
            logger.warning(
                "\n"
                "********************************************************************************\n"
                "[WARNING] PRIMARY SOURCE (%s) FAILED FOR DATE %s: %s\n"
                "--> FALLING BACK TO SECONDARY SOURCE (%s).\n"
                "--> Note: Fallback data is best-effort and does not contain all official fields.\n"
                "********************************************************************************",
                self.primary_source.source_name,
                trade_date,
                primary_error,
                self.fallback_source.source_name,
            )
            try:
                fallback_file = self.fallback_source.fetch(trade_date, partition_dir)
                if read_file_metadata(fallback_file) is None:
                    write_file_metadata(
                        fallback_file,
                        source_name=self.fallback_source.source_name,
                        url="unknown",
                    )
                return fallback_file
            except Exception as fallback_err:
                logger.error("Fallback source also failed: %s", fallback_err)
                raise RuntimeError(
                    f"Both primary ({primary_error}) and fallback ({fallback_err}) sources failed for {trade_date}."
                ) from fallback_err

        raise RuntimeError(f"Ingestion failed for {trade_date}: {primary_error}") from primary_error
