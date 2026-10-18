"""Abstract base class interface for Bhavcopy data sources.

Why this design:
Using an abstract interface follows the Strategy Pattern and Dependency Inversion Principle.
Downstream pipeline stages (cleaners, loaders) remain completely decoupled from how or where
raw market data was acquired (official NSE archives vs yfinance fallback).
"""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path


class BhavcopySource(ABC):
    """Abstract interface for ingesting raw daily market files."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Returns the unique human-readable identifier for the source."""
        pass

    @abstractmethod
    def fetch(self, trade_date: date, output_dir: Path) -> Path:
        """Fetches the raw daily bhavcopy file for the specified date.

        Args:
            trade_date: The date for which to download the market file.
            output_dir: Destination folder where the untouched raw file must be written.

        Returns:
            The Path to the locally saved raw file.

        Raises:
            FileNotFoundError: If the file does not exist (holiday, weekend, or not yet published).
            PermissionError: If rate limits (403, 429) or anti-bot defenses block the request.
            ConnectionError: If network connectivity or transport errors occur.
        """
        pass
