"""Fallback data source powered by Yahoo Finance (yfinance).

Why this design:
Official exchange archives may become temporarily unreachable, IP-restricted,
or historically truncated. A fallback provider guarantees pipeline continuity for
historical backfills and local development when official files are unavailable.
This is explicitly marked as best-effort since yfinance is an unofficial API.
"""

import csv
from datetime import date, timedelta
from pathlib import Path

import yaml
import yfinance as yf

from nse_pipeline.ingest.base import BhavcopySource
from nse_pipeline.utils.logging import logger


class YFinanceSource(BhavcopySource):
    """Fallback market data extractor built on yfinance for configured symbols."""

    def __init__(self, symbols_path: Path | str = "config/symbols.yaml"):
        self.symbols_path = Path(symbols_path)

    @property
    def source_name(self) -> str:
        return "yfinance_fallback"

    def _load_symbols(self) -> list[str]:
        """Loads target symbol list from config/symbols.yaml."""
        if not self.symbols_path.is_file():
            logger.warning(
                "Symbols config %s not found; defaulting to NIFTY 50 anchors", self.symbols_path
            )
            return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"]

        with open(self.symbols_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get("symbols", [])

    def fetch(self, trade_date: date, output_dir: Path) -> Path:
        """Fetches OHLCV data for configured symbols via yfinance for the specified date."""
        output_dir.mkdir(parents=True, exist_ok=True)
        symbols = self._load_symbols()
        if not symbols:
            raise ValueError("Symbol list is empty. Cannot fetch fallback data.")

        start_str = trade_date.strftime("%Y-%m-%d")
        end_str = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")
        timestamp_str = trade_date.strftime("%d-%b-%Y").upper()
        date_yyyymmdd = trade_date.strftime("%Y%m%d")

        logger.info(
            "Fetching fallback data via yfinance for %d symbols on %s",
            len(symbols),
            start_str,
        )

        rows = []
        for symbol in symbols:
            ticker_sym = f"{symbol}.NS"
            try:
                ticker = yf.Ticker(ticker_sym)
                hist = ticker.history(start=start_str, end=end_str)
                if hist.empty:
                    continue

                row_data = hist.iloc[0]
                open_p = round(float(row_data["Open"]), 2)
                high_p = round(float(row_data["High"]), 2)
                low_p = round(float(row_data["Low"]), 2)
                close_p = round(float(row_data["Close"]), 2)
                volume = int(row_data["Volume"])
                turnover = round(close_p * volume, 2)

                rows.append(
                    {
                        "SYMBOL": symbol,
                        "SERIES": "EQ",
                        "OPEN": open_p,
                        "HIGH": high_p,
                        "LOW": low_p,
                        "CLOSE": close_p,
                        "LAST": close_p,
                        "PREVCLOSE": open_p,  # Best-effort approximation for fallback
                        "TOTTRDQTY": volume,
                        "TOTTRDVAL": turnover,
                        "TIMESTAMP": timestamp_str,
                        "TOTALTRADES": 0,  # yfinance does not report trade count
                        "ISIN": "",
                    }
                )
            except Exception as e:
                logger.debug("Failed to fetch yfinance data for %s: %s", ticker_sym, e)
                continue

        if not rows:
            raise FileNotFoundError(
                f"No yfinance data returned for {trade_date}. Date may be a weekend, holiday, or invalid."
            )

        target_file = output_dir / f"yfinance_fallback_{date_yyyymmdd}.csv"
        fieldnames = [
            "SYMBOL",
            "SERIES",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "LAST",
            "PREVCLOSE",
            "TOTTRDQTY",
            "TOTTRDVAL",
            "TIMESTAMP",
            "TOTALTRADES",
            "ISIN",
        ]

        with open(target_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(
            "Saved %d records to yfinance fallback file: %s",
            len(rows),
            target_file,
        )
        return target_file
