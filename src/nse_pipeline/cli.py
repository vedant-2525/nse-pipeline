"""Command Line Interface (CLI) for the NSE Market Data Pipeline.

Provides subcommands to run ingestion, pipeline phases, and backfills from the terminal.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

from nse_pipeline.ingest.downloader import BhavcopyDownloader
from nse_pipeline.ingest.nse_source import NseSource
from nse_pipeline.ingest.yfinance_source import YFinanceSource
from nse_pipeline.utils.calendar import is_trading_day
from nse_pipeline.utils.logging import logger


def parse_date(date_str: str) -> date:
    """Parses a YYYY-MM-DD string into a datetime.date object."""
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Expected format is YYYY-MM-DD."
        ) from e


def handle_ingest(args: argparse.Namespace) -> int:
    """Handles the 'ingest' CLI subcommand."""
    target_date: date = args.date
    output_dir: Path = Path(args.output_dir)
    force: bool = args.force

    logger.info("Executing ingest command for date: %s", target_date)

    primary = NseSource() if args.source in ("nse", "auto") else None
    fallback = YFinanceSource() if args.source in ("yfinance", "auto") else None

    # If user explicitly chose yfinance as sole source:
    if args.source == "yfinance":
        primary = YFinanceSource()
        fallback = None

    downloader = BhavcopyDownloader(
        output_base_dir=output_dir,
        primary_source=primary,
        fallback_source=fallback,
    )

    try:
        file_path = downloader.download(
            trade_date=target_date,
            force=force,
            use_fallback=(args.source == "auto"),
        )
        logger.info("SUCCESS: Raw file saved in Bronze lake at: %s", file_path)
        print(f"Downloaded: {file_path}")
        return 0
    except Exception as e:
        logger.error("FAILED to ingest date %s: %s", target_date, e)
        print(f"Error: {e}", file=sys.stderr)
        return 1


def handle_run(args: argparse.Namespace) -> int:
    """Handles the 'run' CLI subcommand to execute the pipeline for a single date."""
    target_date: date = args.date
    logger.info("Starting pipeline execution for trade date: %s", target_date)

    if not is_trading_day(target_date) and not args.force:
        logger.warning("Date %s is not an active trading day. Skipping run.", target_date)
        print(f"Skipped: {target_date} is not a trading day.")
        return 0

    # Phase 1: Ingestion
    downloader = BhavcopyDownloader(output_base_dir=Path("data/bronze"))
    try:
        raw_file = downloader.download(trade_date=target_date, force=args.force)
        logger.info("Phase 1 Complete: Raw file in Bronze -> %s", raw_file)
    except Exception as e:
        logger.error("Phase 1 Ingest failed: %s", e)
        return 1

    # Future phases (Transform, Quality, Load) will be wired up in Phases 2-5
    logger.info("Pipeline run for %s completed Phase 1 successfully.", target_date)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="nse-pipeline",
        description="NSE Market Data Pipeline CLI tool",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Ingest command
    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest raw bhavcopy for a specific date into Bronze"
    )
    ingest_parser.add_argument(
        "--date",
        type=parse_date,
        required=True,
        help="Trading date in YYYY-MM-DD format (e.g. 2024-10-11)",
    )
    ingest_parser.add_argument(
        "--source",
        choices=["auto", "nse", "yfinance"],
        default="auto",
        help="Data source to use (default: auto with fallback)",
    )
    ingest_parser.add_argument(
        "--output-dir",
        default="data/bronze",
        help="Base directory for Bronze data lake (default: data/bronze)",
    )
    ingest_parser.add_argument(
        "--force",
        action="store_true",
        help="Force download even if file already exists or date is a holiday/weekend",
    )

    # Run command (runs pipeline end-to-end for one date)
    run_parser = subparsers.add_parser("run", help="Run end-to-end pipeline stages for a date")
    run_parser.add_argument(
        "--date",
        type=parse_date,
        required=True,
        help="Trading date in YYYY-MM-DD format",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="Force execution even if date is a holiday or files already exist",
    )

    args = parser.parse_args(argv)

    if args.command == "ingest":
        return handle_ingest(args)
    elif args.command == "run":
        return handle_run(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
