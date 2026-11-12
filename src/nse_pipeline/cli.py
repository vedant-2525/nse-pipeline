"""Command Line Interface (CLI) for the NSE Market Data Pipeline.

Provides subcommands to run ingestion, Silver transformations, and pipeline phases.
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import yaml

from nse_pipeline.ingest.downloader import BhavcopyDownloader
from nse_pipeline.ingest.nse_source import NseSource
from nse_pipeline.ingest.yfinance_source import YFinanceSource
from nse_pipeline.load.db import (
    apply_sql_migrations,
    get_db_connection,
    is_postgres_reachable,
    populate_dim_date,
    seed_dim_symbols,
)
from nse_pipeline.load.loader import load_silver_partition
from nse_pipeline.transform.run_silver import run_silver_transform
from nse_pipeline.utils.calendar import (
    check_holiday_coverage,
    get_trading_days,
    is_trading_day,
)
from nse_pipeline.utils.logging import logger
from nse_pipeline.utils.spark import stop_spark_session


def parse_date(date_str: str) -> date:
    """Parses a YYYY-MM-DD string into a datetime.date object."""
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Expected format is YYYY-MM-DD."
        ) from e


def load_configured_symbols(config_path: str = "config/symbols.yaml") -> list[str]:
    """Loads configured equity symbols (e.g. Nifty 50)."""
    p = Path(config_path)
    if p.is_file():
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get("symbols", [])
    return []


def handle_ingest(args: argparse.Namespace) -> int:
    """Handles the 'ingest' CLI subcommand."""
    target_date: date = args.date
    output_dir: Path = Path(args.output_dir)
    force: bool = args.force

    check_holiday_coverage(target_date)
    logger.info("Executing ingest command for date: %s", target_date)

    primary = NseSource() if args.source in ("nse", "auto") else None
    fallback = YFinanceSource() if args.source in ("yfinance", "auto") else None

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


def handle_transform(args: argparse.Namespace) -> int:
    """Handles the 'transform' CLI subcommand (Bronze -> Silver)."""
    dates_to_run: list[date] = []

    if args.date:
        dates_to_run = [args.date]
    elif args.from_date and args.to_date:
        if args.from_date > args.to_date:
            print("Error: --from date cannot be later than --to date.", file=sys.stderr)
            return 1
        dates_to_run = get_trading_days(args.from_date, args.to_date)
    else:
        print("Error: Either --date or both --from and --to must be specified.", file=sys.stderr)
        return 1

    check_holiday_coverage(dates_to_run or [args.from_date, args.to_date])

    symbols_filter = None if args.all_symbols else load_configured_symbols()
    allow_fallback: bool = args.allow_fallback

    has_error = False
    try:
        for dt in dates_to_run:
            if not is_trading_day(dt):
                print(f"Skipped: {dt} is not an official NSE trading day (holiday/weekend).")
                continue

            try:
                metrics = run_silver_transform(
                    trade_date=dt,
                    bronze_dir=args.bronze_dir,
                    silver_dir=args.silver_dir,
                    quarantine_dir=args.quarantine_dir,
                    symbol_universe=symbols_filter,
                    allow_fallback=allow_fallback,
                )
                print(
                    f"Transform Success [{dt}]: Read={metrics['read_count']} | "
                    f"NonEquity={metrics['non_equity_filtered']} | "
                    f"Universe={metrics['universe_filtered']} | "
                    f"Rejected={metrics['rejected']} | "
                    f"Deduped={metrics['deduped']} | "
                    f"RowCount={metrics['row_count']} | "
                    f"Parquet={metrics['parquet_path']}"
                )
            except Exception as e:
                logger.error("Transform failed for date %s: %s", dt, e)
                print(f"Transform Error [{dt}]: {e}", file=sys.stderr)
                has_error = True
                break
    finally:
        stop_spark_session()

    return 1 if has_error else 0


def handle_db(args: argparse.Namespace) -> int:
    """Handles the 'db' CLI command (e.g. 'db init')."""
    if getattr(args, "db_action", None) != "init":
        print("Usage: python -m nse_pipeline.cli db init", file=sys.stderr)
        return 1

    if not is_postgres_reachable():
        print(
            "Error: PostgreSQL warehouse is not reachable.\n"
            "Action required: Ensure Docker Desktop is running and start the container:\n"
            "  docker compose up -d\n"
            "Then re-run: python -m nse_pipeline.cli db init",
            file=sys.stderr,
        )
        return 1

    try:
        with get_db_connection(autocommit=True) as conn:
            applied = apply_sql_migrations(conn)
            print(f"Applied {len(applied)} SQL migration files: {', '.join(applied)}")
            symbols_count = seed_dim_symbols(conn)
            print(f"Seeded {symbols_count} symbols into gold.dim_symbol.")
            dates_count = populate_dim_date(conn)
            print(f"Populated {dates_count} dates into gold.dim_date (2020 to 2030).")

        print("Warehouse initialized successfully.")
        return 0
    except Exception as e:
        logger.error("Database initialization failed: %s", e)
        print(f"Database Initialization Error: {e}", file=sys.stderr)
        return 1


def handle_load(args: argparse.Namespace) -> int:
    """Handles the 'load' CLI command (Silver Parquet -> Gold Fact Table)."""
    dates_to_run: list[date] = []

    if args.date:
        dates_to_run = [args.date]
    elif args.from_date and args.to_date:
        if args.from_date > args.to_date:
            print("Error: --from date cannot be later than --to date.", file=sys.stderr)
            return 1
        dates_to_run = get_trading_days(args.from_date, args.to_date)
    else:
        print("Error: Either --date or both --from and --to must be specified.", file=sys.stderr)
        return 1

    check_holiday_coverage(dates_to_run or [args.from_date, args.to_date])

    if not is_postgres_reachable():
        print(
            "Error: PostgreSQL warehouse is not reachable.\n"
            "Action required: Start the database container via: docker compose up -d",
            file=sys.stderr,
        )
        return 1

    has_error = False
    for dt in dates_to_run:
        if not is_trading_day(dt):
            print(f"Skipped: {dt} is not an official NSE trading day (holiday/weekend).")
            continue

        try:
            metrics = load_silver_partition(
                trade_date=dt,
                silver_base_dir=args.silver_dir,
                allow_fallback=args.allow_fallback,
                include_all_symbols=getattr(args, "include_all_symbols", False),
            )
            print(
                f"Load Success [{dt}]: RowsIn={metrics['rows_in']} | "
                f"RowsOut={metrics['rows_out']} | Duration={metrics['duration_seconds']}s"
            )
        except Exception as e:
            logger.error("Warehouse load failed for date %s: %s", dt, e)
            print(f"Load Error [{dt}]: {e}", file=sys.stderr)
            has_error = True
            break

    return 1 if has_error else 0


def handle_run(args: argparse.Namespace) -> int:
    """Handles the 'run' CLI subcommand to execute the pipeline for a single date."""
    target_date: date = args.date
    check_holiday_coverage(target_date)
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
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Phase 2: Silver Transformation
    try:
        metrics = run_silver_transform(trade_date=target_date)
        logger.info("Phase 2 Complete: Silver Parquet written -> %s", metrics["parquet_path"])
    except Exception as e:
        logger.error("Phase 2 Silver Transform failed: %s", e)
        print(f"Error: {e}", file=sys.stderr)
        return 1

    logger.info("Pipeline run for %s completed Phase 1 & 2 successfully.", target_date)
    return 0


def handle_backfill(args: argparse.Namespace) -> int:
    """Handles the 'backfill' CLI command across a date range."""
    from nse_pipeline.orchestration.backfill import run_backfill

    if not args.from_date or not args.to_date:
        print("Error: Both --from and --to dates must be specified for backfill.", file=sys.stderr)
        return 1

    exit_code, _ = run_backfill(
        from_date=args.from_date,
        to_date=args.to_date,
        force=args.force,
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        quarantine_dir=args.quarantine_dir,
        reports_dir=args.reports_dir,
    )
    return exit_code


def handle_daily(args: argparse.Namespace) -> int:
    """Handles the 'daily' CLI command for single-day pipeline runs."""
    from nse_pipeline.orchestration.daily import run_daily_pipeline

    exit_code, _ = run_daily_pipeline(
        target_date=args.date,
        cutoff_hour=args.cutoff_hour,
        force=args.force,
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
        quarantine_dir=args.quarantine_dir,
        reports_dir=args.reports_dir,
    )
    return exit_code


def main(argv: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        prog="nse-pipeline",
        description="NSE Market Data Pipeline CLI tool",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # 1. Ingest command
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

    # 2. Transform command (Phase 2)
    transform_parser = subparsers.add_parser(
        "transform", help="Transform Bronze raw files into clean Silver Parquet"
    )
    transform_parser.add_argument(
        "--date",
        type=parse_date,
        help="Single trading date in YYYY-MM-DD format",
    )
    transform_parser.add_argument(
        "--from",
        dest="from_date",
        type=parse_date,
        help="Start date in YYYY-MM-DD format for range processing",
    )
    transform_parser.add_argument(
        "--to",
        dest="to_date",
        type=parse_date,
        help="End date in YYYY-MM-DD format for range processing",
    )
    transform_parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Path to Bronze storage base folder",
    )
    transform_parser.add_argument(
        "--silver-dir",
        default="data/silver/equity_prices",
        help="Path to Silver storage folder",
    )
    transform_parser.add_argument(
        "--quarantine-dir",
        default="data/quarantine/equity_prices",
        help="Path to Quarantine storage folder",
    )
    transform_parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Process all equity symbols in the market rather than filtering by config/symbols.yaml",
    )
    transform_parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Permit processing fallback (yfinance) files when official NSE files are missing",
    )

    # 3. Load command (Phase 3)
    load_parser = subparsers.add_parser(
        "load", help="Load clean Silver Parquet data into Gold warehouse"
    )
    load_parser.add_argument(
        "--date",
        type=parse_date,
        help="Single trading date in YYYY-MM-DD format",
    )
    load_parser.add_argument(
        "--from",
        dest="from_date",
        type=parse_date,
        help="Start date in YYYY-MM-DD format for range processing",
    )
    load_parser.add_argument(
        "--to",
        dest="to_date",
        type=parse_date,
        help="End date in YYYY-MM-DD format for range processing",
    )
    load_parser.add_argument(
        "--silver-dir",
        default="data/silver/equity_prices",
        help="Base directory containing Silver Parquet partitions",
    )
    load_parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Permit loading fallback (yfinance) partition into warehouse",
    )
    load_parser.add_argument(
        "--include-all-symbols",
        action="store_true",
        help="Permit loading Silver partitions generated with --all-symbols ('all_eq' universe)",
    )

    # 4. Database management command (Phase 3)
    db_parser = subparsers.add_parser("db", help="Database management commands")
    db_subparsers = db_parser.add_subparsers(dest="db_action", help="DB actions")
    db_subparsers.add_parser(
        "init", help="Apply SQL schemas and seed dimensions (dim_symbol, dim_date)"
    )

    # 5. Run command (runs pipeline end-to-end for one date)
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

    # 6. Backfill command (Phase 4)
    backfill_parser = subparsers.add_parser(
        "backfill", help="Run historical batch backfill across a date range"
    )
    backfill_parser.add_argument(
        "--from",
        dest="from_date",
        type=parse_date,
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    backfill_parser.add_argument(
        "--to",
        dest="to_date",
        type=parse_date,
        required=True,
        help="End date in YYYY-MM-DD format",
    )
    backfill_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-execute backfill even if data is already loaded in warehouse",
    )
    backfill_parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Path to Bronze storage base folder (default: data/bronze)",
    )
    backfill_parser.add_argument(
        "--silver-dir",
        default="data/silver/equity_prices",
        help="Path to Silver storage folder (default: data/silver/equity_prices)",
    )
    backfill_parser.add_argument(
        "--quarantine-dir",
        default="data/quarantine/equity_prices",
        help="Path to Quarantine storage folder (default: data/quarantine/equity_prices)",
    )
    backfill_parser.add_argument(
        "--reports-dir",
        default="data/reports",
        help="Path to directory where backfill audit JSON reports are written (default: data/reports)",
    )

    # 7. Daily command
    daily_parser = subparsers.add_parser(
        "daily", help="Run end-to-end pipeline for the most recent or specified trading day"
    )
    daily_parser.add_argument(
        "--date",
        type=parse_date,
        help="Target trading date in YYYY-MM-DD format (default: resolved from current time and cutoff)",
    )
    daily_parser.add_argument(
        "--cutoff-hour",
        type=int,
        default=19,
        help="Hour in IST after which today's file is expected (default: 19)",
    )
    daily_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-execute pipeline even if data is already loaded in warehouse",
    )
    daily_parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Path to Bronze storage base folder (default: data/bronze)",
    )
    daily_parser.add_argument(
        "--silver-dir",
        default="data/silver/equity_prices",
        help="Path to Silver storage folder (default: data/silver/equity_prices)",
    )
    daily_parser.add_argument(
        "--quarantine-dir",
        default="data/quarantine/equity_prices",
        help="Path to Quarantine storage folder (default: data/quarantine/equity_prices)",
    )
    daily_parser.add_argument(
        "--reports-dir",
        default="data/reports",
        help="Path to directory where audit JSON reports are written (default: data/reports)",
    )

    args = parser.parse_args(argv)

    if args.command == "ingest":
        return handle_ingest(args)
    elif args.command == "transform":
        return handle_transform(args)
    elif args.command == "load":
        return handle_load(args)
    elif args.command == "db":
        return handle_db(args)
    elif args.command == "run":
        return handle_run(args)
    elif args.command == "backfill":
        return handle_backfill(args)
    elif args.command == "daily":
        return handle_daily(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
