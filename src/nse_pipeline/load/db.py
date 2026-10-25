"""Database connection, schema initialization, and dimensional seeding helpers using psycopg 3.

Why this design:
1. Native psycopg 3 provides high performance and type safety without the overhead of heavy ORMs.
2. Idempotent initialization: SQL migration scripts and dimensional seeds can be re-run safely.
3. Separation of Concerns: Connection handling, schema execution, and seeding are encapsulated here,
   allowing tests to mock connections or test against local PostgreSQL containers.
"""

import csv
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv

from nse_pipeline.utils.calendar import get_holidays
from nse_pipeline.utils.logging import logger

# Load environment configuration from .env if present
load_dotenv()


def get_db_config() -> dict[str, Any]:
    """Retrieves PostgreSQL connection parameters from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "nse_warehouse"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "connect_timeout": 3,
    }


def get_db_connection(autocommit: bool = False) -> psycopg.Connection:
    """Creates a new psycopg 3 connection to the PostgreSQL warehouse."""
    config = get_db_config()
    conn = psycopg.connect(**config, autocommit=autocommit)
    return conn


def is_postgres_reachable() -> bool:
    """Verifies whether the target PostgreSQL warehouse is reachable."""
    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                res = cur.fetchone()
                return bool(res and res[0] == 1)
    except Exception:
        return False


def apply_sql_migrations(conn: psycopg.Connection, sql_dir: Path | str = "sql") -> list[str]:
    """Applies numbered SQL migration files in ascending order.

    Returns:
        List of applied SQL filenames.
    """
    sql_path = Path(sql_dir)
    if not sql_path.is_dir():
        raise FileNotFoundError(f"SQL migrations directory not found at: {sql_path}")

    sql_files = sorted([f for f in sql_path.glob("*.sql") if f.name != ".gitkeep"])
    applied = []

    with conn.transaction():
        with conn.cursor() as cur:
            for sf in sql_files:
                logger.info("Applying SQL migration: %s", sf.name)
                content = sf.read_text(encoding="utf-8")
                cur.execute(content)
                applied.append(sf.name)

    logger.info("Successfully applied %d SQL migrations.", len(applied))
    return applied


def seed_dim_symbols(
    conn: psycopg.Connection, sectors_path: Path | str = "config/sectors.csv"
) -> int:
    """Seeds gold.dim_symbol from config/sectors.csv (upsert).

    Returns:
        Number of symbols seeded or updated.
    """
    path = Path(sectors_path)
    if not path.is_file():
        raise FileNotFoundError(f"Sectors mapping file not found at: {path}")

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = [
            (
                row["symbol"].strip().upper(),
                row.get("company_name", "").strip(),
                row.get("sector", "").strip(),
            )
            for row in reader
            if row.get("symbol")
        ]

    upsert_sql = """
        INSERT INTO gold.dim_symbol (symbol, company_name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            updated_at = CURRENT_TIMESTAMP;
    """

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(upsert_sql, records)

    logger.info("Successfully seeded/updated %d symbols in gold.dim_symbol.", len(records))
    return len(records)


def populate_dim_date(
    conn: psycopg.Connection,
    start_date: date = date(2020, 1, 1),
    end_date: date = date(2030, 12, 31),
    holidays_path: Path | str = "config/holidays.csv",
) -> int:
    """Populates gold.dim_date calendar for the range 2020-01-01 to 2030-12-31.

    Returns:
        Total number of calendar days populated.
    """
    holidays_map = get_holidays(holidays_path)
    current = start_date
    records = []

    while current <= end_date:
        date_id = current.year * 10000 + current.month * 100 + current.day
        year = current.year
        quarter = (current.month - 1) // 3 + 1
        month = current.month
        day_of_week = current.isoweekday()  # 1=Mon, 7=Sun
        day_name = current.strftime("%A")
        is_weekend = day_of_week in (6, 7)
        is_holiday = current in holidays_map
        is_trading_day = (not is_weekend) and (not is_holiday)

        records.append(
            (
                date_id,
                current,
                year,
                quarter,
                month,
                day_of_week,
                day_name,
                is_weekend,
                is_holiday,
                is_trading_day,
            )
        )
        current += timedelta(days=1)

    insert_sql = """
        INSERT INTO gold.dim_date (
            date_id, date, year, quarter, month, day_of_week, day_name, is_weekend, is_holiday, is_trading_day
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO UPDATE SET
            is_holiday = EXCLUDED.is_holiday,
            is_trading_day = EXCLUDED.is_trading_day;
    """

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(insert_sql, records)

    logger.info("Successfully populated %d date dimension rows in gold.dim_date.", len(records))
    return len(records)
