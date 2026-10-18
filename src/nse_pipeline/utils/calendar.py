"""Trading calendar utilities for NSE market schedules.

Stock exchanges do not operate on weekends or gazetted market holidays.
We load holidays from an external CSV file so schedule changes can be made
without requiring code deployments.
"""

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

_HOLIDAY_CACHE: dict[str, set[date]] = {}


def is_weekend(target_date: date) -> bool:
    """Returns True if the date falls on Saturday (5) or Sunday (6).

    Why this design:
    NSE equity markets are closed every Saturday and Sunday. Filtering weekends
    early prevents futile network requests to the exchange servers.
    """
    return target_date.weekday() >= 5


def get_holidays(holidays_path: Path | str = "config/holidays.csv") -> set[date]:
    """Reads holiday dates from config/holidays.csv and caches the result.

    Why this design:
    Caching prevents reading the CSV from disk repeatedly during batch backfills
    across hundreds of trading days.
    """
    path_key = str(Path(holidays_path).resolve())
    if path_key in _HOLIDAY_CACHE:
        return _HOLIDAY_CACHE[path_key]

    holidays: set[date] = set()
    file_path = Path(holidays_path)

    if file_path.is_file():
        with open(file_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_date = row.get("holiday_date") or row.get("trade_date")
                if raw_date:
                    try:
                        holidays.add(datetime.strptime(raw_date.strip(), "%Y-%m-%d").date())
                    except ValueError:
                        continue

    _HOLIDAY_CACHE[path_key] = holidays
    return holidays


def is_trading_day(target_date: date, holidays_path: Path | str = "config/holidays.csv") -> bool:
    """Returns True if target_date is neither a weekend nor an official NSE holiday."""
    if is_weekend(target_date):
        return False
    holidays = get_holidays(holidays_path)
    return target_date not in holidays


def get_previous_trading_day(
    target_date: date, holidays_path: Path | str = "config/holidays.csv"
) -> date:
    """Finds the most recent valid trading day strictly before target_date.

    Why this design:
    Daily return calculations and moving averages depend on comparing with the
    immediately preceding active trading session, not simply (T - 1 calendar day).
    """
    candidate = target_date - timedelta(days=1)
    while not is_trading_day(candidate, holidays_path):
        candidate -= timedelta(days=1)
    return candidate


def get_trading_days(
    start_date: date, end_date: date, holidays_path: Path | str = "config/holidays.csv"
) -> list[date]:
    """Returns a sorted list of all active trading days between start_date and end_date (inclusive)."""
    days: list[date] = []
    current = start_date
    while current <= end_date:
        if is_trading_day(current, holidays_path):
            days.append(current)
        current += timedelta(days=1)
    return days
