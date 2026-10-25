"""Unit tests for trading calendar and holiday helper functions."""

from datetime import date
from pathlib import Path

from nse_pipeline.utils.calendar import (
    check_holiday_coverage,
    get_holidays,
    get_previous_trading_day,
    get_trading_days,
    is_trading_day,
    is_weekend,
)


def test_is_weekend():
    """Verify Saturday and Sunday are identified as weekends, while Monday-Friday are not."""
    saturday = date(2024, 10, 12)
    sunday = date(2024, 10, 13)
    monday = date(2024, 10, 14)
    wednesday = date(2024, 10, 16)
    friday = date(2024, 10, 18)

    assert is_weekend(saturday) is True
    assert is_weekend(sunday) is True
    assert is_weekend(monday) is False
    assert is_weekend(wednesday) is False
    assert is_weekend(friday) is False


def test_get_holidays_from_csv(tmp_path: Path):
    """Verify holiday parser reads dates correctly from CSV."""
    test_holidays_csv = tmp_path / "holidays.csv"
    test_holidays_csv.write_text(
        "holiday_date,description\n2024-01-26,Republic Day\n2024-08-15,Independence Day\n",
        encoding="utf-8",
    )

    holidays = get_holidays(test_holidays_csv)
    assert date(2024, 1, 26) in holidays
    assert date(2024, 8, 15) in holidays
    assert date(2024, 1, 1) not in holidays


def test_is_trading_day_weekend_and_holiday(tmp_path: Path):
    """Verify market holidays and weekends are not classified as trading days."""
    test_holidays_csv = tmp_path / "holidays.csv"
    test_holidays_csv.write_text(
        "holiday_date,description\n2024-01-26,Republic Day\n",
        encoding="utf-8",
    )

    holiday = date(2024, 1, 26)  # Friday holiday
    weekend = date(2024, 1, 27)  # Saturday
    trading_day = date(2024, 1, 25)  # Thursday normal session

    assert is_trading_day(holiday, test_holidays_csv) is False
    assert is_trading_day(weekend, test_holidays_csv) is False
    assert is_trading_day(trading_day, test_holidays_csv) is True


def test_get_previous_trading_day_across_weekend_and_holiday(tmp_path: Path):
    """Verify previous trading day calculation skips weekends and holidays."""
    test_holidays_csv = tmp_path / "holidays.csv"
    # 2024-01-26 (Friday) is a holiday; 2024-01-27 (Sat) and 28 (Sun) are weekend
    test_holidays_csv.write_text(
        "holiday_date,description\n2024-01-26,Republic Day\n",
        encoding="utf-8",
    )

    monday = date(2024, 1, 29)
    # The previous trading day before Monday Jan 29 should skip Sunday, Saturday, and Friday holiday -> Thursday Jan 25
    prev = get_previous_trading_day(monday, test_holidays_csv)
    assert prev == date(2024, 1, 25)


def test_get_trading_days_range(tmp_path: Path):
    """Verify trading days generator in a date window."""
    test_holidays_csv = tmp_path / "holidays.csv"
    test_holidays_csv.write_text(
        "holiday_date,description\n2024-10-02,Gandhi Jayanti\n",
        encoding="utf-8",
    )

    # 2024-09-30 (Mon) to 2024-10-04 (Fri)
    # Oct 2 is a holiday, Oct 5-6 are weekend
    start = date(2024, 9, 30)
    end = date(2024, 10, 4)

    trading_days = get_trading_days(start, end, test_holidays_csv)
    assert trading_days == [
        date(2024, 9, 30),
        date(2024, 10, 1),
        # 2024-10-02 omitted (holiday)
        date(2024, 10, 3),
        date(2024, 10, 4),
    ]


def test_check_holiday_coverage_warning(capsys):
    """Verify warning is emitted when requested date's year is missing from holidays.csv."""
    # 2024 is covered in config/holidays.csv
    missing = check_holiday_coverage(date(2024, 10, 11))
    assert missing == []
    captured = capsys.readouterr()
    assert "WARNING" not in captured.err

    # 2026 has no entries in config/holidays.csv
    missing_2026 = check_holiday_coverage(date(2026, 1, 15))
    assert missing_2026 == [2026]
    captured_2026 = capsys.readouterr()
    assert "WARNING: Year 2026 has no holiday entries" in captured_2026.err
    assert "treated as active trading days" in captured_2026.err
