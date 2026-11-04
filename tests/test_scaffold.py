"""Scaffold and environment tests for Phase 0."""

import csv
import os
from pathlib import Path

import yaml

import nse_pipeline


def test_package_version():
    """Verify nse_pipeline package is importable and has a version string."""
    assert hasattr(nse_pipeline, "__version__")
    assert isinstance(nse_pipeline.__version__, str)


def test_settings_yaml_valid():
    """Verify settings.yaml exists and contains essential configuration blocks."""
    config_path = Path("config/settings.yaml")
    assert config_path.is_file(), "config/settings.yaml must exist"

    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert "storage" in data
    assert "ingest" in data
    assert "bronze_path" in data["storage"]
    assert "silver_path" in data["storage"]


def test_symbols_yaml_valid():
    """Verify symbols.yaml exists, has no duplicates, and contains expected symbols."""
    symbols_path = Path("config/symbols.yaml")
    assert symbols_path.is_file(), "config/symbols.yaml must exist"

    with open(symbols_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert "symbols" in data
    symbols = data["symbols"]
    assert isinstance(symbols, list)
    assert len(symbols) >= 50
    # Enforce uniqueness (no duplicates)
    assert len(symbols) == len(set(symbols)), (
        "config/symbols.yaml must contain no duplicate entries"
    )
    assert "RELIANCE" in symbols
    assert "TCS" in symbols


def test_sectors_csv_valid():
    """Verify sectors.csv has correct columns and matches the symbol universe."""
    sectors_path = Path("config/sectors.csv")
    assert sectors_path.is_file(), "config/sectors.csv must exist"

    with open(sectors_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["symbol", "company_name", "sector"]
        rows = list(reader)

    assert len(rows) >= 50
    symbols_in_sectors = {r["symbol"] for r in rows}
    assert "RELIANCE" in symbols_in_sectors

    # Every symbol in config/symbols.yaml must have an entry in config/sectors.csv
    symbols_path = Path("config/symbols.yaml")
    with open(symbols_path, encoding="utf-8") as f:
        symbols_config = yaml.safe_load(f).get("symbols", [])
    missing = set(symbols_config) - symbols_in_sectors
    assert not missing, f"Symbols in symbols.yaml missing from sectors.csv: {missing}"


def test_holidays_csv_valid():
    """Verify holidays.csv has required header and entries."""
    holidays_path = Path("config/holidays.csv")
    assert holidays_path.is_file(), "config/holidays.csv must exist"

    with open(holidays_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["holiday_date", "description"]
        rows = list(reader)

    assert len(rows) > 0


def test_isolation_guard_prevents_real_warehouse_access():
    """Prove the autouse _no_real_warehouse_access guard is active for offline tests.

    This test does NOT use the postgres_test_db fixture, so the autouse fixture
    must have set POSTGRES_DB to a name ending in '_test'.  Any accidental
    get_db_connection() call in an offline test would therefore hit a safe
    guard database, never nse_warehouse.
    """
    db = os.environ.get("POSTGRES_DB", "")
    assert db.endswith("_test"), (
        f"Expected POSTGRES_DB to end in '_test' (isolation guard active), got '{db}'"
    )
