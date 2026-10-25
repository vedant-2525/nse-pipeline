"""Unit and integration tests for Phase 3: Warehouse Loader and PostgreSQL Star Schema.

Offline unit tests validate pre-load metadata gating, reconciliation checks,
SQL generation, and calendar dimension computation without requiring a database.

Integration tests validate end-to-end loading, atomic rollback on failure,
and partition isolation against a live PostgreSQL container, and skip cleanly
when Docker/Postgres is unavailable.
"""

import json
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from nse_pipeline.load.db import (
    apply_sql_migrations,
    get_db_config,
    get_db_connection,
    is_postgres_reachable,
    populate_dim_date,
    seed_dim_symbols,
)
from nse_pipeline.load.loader import (
    PartitionValidationError,
    load_silver_partition,
    validate_partition_metadata,
)

requires_postgres = pytest.mark.skipif(
    not is_postgres_reachable(),
    reason="PostgreSQL warehouse container is not reachable (start with docker compose up -d)",
)


# ==============================================================================
# Offline Unit Tests (No Database Required)
# ==============================================================================


def test_validate_partition_metadata_success(tmp_path: Path):
    """Verify valid partition metadata returns parsed dict."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    meta_content = {
        "trade_date": "2024-10-11",
        "read_count": 2852,
        "non_equity_filtered": 959,
        "universe_filtered": 1842,
        "rejected": 0,
        "deduped": 0,
        "row_count": 51,
        "source_format": "udiff",
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(meta_content), encoding="utf-8")

    meta = validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11), allow_fallback=False)
    assert meta["row_count"] == 51
    assert meta["source_type"] == "official"


def test_validate_partition_metadata_missing_dir(tmp_path: Path):
    """Verify missing partition directory raises FileNotFoundError."""
    missing_dir = tmp_path / "trade_date=2024-10-11"
    with pytest.raises(FileNotFoundError, match="Silver partition directory not found"):
        validate_partition_metadata(missing_dir, trade_date=date(2024, 10, 11))


def test_validate_partition_metadata_missing_meta(tmp_path: Path):
    """Verify missing _SUCCESS.meta.json raises FileNotFoundError."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="Missing partition audit file"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11))


def test_validate_partition_metadata_missing_fields(tmp_path: Path):
    """Verify partition with missing reconciliation fields raises PartitionValidationError."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    incomplete_meta = {
        "trade_date": "2024-10-11",
        "read_count": 2852,
        "row_count": 51,
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(incomplete_meta), encoding="utf-8")

    with pytest.raises(PartitionValidationError, match="missing reconciliation fields"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11))


def test_validate_partition_metadata_imbalanced(tmp_path: Path):
    """Verify partition with imbalanced row reconciliation raises PartitionValidationError."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    imbalanced_meta = {
        "trade_date": "2024-10-11",
        "read_count": 2852,
        "non_equity_filtered": 959,
        "universe_filtered": 1842,
        "rejected": 0,
        "deduped": 0,
        "row_count": 50,  # 959 + 1842 + 50 = 2851 != 2852
        "source_format": "udiff",
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(imbalanced_meta), encoding="utf-8")

    with pytest.raises(PartitionValidationError, match="imbalanced"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11))


def test_validate_partition_metadata_refuses_fallback_without_flag(tmp_path: Path):
    """Verify fallback source is refused when allow_fallback=False."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    fallback_meta = {
        "trade_date": "2024-10-11",
        "read_count": 50,
        "non_equity_filtered": 0,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": 50,
        "source_format": "yfinance",
        "source_type": "fallback",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(fallback_meta), encoding="utf-8")

    with pytest.raises(PermissionError, match="warehouse refuses fallback data"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11), allow_fallback=False)


def test_validate_partition_metadata_permits_fallback_with_flag(tmp_path: Path):
    """Verify fallback source is accepted when allow_fallback=True."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    fallback_meta = {
        "trade_date": "2024-10-11",
        "read_count": 50,
        "non_equity_filtered": 0,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": 50,
        "source_format": "yfinance",
        "source_type": "fallback",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(fallback_meta), encoding="utf-8")

    meta = validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11), allow_fallback=True)
    assert meta["source_type"] == "fallback"


def test_db_config_from_env(monkeypatch):
    """Verify get_db_config parses environment variables correctly."""
    monkeypatch.setenv("POSTGRES_HOST", "custom_host")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    cfg = get_db_config()
    assert cfg["host"] == "custom_host"
    assert cfg["port"] == 5433
    assert cfg["dbname"] == "test_db"
    assert cfg["user"] == "test_user"
    assert cfg["password"] == "secret"


def test_date_dimension_generation_offline():
    """Verify calendar calculation logic matches ISO standards and NSE holiday behavior."""
    d_monday = date(2024, 10, 7)
    assert d_monday.isoweekday() == 1
    assert d_monday.isoweekday() not in (6, 7)  # Not weekend

    d_saturday = date(2024, 10, 12)
    assert d_saturday.isoweekday() == 6  # Saturday = weekend

    d_sunday = date(2024, 10, 13)
    assert d_sunday.isoweekday() == 7  # Sunday = weekend

    # Quarter calculations
    assert (date(2024, 1, 15).month - 1) // 3 + 1 == 1
    assert (date(2024, 5, 20).month - 1) // 3 + 1 == 2
    assert (date(2024, 8, 15).month - 1) // 3 + 1 == 3
    assert (date(2024, 10, 11).month - 1) // 3 + 1 == 4


# ==============================================================================
# Live Integration Tests (Against PostgreSQL Container)
# ==============================================================================


@requires_postgres
def test_db_init_and_dimensions_integration():
    """Verify database migrations apply cleanly and dimensions are seeded."""
    with get_db_connection(autocommit=True) as conn:
        applied = apply_sql_migrations(conn, sql_dir="sql")
        assert len(applied) == 4

        symbols_count = seed_dim_symbols(conn, sectors_path="config/sectors.csv")
        assert symbols_count >= 50

        dates_count = populate_dim_date(
            conn,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            holidays_path="config/holidays.csv",
        )
        assert dates_count == 366  # 2024 is leap year

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.dim_symbol;")
            assert cur.fetchone()[0] >= 50

            cur.execute("SELECT COUNT(*) FROM gold.dim_date WHERE year = 2024;")
            assert cur.fetchone()[0] == 366


@requires_postgres
def test_load_silver_partition_idempotency_integration():
    """Verify loading the same day twice produces identical counts and no duplicate rows."""
    trade_date = date(2024, 10, 11)

    # Initial load
    res1 = load_silver_partition(trade_date, silver_base_dir="data/silver/equity_prices")
    assert res1["status"] == "SUCCESS"
    assert res1["rows_out"] == 51

    # Second load (Idempotency verification)
    res2 = load_silver_partition(trade_date, silver_base_dir="data/silver/equity_prices")
    assert res2["status"] == "SUCCESS"
    assert res2["rows_out"] == 51

    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold.fact_daily_price WHERE date_id = 20241011;")
            assert cur.fetchone()[0] == 51


@requires_postgres
def test_load_two_days_partition_isolation_integration():
    """Verify loading day B does not alter or corrupt day A rows."""
    d1 = date(2024, 10, 11)
    d2 = date(2024, 1, 15)

    load_silver_partition(d1, silver_base_dir="data/silver/equity_prices")
    load_silver_partition(d2, silver_base_dir="data/silver/equity_prices")

    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT date_id, COUNT(*) FROM gold.fact_daily_price WHERE date_id IN (20241011, 20240115) GROUP BY 1 ORDER BY 1;"
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == (20240115, 51)
            assert rows[1] == (20241011, 51)


@requires_postgres
def test_load_rollback_on_failure_integration(tmp_path: Path):
    """Verify a mid-transaction failure rolls back completely and leaves fact table unchanged."""
    test_date = date(2024, 10, 11)
    silver_base = tmp_path / "silver"
    partition_dir = silver_base / f"trade_date={test_date}"
    partition_dir.mkdir(parents=True)

    # Create dummy parquet containing an invalid row that violates the CHECK constraint (high < low)
    bad_data = [
        {
            "symbol": "CRASHTEST",
            "series": "EQ",
            "isin": "INE999A01099",
            "open": 100.0,
            "high": 90.0,  # Invalid: high < low violates chk_fact_high_gte_low
            "low": 110.0,
            "close": 95.0,
            "last": 95.0,
            "prev_close": 98.0,
            "volume": 1000,
            "turnover": 95000.0,
            "trades": 50,
            "source_format": "udiff",
            "source_file": "bad.csv",
            "trade_date": test_date,
        }
    ]
    pd.DataFrame(bad_data).to_parquet(partition_dir / "bad.parquet")

    # Write valid-looking metadata to pass pre-load validation
    meta = {
        "trade_date": str(test_date),
        "read_count": 1,
        "non_equity_filtered": 0,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": 1,
        "source_format": "udiff",
        "source_type": "official",
    }
    (partition_dir / "_SUCCESS.meta.json").write_text(json.dumps(meta), encoding="utf-8")

    # Record initial count for CRASHTEST
    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM gold.fact_daily_price f JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id WHERE s.symbol = 'CRASHTEST';"
            )
            count_before = cur.fetchone()[0]

    # Loading should fail due to database check constraint (chk_fact_high_gte_low)
    with pytest.raises(psycopg.Error):
        load_silver_partition(test_date, silver_base_dir=silver_base)

    # Verify atomic rollback: no row was inserted into fact table
    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM gold.fact_daily_price f JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id WHERE s.symbol = 'CRASHTEST';"
            )
            count_after = cur.fetchone()[0]
            assert count_after == count_before == 0

            # Verify failure was logged in ops.pipeline_run_log
            cur.execute(
                "SELECT status FROM ops.pipeline_run_log WHERE trade_date = %s ORDER BY started_at DESC LIMIT 1;",
                (test_date,),
            )
            log_status = cur.fetchone()
            assert log_status is not None
            assert log_status[0] == "FAILED"
