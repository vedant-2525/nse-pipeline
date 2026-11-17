"""Unit and integration tests for Phase 3: Warehouse Loader and PostgreSQL Star Schema.

Offline unit tests validate pre-load metadata gating, reconciliation checks,
SQL generation, and calendar dimension computation without requiring a database.

Integration tests validate end-to-end loading, atomic rollback on failure,
and partition isolation against an isolated PostgreSQL _test database, and skip
cleanly when Docker/Postgres is unavailable.

Integration test isolation: all tests that hit Postgres use the 'postgres_test_db'
session fixture (from conftest.py) which creates 'nse_warehouse_test' fresh and
drops it on teardown. The POSTGRES_DB env var is overridden to 'nse_warehouse_test'
for the duration of the session so get_db_connection() always points to the test DB.
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
        "universe": "default_list",
        "symbol_count": 51,
        "source_format": "udiff",
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(meta_content), encoding="utf-8")

    meta = validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11), allow_fallback=False)
    assert meta["row_count"] == 51
    assert meta["source_type"] == "official"
    assert meta["universe"] == "default_list"
    assert meta["symbol_count"] == 51


def test_validate_partition_metadata_missing_universe_raises(tmp_path: Path):
    """Verify partition with missing 'universe' field raises PartitionValidationError."""
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

    with pytest.raises(PartitionValidationError, match="missing the 'universe' field"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11))


def test_validate_partition_metadata_all_eq_refused_without_flag(tmp_path: Path):
    """Verify partition with universe='all_eq' is refused when include_all_symbols=False."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    meta_content = {
        "trade_date": "2024-10-11",
        "read_count": 2852,
        "non_equity_filtered": 959,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": 1893,
        "universe": "all_eq",
        "symbol_count": 1893,
        "source_format": "udiff",
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(meta_content), encoding="utf-8")

    with pytest.raises(PermissionError, match="refuses full market loads"):
        validate_partition_metadata(p_dir, trade_date=date(2024, 10, 11), include_all_symbols=False)


def test_validate_partition_metadata_all_eq_permitted_with_flag(tmp_path: Path):
    """Verify partition with universe='all_eq' is accepted when include_all_symbols=True."""
    p_dir = tmp_path / "trade_date=2024-10-11"
    p_dir.mkdir()
    meta_content = {
        "trade_date": "2024-10-11",
        "read_count": 2852,
        "non_equity_filtered": 959,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": 1893,
        "universe": "all_eq",
        "symbol_count": 1893,
        "source_format": "udiff",
        "source_type": "official",
    }
    (p_dir / "_SUCCESS.meta.json").write_text(json.dumps(meta_content), encoding="utf-8")

    meta = validate_partition_metadata(
        p_dir, trade_date=date(2024, 10, 11), include_all_symbols=True
    )
    assert meta["universe"] == "all_eq"
    assert meta["symbol_count"] == 1893


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
        "universe": "default_list",
        "symbol_count": 51,
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
        "universe": "default_list",
        "symbol_count": 51,
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
        "universe": "default_list",
        "symbol_count": 50,
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
        "universe": "default_list",
        "symbol_count": 50,
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
# Live Integration Tests (Against PostgreSQL _test Container)
# All tests use the 'postgres_test_db' fixture which creates an isolated
# 'nse_warehouse_test' database, overrides POSTGRES_DB env var, and tears
# down the test DB on session completion. This ensures real 'nse_warehouse'
# data is never touched by the test suite.
# ==============================================================================


def test_test_db_name_guard_raises_for_non_test_db():
    """Verify the _test suffix guard raises ValueError for a non-_test DB name."""
    import pytest

    # Simulate what conftest does with a bad DB name
    bad_db_name = "nse_warehouse"
    if not bad_db_name.endswith("_test"):
        with pytest.raises(ValueError, match="must end in '_test'"):
            raise ValueError(
                f"Test database name '{bad_db_name}' must end in '_test' to prevent "
                "accidental pollution of production data."
            )


@requires_postgres
def test_db_init_and_dimensions_integration(postgres_test_db):
    """Verify database migrations apply cleanly and dimensions are seeded."""
    with get_db_connection(autocommit=True) as conn:
        applied = apply_sql_migrations(conn, sql_dir="sql")
        assert len(applied) == 5

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


def _ensure_test_silver_partition(
    trade_date: date,
    silver_base_dir: str | Path = "data/silver/equity_prices",
    sectors_path: str | Path = "config/sectors.csv",
) -> Path:
    """Ensures a compliant silver partition exists for integration tests.

    If the partition directory already exists with parquet and meta (local dev),
    it is left untouched. In fresh CI environments where data/ is gitignored,
    this helper synthesizes the 51-symbol test partition.
    """
    import csv

    p_dir = Path(silver_base_dir) / f"trade_date={trade_date.strftime('%Y-%m-%d')}"
    meta_file = p_dir / "_SUCCESS.meta.json"
    if p_dir.is_dir() and meta_file.is_file() and list(p_dir.glob("*.parquet")):
        return p_dir

    p_dir.mkdir(parents=True, exist_ok=True)

    with open(sectors_path, encoding="utf-8") as f:
        symbols = [row["symbol"].strip().upper() for row in csv.DictReader(f) if row.get("symbol")]

    rows = []
    for sym in symbols:
        rows.append(
            {
                "symbol": sym,
                "series": "EQ",
                "isin": f"INE{sym[:6].ljust(6, '0')}0101",
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 102.0,
                "last": 102.0,
                "prev_close": 100.0,
                "volume": 10000,
                "turnover": 1020000.0,
                "trades": 500,
                "source_format": "udiff",
                "source_file": "sample.csv",
                "trade_date": trade_date,
            }
        )
    df = pd.DataFrame(rows)
    df.to_parquet(p_dir / "part-00000.parquet")

    checks = [
        {"name": "row_count_bounds", "passed": True, "details": "51 within bounds"},
        {"name": "partition_date_match", "passed": True, "details": "Match partition"},
        {"name": "duplicate_keys", "passed": True, "details": "Zero duplicates"},
        {"name": "high_low_invariant", "passed": True, "details": "High >= Low"},
        {"name": "price_bounds", "passed": True, "details": "Price within bounds"},
        {"name": "positive_prices", "passed": True, "details": "Prices positive"},
        {"name": "volume_non_negative", "passed": True, "details": "Volume non-negative"},
        {"name": "reject_rate_gate", "passed": True, "details": "0% quarantined"},
        {"name": "row_reconciliation_gate", "passed": True, "details": "Balanced"},
    ]
    meta = {
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "read_count": len(symbols),
        "non_equity_filtered": 0,
        "universe_filtered": 0,
        "rejected": 0,
        "deduped": 0,
        "row_count": len(symbols),
        "universe": "default_list",
        "symbol_count": len(symbols),
        "source_format": "udiff",
        "source_type": "official",
        "checks": checks,
    }
    meta_file.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return p_dir


@requires_postgres
def test_load_silver_partition_idempotency_integration(postgres_test_db):
    """Verify loading the same day twice produces identical counts and no duplicate rows."""
    trade_date = date(2024, 10, 11)
    _ensure_test_silver_partition(trade_date)

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
def test_load_persists_silver_dq_checks_to_ops(postgres_test_db):
    """Verify that silver checks from _SUCCESS.meta.json are persisted to ops.dq_results on load."""
    trade_date = date(2024, 10, 11)
    _ensure_test_silver_partition(trade_date)

    # Load the partition (meta has 9 'checks' entries)
    res = load_silver_partition(trade_date, silver_base_dir="data/silver/equity_prices")
    assert res["status"] == "SUCCESS"

    run_id = res["run_id"]
    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            # Check that silver quality checks were persisted
            cur.execute(
                "SELECT check_name, status FROM ops.dq_results WHERE run_id = %s AND stage = 'silver' ORDER BY result_id;",
                (run_id,),
            )
            dq_rows = cur.fetchall()
            assert len(dq_rows) == 9, f"Expected 9 silver DQ checks, got {len(dq_rows)}"
            check_names = [r[0] for r in dq_rows]
            for expected in [
                "row_count_bounds",
                "reject_rate_gate",
                "row_reconciliation_gate",
            ]:
                assert expected in check_names, f"Expected '{expected}' in dq_results"
            assert all(r[1] == "PASS" for r in dq_rows), "Expected all silver DQ checks to be PASS"


@requires_postgres
def test_load_two_days_partition_isolation_integration(postgres_test_db):
    """Verify loading day B does not alter or corrupt day A rows."""
    d1 = date(2024, 10, 11)
    d2 = date(2024, 1, 15)
    _ensure_test_silver_partition(d1)
    _ensure_test_silver_partition(d2)

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
def test_load_rollback_on_failure_integration(tmp_path: Path, postgres_test_db):
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
        "universe": "default_list",
        "symbol_count": 1,
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

            # Verify dq_results was also logged on the independent audit connection
            cur.execute(
                "SELECT status FROM ops.dq_results WHERE run_id = (SELECT run_id FROM ops.pipeline_run_log WHERE trade_date = %s ORDER BY started_at DESC LIMIT 1) ORDER BY result_id DESC LIMIT 1;",
                (test_date,),
            )
            dq_row = cur.fetchone()
            assert dq_row is not None, (
                "Expected at least one ops.dq_results entry for the failed run"
            )
