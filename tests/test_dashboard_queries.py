"""Integration tests for Streamlit dashboard queries against isolated test DB."""

from datetime import date

import pytest

from dashboard.queries import (
    fetch_available_symbols,
    fetch_date_range,
    fetch_market_movers,
    fetch_recent_dq_failures,
    fetch_recent_pipeline_runs,
    fetch_sector_performance,
    fetch_stock_history,
    fetch_suspected_corporate_actions,
    fetch_warehouse_summary,
)
from nse_pipeline.load.db import get_db_connection, is_postgres_reachable

requires_postgres = pytest.mark.skipif(
    not is_postgres_reachable(),
    reason="PostgreSQL container is not reachable",
)


@requires_postgres
def test_dashboard_queries_empty_db(postgres_test_db):
    """Verify all dashboard queries execute gracefully against an empty test DB without raising exceptions."""
    summary = fetch_warehouse_summary()
    assert summary["is_reachable"] is True
    assert summary["latest_date"] is None
    assert summary["total_symbols"] == 0

    symbols = fetch_available_symbols()
    assert symbols == []

    min_dt, max_dt = fetch_date_range()
    assert min_dt is None
    assert max_dt is None

    df_hist = fetch_stock_history("RELIANCE")
    assert df_hist.empty

    gainers, losers = fetch_market_movers(date(2024, 10, 15))
    assert gainers.empty
    assert losers.empty

    df_sec = fetch_sector_performance()
    assert df_sec.empty

    df_runs = fetch_recent_pipeline_runs()
    assert df_runs.empty

    df_fail = fetch_recent_dq_failures()
    assert df_fail.empty

    df_ca = fetch_suspected_corporate_actions()
    assert df_ca.empty


@requires_postgres
def test_dashboard_queries_with_populated_data(postgres_test_db):
    """Verify queries return proper DataFrame shapes when data is inserted in the test DB."""
    with get_db_connection(autocommit=True) as conn:
        with conn.cursor() as cur:
            # Insert test symbol
            cur.execute(
                """
                INSERT INTO gold.dim_symbol (symbol, company_name, sector)
                VALUES ('TESTCO', 'Test Company Ltd.', 'Technology')
                ON CONFLICT (symbol) DO NOTHING;
                """
            )
            cur.execute("SELECT symbol_id FROM gold.dim_symbol WHERE symbol = 'TESTCO';")
            sym_id = cur.fetchone()[0]

            # Insert fact row
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20241015, 100.0, 105.0, 95.0, 102.0, 102.0, 100.0, 10000, 1020000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )

    summary = fetch_warehouse_summary()
    assert summary["is_reachable"] is True
    assert summary["latest_date"] == date(2024, 10, 15)
    assert summary["total_symbols"] == 1
    assert summary["total_rows"] == 1

    symbols = fetch_available_symbols()
    assert "TESTCO" in symbols

    min_dt, max_dt = fetch_date_range()
    assert min_dt == date(2024, 10, 15)
    assert max_dt == date(2024, 10, 15)

    df_hist = fetch_stock_history("TESTCO")
    assert not df_hist.empty
    assert len(df_hist) == 1
    assert float(df_hist["close"].iloc[0]) == 102.0

    gainers, losers = fetch_market_movers(date(2024, 10, 15))
    assert not gainers.empty
    assert gainers["symbol"].iloc[0] == "TESTCO"
