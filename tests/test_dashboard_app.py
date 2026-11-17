"""Smoke and integration tests for the Streamlit dashboard app module.

These tests verify that:
1. The dashboard.app module can be imported without raising any exception.
2. AppTest renders when the DB is unreachable (shows friendly error).
3. AppTest renders all 6 sections with synthetic data and with empty data states.
4. AppTest renders correctly against the _test database when Postgres is reachable.
"""

import sys
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest


def test_dashboard_module_importable():
    """dashboard.app must import cleanly (no ModuleNotFoundError or syntax errors)."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("dashboard"):
            del sys.modules[mod]
    import dashboard.app  # noqa: F401


def test_dashboard_apptest_db_unreachable():
    """AppTest renders without exception when DB is unreachable; shows error message."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    with (
        patch("nse_pipeline.load.db.is_postgres_reachable", return_value=False),
        patch("dashboard.queries.is_postgres_reachable", return_value=False),
    ):
        at = AppTest.from_file("dashboard/app.py", default_timeout=20)
        at.run()
    assert not at.exception, f"AppTest raised: {at.exception}"
    assert len(at.error) > 0
    assert "PostgreSQL warehouse is unreachable" in at.error[0].value


def test_dashboard_apptest_all_sections_synthetic_data():
    """AppTest renders each of the six sections with no exception using synthetic data."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    mock_summary = {
        "is_reachable": True,
        "latest_date": date(2025, 3, 28),
        "total_symbols": 51,
        "total_dates": 75,
        "total_rows": 3825,
    }
    mock_symbols = ["RELIANCE", "TCS", "INFY"]
    mock_date_range = (date(2025, 1, 1), date(2025, 3, 28))
    mock_stock_df = pd.DataFrame(
        {
            "trade_date": [date(2025, 3, 27), date(2025, 3, 28)],
            "close": [1350.0, 1360.0],
            "ma_20d": [1340.0, 1342.0],
            "ma_50d": [1330.0, 1332.0],
            "volume": [1000000, 1200000],
            "return_vs_prev_close": [0.5, 0.74],
            "return_vs_lag_close": [0.5, 0.74],
        }
    )
    mock_gainers = pd.DataFrame({"symbol": ["INFY"], "close": [1800.0], "return_pct": [2.5]})
    mock_losers = pd.DataFrame({"symbol": ["TCS"], "close": [3900.0], "return_pct": [-1.2]})
    mock_sector = pd.DataFrame(
        {
            "sector": ["Information Technology"],
            "avg_return_pct": [0.65],
            "symbol_count": [5],
        }
    )
    mock_runs = pd.DataFrame(
        {
            "run_id": ["test-uuid-1"],
            "stage": ["load"],
            "trade_date": [date(2025, 3, 28)],
            "status": ["SUCCESS"],
            "rows_in": [51],
            "rows_out": [51],
            "error": [None],
            "started_at": ["2025-03-28 19:00:00"],
            "finished_at": ["2025-03-28 19:00:02"],
        }
    )
    mock_dq = pd.DataFrame(
        {
            "check_id": [1],
            "run_id": ["test-uuid-1"],
            "check_name": ["price_bounds"],
            "passed": [False],
            "details": ["synthetic DQ failure"],
            "checked_at": ["2025-03-28 19:00:01"],
        }
    )
    mock_ca = pd.DataFrame(
        {
            "symbol": ["RELIANCE"],
            "company_name": ["Reliance Industries Ltd"],
            "trade_date": [date(2024, 10, 28)],
            "previous_loaded_close": [2655.7],
            "file_prev_close": [2655.7],
            "current_close": [1334.35],
            "discrepancy_ratio": [1.0],
            "return_vs_lag_pct": [-49.75],
            "return_vs_prev_close_pct": [-49.75],
            "days_gap": [3],
        }
    )

    with (
        patch("dashboard.queries.fetch_warehouse_summary", return_value=mock_summary),
        patch("dashboard.queries.fetch_available_symbols", return_value=mock_symbols),
        patch("dashboard.queries.fetch_date_range", return_value=mock_date_range),
        patch("dashboard.queries.fetch_stock_history", return_value=mock_stock_df),
        patch(
            "dashboard.queries.fetch_market_movers",
            return_value=(mock_gainers, mock_losers),
        ),
        patch("dashboard.queries.fetch_sector_performance", return_value=mock_sector),
        patch("dashboard.queries.fetch_recent_pipeline_runs", return_value=mock_runs),
        patch("dashboard.queries.fetch_recent_dq_failures", return_value=mock_dq),
        patch("dashboard.queries.fetch_suspected_corporate_actions", return_value=mock_ca),
    ):
        at = AppTest.from_file("dashboard/app.py", default_timeout=20)
        at.run()

    assert not at.exception, f"AppTest raised: {at.exception}"
    header_titles = [h.value for h in at.header]
    assert "1. Warehouse Overview" in header_titles
    assert "2. Stock Explorer" in header_titles
    assert "3. Market Movers" in header_titles
    assert "4. Sector Performance" in header_titles
    assert "5. Pipeline Health & Audit Logs" in header_titles
    # Verify corporate action section is labeled 'Suspected'
    ca_header = [h for h in header_titles if "Suspected Corporate Actions" in h]
    assert len(ca_header) == 1, "Section 6 header missing"
    assert "Suspected Corporate Actions" in ca_header[0]


def test_dashboard_apptest_empty_data_states():
    """AppTest renders friendly messages when tables/queries return empty datasets."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    mock_summary = {
        "is_reachable": True,
        "latest_date": date(2025, 3, 28),
        "total_symbols": 51,
        "total_dates": 75,
        "total_rows": 3825,
    }

    with (
        patch("dashboard.queries.fetch_warehouse_summary", return_value=mock_summary),
        patch("dashboard.queries.fetch_available_symbols", return_value=[]),
        patch("dashboard.queries.fetch_date_range", return_value=(None, None)),
        patch(
            "dashboard.queries.fetch_market_movers",
            return_value=(pd.DataFrame(), pd.DataFrame()),
        ),
        patch(
            "dashboard.queries.fetch_sector_performance",
            return_value=pd.DataFrame(),
        ),
        patch(
            "dashboard.queries.fetch_recent_pipeline_runs",
            return_value=pd.DataFrame(),
        ),
        patch(
            "dashboard.queries.fetch_recent_dq_failures",
            return_value=pd.DataFrame(),
        ),
        patch(
            "dashboard.queries.fetch_suspected_corporate_actions",
            return_value=pd.DataFrame(),
        ),
    ):
        at = AppTest.from_file("dashboard/app.py", default_timeout=20)
        at.run()

    assert not at.exception, f"AppTest raised: {at.exception}"
    info_texts = [i.value for i in at.info]
    assert "No symbol data available to explore." in info_texts
    assert "No pipeline run records logged yet." in info_texts
    assert "No suspected corporate actions detected in current warehouse data." in info_texts
    success_texts = [s.value for s in at.success]
    assert any("No failed data quality checks recorded" in s for s in success_texts)


@pytest.mark.postgres_integration
def test_dashboard_apptest_with_test_db(postgres_test_db):
    """AppTest renders without exception against the test database."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    at = AppTest.from_file("dashboard/app.py", default_timeout=30)
    at.run()
    assert not at.exception, f"AppTest raised: {at.exception}"
