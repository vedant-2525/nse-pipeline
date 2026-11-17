"""Queries module for Streamlit dashboard.

All database queries are centralized here, use parameterized SQL,
and return pandas DataFrames for easy rendering in Streamlit.
"""

from datetime import date
from typing import Any

import pandas as pd

from nse_pipeline.load.db import get_db_connection, is_postgres_reachable


def fetch_warehouse_summary() -> dict[str, Any]:
    """Retrieves overview metrics: latest date, count of symbols, count of dates."""
    if not is_postgres_reachable():
        return {
            "is_reachable": False,
            "latest_date": None,
            "total_symbols": 0,
            "total_dates": 0,
            "total_rows": 0,
        }

    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        MAX(d.date) AS latest_date,
                        COUNT(DISTINCT f.symbol_id) AS total_symbols,
                        COUNT(DISTINCT f.date_id) AS total_dates,
                        COUNT(*) AS total_rows
                    FROM gold.fact_daily_price f
                    JOIN gold.dim_date d ON f.date_id = d.date_id;
                    """
                )
                row = cur.fetchone()
                if not row or row[0] is None:
                    return {
                        "is_reachable": True,
                        "latest_date": None,
                        "total_symbols": 0,
                        "total_dates": 0,
                        "total_rows": 0,
                    }
                return {
                    "is_reachable": True,
                    "latest_date": row[0],
                    "total_symbols": row[1] or 0,
                    "total_dates": row[2] or 0,
                    "total_rows": row[3] or 0,
                }
    except Exception:
        return {
            "is_reachable": False,
            "latest_date": None,
            "total_symbols": 0,
            "total_dates": 0,
            "total_rows": 0,
        }


def fetch_available_symbols() -> list[str]:
    """Returns list of distinct active symbols loaded in gold.dim_symbol."""
    if not is_postgres_reachable():
        return []

    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT s.symbol
                    FROM gold.dim_symbol s
                    JOIN gold.fact_daily_price f ON s.symbol_id = f.symbol_id
                    ORDER BY s.symbol;
                    """
                )
                rows = cur.fetchall()
                return [r[0] for r in rows]
    except Exception:
        return []


def fetch_date_range() -> tuple[date | None, date | None]:
    """Returns (min_date, max_date) loaded in fact_daily_price."""
    if not is_postgres_reachable():
        return None, None

    try:
        with get_db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MIN(d.date), MAX(d.date)
                    FROM gold.fact_daily_price f
                    JOIN gold.dim_date d ON f.date_id = d.date_id;
                    """
                )
                row = cur.fetchone()
                if row and row[0] and row[1]:
                    return row[0], row[1]
                return None, None
    except Exception:
        return None, None


def fetch_stock_history(
    symbol: str, start_date: date | None = None, end_date: date | None = None
) -> pd.DataFrame:
    """Returns daily prices, moving averages, and volume for a symbol within date range."""
    if not is_postgres_reachable():
        return pd.DataFrame()

    query = """
        SELECT
            m.trade_date,
            m.close,
            m.ma_20d,
            m.ma_50d,
            f.volume,
            r.return_vs_prev_close,
            r.return_vs_lag_close
        FROM gold.vw_moving_averages m
        JOIN gold.dim_symbol s ON m.symbol = s.symbol
        JOIN gold.dim_date d ON m.trade_date = d.date
        JOIN gold.fact_daily_price f ON s.symbol_id = f.symbol_id AND d.date_id = f.date_id
        LEFT JOIN gold.vw_daily_returns r ON m.symbol = r.symbol AND m.trade_date = r.trade_date
        WHERE m.symbol = %s
    """
    params: list[Any] = [symbol]

    if start_date:
        query += " AND m.trade_date >= %s"
        params.append(start_date)
    if end_date:
        query += " AND m.trade_date <= %s"
        params.append(end_date)

    query += " ORDER BY m.trade_date;"

    try:
        with get_db_connection(autocommit=True) as conn:
            df = pd.read_sql_query(query, conn, params=params)
            return df
    except Exception:
        return pd.DataFrame()


def fetch_market_movers(trade_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns top 5 gainers and top 5 losers for a specific trading date."""
    if not is_postgres_reachable():
        return pd.DataFrame(), pd.DataFrame()

    query = """
        SELECT
            s.symbol,
            f.close,
            f.prev_close,
            ROUND(((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) * 100.0, 2) AS return_pct,
            f.volume
        FROM gold.fact_daily_price f
        JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
        JOIN gold.dim_date d ON f.date_id = d.date_id
        WHERE d.date = %s
        ORDER BY return_pct DESC;
    """
    try:
        with get_db_connection(autocommit=True) as conn:
            df = pd.read_sql_query(query, conn, params=[trade_date])
            if df.empty:
                return pd.DataFrame(), pd.DataFrame()
            gainers = df.head(5).reset_index(drop=True)
            losers = df.tail(5).sort_values("return_pct", ascending=True).reset_index(drop=True)
            return gainers, losers
    except Exception:
        return pd.DataFrame(), pd.DataFrame()


def fetch_sector_performance(trade_date: date | None = None) -> pd.DataFrame:
    """Returns sector performance. If trade_date is None, returns for latest loaded date."""
    if not is_postgres_reachable():
        return pd.DataFrame()

    if trade_date is None:
        query = """
            SELECT sector, trade_date, symbol_count, avg_return_pct
            FROM gold.vw_sector_performance
            WHERE trade_date = (SELECT MAX(trade_date) FROM gold.vw_sector_performance)
            ORDER BY avg_return_pct DESC;
        """
        params = None
    else:
        query = """
            SELECT sector, trade_date, symbol_count, avg_return_pct
            FROM gold.vw_sector_performance
            WHERE trade_date = %s
            ORDER BY avg_return_pct DESC;
        """
        params = [trade_date]

    try:
        with get_db_connection(autocommit=True) as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()


def fetch_recent_pipeline_runs(limit: int = 10) -> pd.DataFrame:
    """Returns recent pipeline execution records from ops.pipeline_run_log."""
    if not is_postgres_reachable():
        return pd.DataFrame()

    query = """
        SELECT
            run_id,
            stage,
            trade_date,
            status,
            rows_in,
            rows_out,
            error,
            started_at,
            finished_at
        FROM ops.pipeline_run_log
        ORDER BY started_at DESC
        LIMIT %s;
    """
    try:
        with get_db_connection(autocommit=True) as conn:
            df = pd.read_sql_query(query, conn, params=[limit])
            if "run_id" in df.columns:
                df["run_id"] = df["run_id"].astype(str)
            return df
    except Exception:
        return pd.DataFrame()


def fetch_recent_dq_failures(limit: int = 10) -> pd.DataFrame:
    """Returns recent data quality check failures from ops.dq_results."""
    if not is_postgres_reachable():
        return pd.DataFrame()

    query = """
        SELECT
            check_id,
            run_id,
            check_name,
            passed,
            details,
            checked_at
        FROM ops.dq_results
        WHERE passed = FALSE
        ORDER BY checked_at DESC
        LIMIT %s;
    """
    try:
        with get_db_connection(autocommit=True) as conn:
            df = pd.read_sql_query(query, conn, params=[limit])
            if "run_id" in df.columns:
                df["run_id"] = df["run_id"].astype(str)
            return df
    except Exception:
        return pd.DataFrame()


def fetch_suspected_corporate_actions() -> pd.DataFrame:
    """Returns flagged corporate actions from gold.vw_suspected_corporate_actions."""
    if not is_postgres_reachable():
        return pd.DataFrame()

    query = """
        SELECT
            symbol,
            company_name,
            trade_date,
            previous_loaded_close,
            file_prev_close,
            current_close,
            discrepancy_ratio,
            return_vs_lag_pct,
            return_vs_prev_close_pct,
            days_gap
        FROM gold.vw_suspected_corporate_actions
        ORDER BY trade_date DESC, discrepancy_ratio DESC;
    """
    try:
        with get_db_connection(autocommit=True) as conn:
            return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()
