"""Streamlit dashboard for NSE Market Data Pipeline.

Provides a read-only window onto the PostgreSQL warehouse:
- Overview (latest date, symbols & dates loaded, freshness, quality checks)
- Stock explorer (symbol & date picker, price chart with 20/50d MAs, volume chart, returns)
- Market movers (top 5 gainers and losers for chosen date)
- Sector performance (industry breakdown bar chart)
- Pipeline health (ops.pipeline_run_log and failed data quality checks)
- Suspected corporate actions (large price movements and prev_close discrepancies)
"""

import sys
from datetime import date
from pathlib import Path

# Ensure project root is in sys.path when running via 'streamlit run dashboard/app.py'
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st  # noqa: E402

from dashboard.queries import (  # noqa: E402
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

st.set_page_config(
    page_title="NSE Market Data Warehouse",
    page_icon="📈",
    layout="wide",
)

st.title("📈 NSE Market Data Warehouse Dashboard")
st.caption("Read-only analytical interface powered by PostgreSQL star-schema and window views.")

# Check DB connectivity
summary = fetch_warehouse_summary()

if not summary["is_reachable"]:
    st.error(
        "⚠️ PostgreSQL warehouse is unreachable. Please start the database container:\n\n"
        "```powershell\n"
        "docker compose up -d\n"
        "```"
    )
    st.stop()

if summary["latest_date"] is None:
    st.warning("Warehouse is currently empty. Run a backfill or daily load to populate data.")
    st.stop()

# ==============================================================================
# 1. Overview Section
# ==============================================================================
st.header("1. Warehouse Overview")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Latest Loaded Date", str(summary["latest_date"]))
with col2:
    st.metric("Symbols Loaded", summary["total_symbols"])
with col3:
    st.metric("Trading Sessions", summary["total_dates"])
with col4:
    st.metric("Total Fact Rows", summary["total_rows"])

# Freshness indicator
today = date.today()
if summary["latest_date"]:
    days_lag = (today - summary["latest_date"]).days
    if days_lag <= 3:
        st.success(
            f"Data is fresh. Latest trading session: {summary['latest_date']} ({days_lag} calendar days ago)."
        )
    else:
        st.info(
            f"Latest data in warehouse is from {summary['latest_date']} ({days_lag} calendar days ago)."
        )

st.divider()

# ==============================================================================
# 2. Stock Explorer
# ==============================================================================
st.header("2. Stock Explorer")

symbols = fetch_available_symbols()
min_date, max_date = fetch_date_range()

if symbols and min_date and max_date:
    exp_col1, exp_col2, exp_col3 = st.columns([2, 2, 2])
    with exp_col1:
        default_idx = symbols.index("RELIANCE") if "RELIANCE" in symbols else 0
        selected_symbol = st.selectbox("Select Symbol", symbols, index=default_idx)
    with exp_col2:
        start_dt = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
    with exp_col3:
        end_dt = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

    if start_dt and end_dt and start_dt <= end_dt:
        df_stock = fetch_stock_history(selected_symbol, start_dt, end_dt)

        if not df_stock.empty:
            # Price chart with moving averages
            st.subheader(f"{selected_symbol} - Close Price and Moving Averages")
            chart_df = df_stock.set_index("trade_date")[["close", "ma_20d", "ma_50d"]]
            st.line_chart(chart_df)

            # Volume chart
            st.subheader(f"{selected_symbol} - Trading Volume")
            vol_df = df_stock.set_index("trade_date")[["volume"]]
            st.bar_chart(vol_df)

            # Returns table
            st.subheader(f"{selected_symbol} - Daily Return History")
            display_cols = [
                "trade_date",
                "close",
                "ma_20d",
                "ma_50d",
                "volume",
                "return_vs_prev_close",
                "return_vs_lag_close",
            ]
            st.dataframe(
                df_stock[display_cols].sort_values("trade_date", ascending=False),
                use_container_width=True,
            )
        else:
            st.info(f"No price data found for {selected_symbol} in the selected date range.")
    else:
        st.error("Start date must be earlier than or equal to end date.")
else:
    st.info("No symbol data available to explore.")

st.divider()

# ==============================================================================
# 3. Market Movers
# ==============================================================================
st.header("3. Market Movers")

if min_date and max_date:
    mover_date = st.date_input(
        "Select Trading Session for Movers",
        max_date,
        min_value=min_date,
        max_value=max_date,
        key="movers_date",
    )
    gainers, losers = fetch_market_movers(mover_date)

    mov_col1, mov_col2 = st.columns(2)
    with mov_col1:
        st.subheader("Top 5 Gainers")
        if not gainers.empty:
            st.dataframe(gainers, use_container_width=True)
        else:
            st.info(f"No data available for {mover_date}.")
    with mov_col2:
        st.subheader("Top 5 Losers")
        if not losers.empty:
            st.dataframe(losers, use_container_width=True)
        else:
            st.info(f"No data available for {mover_date}.")

st.divider()

# ==============================================================================
# 4. Sector Performance
# ==============================================================================
st.header("4. Sector Performance")

if min_date and max_date:
    sector_date = st.date_input(
        "Select Trading Session for Sector Aggregation",
        max_date,
        min_value=min_date,
        max_value=max_date,
        key="sector_date",
    )
    df_sector = fetch_sector_performance(sector_date)

    if not df_sector.empty:
        st.bar_chart(df_sector.set_index("sector")["avg_return_pct"])
        st.dataframe(df_sector, use_container_width=True)
    else:
        st.info(f"No sector aggregation data available for {sector_date}.")

st.divider()

# ==============================================================================
# 5. Pipeline Health & Audit Logs
# ==============================================================================
st.header("5. Pipeline Health & Audit Logs")

hlth_col1, hlth_col2 = st.columns(2)
with hlth_col1:
    st.subheader("Recent Pipeline Runs (ops.pipeline_run_log)")
    df_runs = fetch_recent_pipeline_runs(10)
    if not df_runs.empty:
        st.dataframe(df_runs, use_container_width=True)
    else:
        st.info("No pipeline run records logged yet.")

with hlth_col2:
    st.subheader("Data Quality Failures (ops.dq_results)")
    df_failures = fetch_recent_dq_failures(10)
    if not df_failures.empty:
        st.dataframe(df_failures, use_container_width=True)
    else:
        st.success("No failed data quality checks recorded. All pipeline gates passed!")

st.divider()

# ==============================================================================
# 6. Suspected Corporate Actions
# ==============================================================================
st.header("6. Suspected Corporate Actions (gold.vw_suspected_corporate_actions)")
st.caption(
    "Trading days where the close moved more than 20% versus the previous loaded trading day, "
    "or where the file's prev_close differs from the previous loaded close by more than 5%. "
    "Possible causes include splits, bonus issues, genuine price crashes and missing sessions; "
    "verify before concluding."
)

df_actions = fetch_suspected_corporate_actions()
if not df_actions.empty:
    st.dataframe(df_actions, use_container_width=True)
else:
    st.info("No suspected corporate actions detected in current warehouse data.")
