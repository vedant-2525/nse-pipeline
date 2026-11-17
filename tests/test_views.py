"""Integration tests for Phase 4 PostgreSQL analytical window views.

All tests execute inside the isolated 'nse_warehouse_test' database managed by the
session fixture 'postgres_test_db', ensuring zero impact on real warehouse data.
Tests skip cleanly when Docker/PostgreSQL is unreachable.
"""

import statistics
from datetime import date
from decimal import Decimal

import psycopg
import pytest

from nse_pipeline.load.db import get_db_connection, is_postgres_reachable

requires_postgres = pytest.mark.skipif(
    not is_postgres_reachable(),
    reason="PostgreSQL warehouse container is not reachable (start with docker compose up -d)",
)


def _setup_test_symbol(conn: psycopg.Connection, symbol: str, sector: str = "Technology") -> int:
    """Inserts a test symbol into gold.dim_symbol and returns its symbol_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.dim_symbol (symbol, company_name, sector)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET sector = EXCLUDED.sector
            RETURNING symbol_id;
            """,
            (symbol, f"{symbol} Test Corp", sector),
        )
        return cur.fetchone()[0]


def _get_consecutive_trading_days(
    conn: psycopg.Connection, start_date: date, count: int
) -> list[date]:
    """Retrieves count consecutive trading days from gold.dim_date starting at or after start_date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT date
            FROM gold.dim_date
            WHERE date >= %s AND is_trading_day = TRUE
            ORDER BY date
            LIMIT %s;
            """,
            (start_date, count),
        )
        return [r[0] for r in cur.fetchall()]


@requires_postgres
def test_view_daily_returns(postgres_test_db):
    """Verify gold.vw_daily_returns calculates LAG close, relative returns, and days_gap.

    Hand-calculated expected numbers:
    Day 1 (2024-01-01):
      close = 100.00, prev_close = 95.00
      prev_trading_close = NULL, return_vs_lag = NULL, days_gap = NULL
      return_vs_prev_close = (100.00 - 95.00) / 95.00 = 5 / 95 = 0.052632
    Day 2 (2024-01-02):
      close = 110.00, prev_close = 100.00
      prev_trading_close = 100.00, return_vs_lag = (110.00 - 100.00) / 100.00 = 0.100000 (+10.0%)
      return_vs_prev_close = (110.00 - 100.00) / 100.00 = 0.100000 (+10.0%)
      days_gap = 1
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "RETTEST")

        with conn.cursor() as cur:
            # Clean up prior test data
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            # Day 1
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240101, 96.0, 101.0, 95.0, 100.0, 100.0, 95.0, 1000, 100000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )
            # Day 2
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240102, 102.0, 112.0, 100.0, 110.0, 110.0, 100.0, 1200, 132000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )

            cur.execute(
                """
                SELECT trade_date, close, prev_trading_close, return_vs_lag_close, return_vs_prev_close, days_gap
                FROM gold.vw_daily_returns
                WHERE symbol = 'RETTEST'
                ORDER BY trade_date;
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 2

            # Day 1 Assertions
            r1 = rows[0]
            assert r1[0] == date(2024, 1, 1)
            assert r1[1] == Decimal("100.0000")
            assert r1[2] is None  # No preceding row
            assert r1[3] is None  # No lag return
            assert float(r1[4]) == pytest.approx(0.052632, abs=1e-5)
            assert r1[5] is None  # No days gap

            # Day 2 Assertions
            r2 = rows[1]
            assert r2[0] == date(2024, 1, 2)
            assert r2[1] == Decimal("110.0000")
            assert r2[2] == Decimal("100.0000")
            assert float(r2[3]) == pytest.approx(0.100000, abs=1e-5)
            assert float(r2[4]) == pytest.approx(0.100000, abs=1e-5)
            assert r2[5] == 1  # 1 calendar day gap


@requires_postgres
def test_view_returns_and_corporate_action_gap_isolation(postgres_test_db):
    """Verify returns and corporate actions are NULL/omitted across an intentional trading day gap.

    Deliberate gap: Insert Day 1 (2024-01-01) and Day 3 (2024-01-03), skipping Day 2 (2024-01-02).
    Because Day 2 is a trading day in gold.dim_date, Day 3 is NOT consecutive to Day 1.
    - Day 3 return_vs_lag_close must be NULL.
    - Day 3 must NOT be flagged in vw_suspected_corporate_actions even if price drops 50%.
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "GAPTEST")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            # Day 1: 2024-01-01
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240101, 100.0, 105.0, 95.0, 100.0, 100.0, 100.0, 1000, 100000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )
            # Day 3: 2024-01-03 (Deliberate gap: 2024-01-02 missing)
            # Price halved from 100 to 50, but across non-adjacent trading days
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240103, 50.0, 55.0, 48.0, 50.0, 50.0, 50.0, 2000, 100000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )

            # Assert return_vs_lag_close is NULL across the gap
            cur.execute(
                """
                SELECT trade_date, prev_trading_close, return_vs_lag_close, days_gap
                FROM gold.vw_daily_returns
                WHERE symbol = 'GAPTEST'
                ORDER BY trade_date;
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            r_gap = rows[1]
            assert r_gap[0] == date(2024, 1, 3)
            assert r_gap[1] is None, "prev_trading_close must be NULL across gap"
            assert r_gap[2] is None, "return_vs_lag_close must be NULL across gap"
            assert r_gap[3] is None, "days_gap must be NULL across gap"

            # Assert vw_suspected_corporate_actions does NOT flag across the gap
            cur.execute(
                """
                SELECT * FROM gold.vw_suspected_corporate_actions
                WHERE symbol = 'GAPTEST';
                """
            )
            ca_rows = cur.fetchall()
            assert len(ca_rows) == 0, (
                "Non-consecutive trading days must not trigger corporate action flags"
            )


@requires_postgres
def test_view_moving_averages(postgres_test_db):
    """Verify gold.vw_moving_averages returns NULL before 20/50 rows and exact rolling averages.

    Hand-calculated expected numbers:
    - Days 1 to 19: close = 100.00 -> ma_20d is NULL
    - Day 20: close = 100.00 -> ma_20d = 20 * 100 / 20 = 100.0000
    - Day 21: close = 120.00 -> ma_20d = (19 * 100 + 120) / 20 = 2020 / 20 = 101.0000
    - ma_50d is NULL for all 21 days (fewer than 50 rows)
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "MATEST")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            # Query 21 consecutive trading days from dim_date
            trading_days = _get_consecutive_trading_days(conn, date(2024, 1, 1), 21)
            assert len(trading_days) == 21

            for i, cur_dt in enumerate(trading_days):
                date_id = int(cur_dt.strftime("%Y%m%d"))
                close_price = 120.0 if i == 20 else 100.0
                cur.execute(
                    """
                    INSERT INTO gold.fact_daily_price (
                        symbol_id, date_id, open, high, low, close, last, prev_close,
                        volume, turnover, source_format, source_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1000, 100000.0, 'udiff', 'test');
                    """,
                    (
                        sym_id,
                        date_id,
                        close_price,
                        close_price,
                        close_price,
                        close_price,
                        close_price,
                        100.0,
                    ),
                )

            cur.execute(
                """
                SELECT trade_date, close, ma_20d, ma_50d
                FROM gold.vw_moving_averages
                WHERE symbol = 'MATEST'
                ORDER BY trade_date;
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 21

            # Rows 0-18 (Days 1 to 19): ma_20d MUST be NULL
            for r in rows[:19]:
                assert r[2] is None, f"Expected NULL ma_20d for day {r[0]}"
                assert r[3] is None, f"Expected NULL ma_50d for day {r[0]}"

            # Row 19 (Day 20): exactly 100.0000
            assert float(rows[19][2]) == pytest.approx(100.0000, abs=1e-4)
            assert rows[19][3] is None

            # Row 20 (Day 21): exactly 101.0000
            assert float(rows[20][2]) == pytest.approx(101.0000, abs=1e-4)
            assert rows[20][3] is None


@requires_postgres
def test_view_volatility_20d_hand_computed(postgres_test_db):
    """Verify gold.vw_volatility_20d computes exact sample standard deviation of 20 consecutive returns.

    Hand-calculated arithmetic:
    21 consecutive trading days:
    - Day 0: close = 100.00
    - Days 1 to 20: alternating returns r_i = 0.01 * (-1)^i
      r = [0.01, -0.01, 0.01, -0.01, ..., -0.01] (10 values of +0.01, 10 values of -0.01)
      Sum = 0.00, Mean = 0.00
      Sum of squared deviations = 20 * (0.01)^2 = 20 * 0.0001 = 0.0020
      Sample variance (N - 1 = 19 degrees of freedom):
        s^2 = 0.0020 / 19 = 0.00010526315789...
      Sample standard deviation:
        s = sqrt(0.00010526315789) = 0.01025978... (approx 0.010260)
    - Days 1 to 20 (first 20 rows): volatility_20d is NULL because fewer than 21 consecutive days exist.
    - Day 21 (row 21): volatility_20d is exactly 0.010260.
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "VOLTEST")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            trading_days = _get_consecutive_trading_days(conn, date(2024, 1, 1), 21)
            assert len(trading_days) == 21

            # Day 0 base price
            base_price = 100.0
            cur_price = base_price
            returns_sequence = []

            for i, dt in enumerate(trading_days):
                date_id = int(dt.strftime("%Y%m%d"))
                if i == 0:
                    prev_c = cur_price
                else:
                    ret = 0.01 if (i % 2 == 1) else -0.01
                    returns_sequence.append(ret)
                    prev_c = cur_price
                    cur_price = round(prev_c * (1.0 + ret), 4)

                cur.execute(
                    """
                    INSERT INTO gold.fact_daily_price (
                        symbol_id, date_id, open, high, low, close, last, prev_close,
                        volume, turnover, source_format, source_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1000, 100000.0, 'udiff', 'test');
                    """,
                    (
                        sym_id,
                        date_id,
                        cur_price,
                        cur_price,
                        cur_price,
                        cur_price,
                        cur_price,
                        prev_c,
                    ),
                )

            # Independent Python calculation using statistics.stdev (sample standard deviation)
            expected_sample_sd = statistics.stdev(returns_sequence)

            cur.execute(
                """
                SELECT trade_date, close, daily_return, volatility_20d
                FROM gold.vw_volatility_20d
                WHERE symbol = 'VOLTEST'
                ORDER BY trade_date;
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 21

            # Rows 0 to 19 (Days 1 to 20): volatility_20d must be NULL (fewer than 21 rows)
            for r in rows[:20]:
                assert r[3] is None, f"Expected NULL volatility_20d at row {r[0]}, got {r[3]}"

            # Row 20 (Day 21): First non-NULL sample volatility
            r21 = rows[20]
            assert r21[3] is not None
            assert float(r21[3]) == pytest.approx(expected_sample_sd, abs=1e-5)


@requires_postgres
def test_view_volume_spikes(postgres_test_db):
    """Verify gold.vw_volume_spikes flags days with volume > 2x average of PREVIOUS 20 rows.

    Hand-calculated expected numbers:
    - Days 1 to 20: volume = 1,000 each.
      Average of previous 20 rows on Day 21 = (20 * 1,000) / 20 = 1,000.00.
    - Day 21: volume = 2,500.
      2,500 > 2.0 * 1,000 -> Spike detected with volume_multiple = 2,500 / 1,000 = 2.50.
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "VOLSPIKE")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            trading_days = _get_consecutive_trading_days(conn, date(2024, 1, 1), 21)
            assert len(trading_days) == 21

            for i, cur_dt in enumerate(trading_days):
                date_id = int(cur_dt.strftime("%Y%m%d"))
                vol = 2500 if i == 20 else 1000
                cur.execute(
                    """
                    INSERT INTO gold.fact_daily_price (
                        symbol_id, date_id, open, high, low, close, last, prev_close,
                        volume, turnover, source_format, source_type
                    ) VALUES (%s, %s, 100.0, 105.0, 95.0, 100.0, 100.0, 100.0, %s, 100000.0, 'udiff', 'test');
                    """,
                    (sym_id, date_id, vol),
                )

            cur.execute(
                """
                SELECT symbol, trade_date, volume, prev_20d_avg_volume, volume_multiple
                FROM gold.vw_volume_spikes
                WHERE symbol = 'VOLSPIKE';
                """
            )
            spikes = cur.fetchall()
            assert len(spikes) == 1
            sp = spikes[0]
            assert sp[0] == "VOLSPIKE"
            assert sp[1] == trading_days[20]
            assert sp[2] == 2500
            assert float(sp[3]) == pytest.approx(1000.00, abs=1e-2)
            assert float(sp[4]) == pytest.approx(2.50, abs=1e-2)


@requires_postgres
def test_view_suspected_corporate_actions(postgres_test_db):
    """Verify gold.vw_suspected_corporate_actions identifies price/prev_close divergence.

    Hand-calculated numbers:
    Day 1 (2024-01-01): close = 2,000.00, prev_close = 1,980.00
    Day 2 (2024-01-02):
      Previous loaded close (lag_close) = 2,000.00
      File prev_close = 1,000.00 (file adjusted base price)
      Current close = 1,020.00
      discrepancy_ratio = |1,000.00 - 2,000.00| / 2,000.00 = 1,000 / 2,000 = 0.5000 (50.0%)
      return_vs_lag_pct = (1,020 - 2,000) / 2,000 = -49.00%
      return_vs_prev_close_pct = (1,020 - 1,000) / 1,000 = +2.00%

    Because 0.5000 > 0.05 (tolerance threshold), this row is flagged as a suspected corporate action.
    """
    with get_db_connection(autocommit=True) as conn:
        sym_id = _setup_test_symbol(conn, "SPLITCORP")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.fact_daily_price WHERE symbol_id = %s;", (sym_id,))

            # Day 1: normal trading day before price drop
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240101, 1990.0, 2010.0, 1980.0, 2000.0, 2000.0, 1980.0, 50000, 100000000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )
            # Day 2: Price drop session
            cur.execute(
                """
                INSERT INTO gold.fact_daily_price (
                    symbol_id, date_id, open, high, low, close, last, prev_close,
                    volume, turnover, source_format, source_type
                ) VALUES (%s, 20240102, 1005.0, 1030.0, 995.0, 1020.0, 1020.0, 1000.0, 90000, 91800000.0, 'udiff', 'test');
                """,
                (sym_id,),
            )

            cur.execute(
                """
                SELECT symbol, trade_date, previous_loaded_close, file_prev_close,
                       current_close, discrepancy_ratio, return_vs_lag_pct, return_vs_prev_close_pct
                FROM gold.vw_suspected_corporate_actions
                WHERE symbol = 'SPLITCORP';
                """
            )
            rows = cur.fetchall()
            assert len(rows) == 1
            ca = rows[0]
            assert ca[0] == "SPLITCORP"
            assert ca[1] == date(2024, 1, 2)
            assert float(ca[2]) == pytest.approx(2000.00, abs=1e-2)  # lag_close
            assert float(ca[3]) == pytest.approx(1000.00, abs=1e-2)  # adjusted prev_close
            assert float(ca[4]) == pytest.approx(1020.00, abs=1e-2)  # current close
            assert float(ca[5]) == pytest.approx(0.5000, abs=1e-4)  # 50% discrepancy
            assert float(ca[6]) == pytest.approx(-49.00, abs=1e-2)  # fake crash
            assert float(ca[7]) == pytest.approx(2.00, abs=1e-2)  # actual gain
