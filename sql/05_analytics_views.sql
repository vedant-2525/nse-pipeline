-- ==============================================================================
-- 05_analytics_views.sql: Financial analytical window views in Gold schema
-- ==============================================================================
-- These views compute per-symbol financial indicators over chronological trading
-- rows using PostgreSQL native window functions. All views are idempotent and
-- safe to re-run (CREATE OR REPLACE VIEW).
--
-- Sparse Data & Trading-Day Adjacency Architecture (F1 & F2):
-- Technical indicators (returns, moving averages, rolling volatility, volume spikes)
-- operate strictly over contiguous sequences of verified trading days defined in
-- gold.dim_date (where is_trading_day = TRUE).
--
-- "Insufficient history" definition:
-- Whenever a window requirement (e.g. 20 consecutive closes for MA-20, 21 consecutive
-- closes for 20 daily returns in Vol-20, or adjacent trading day for daily returns)
-- is broken—either by starting history or by a gap where a trading day is missing from
-- fact_daily_price—the rolling values evaluate strictly to NULL rather than computing
-- across discontinuous time gaps.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. gold.vw_daily_returns
-- Computes daily price returns using both:
-- (a) return_vs_lag_close: close vs preceding trading day's close (only if consecutive)
-- (b) return_vs_prev_close: close vs official bhavcopy prev_close (exchange-adjusted)
-- Includes days_gap (calendar days between successive trading rows).
-- When the preceding row is NOT the immediately preceding trading day,
-- prev_trading_close, return_vs_lag_close, and days_gap evaluate to NULL.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_daily_returns AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
base AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        td.trading_day_seq,
        f.close,
        f.prev_close,
        LAG(f.close, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS lag_close,
        LAG(d.date, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS prev_trade_date,
        LAG(td.trading_day_seq, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS prev_trading_day_seq
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
)
SELECT
    symbol,
    trade_date,
    close,
    CASE
        WHEN prev_trading_day_seq = trading_day_seq - 1 THEN lag_close
        ELSE NULL
    END AS prev_trading_close,
    CASE
        WHEN prev_trading_day_seq = trading_day_seq - 1 THEN ROUND((close - lag_close) / NULLIF(lag_close, 0), 6)
        ELSE NULL
    END AS return_vs_lag_close,
    ROUND((close - prev_close) / NULLIF(prev_close, 0), 6) AS return_vs_prev_close,
    CASE
        WHEN prev_trading_day_seq = trading_day_seq - 1 THEN (trade_date - prev_trade_date)
        ELSE NULL
    END AS days_gap
FROM base;

-- ------------------------------------------------------------------------------
-- 2. gold.vw_moving_averages
-- Computes 20-day and 50-day rolling simple moving averages of closing prices.
-- To prevent misleading values across data gaps or during history ramp-up,
-- returns NULL unless exactly 20 or 50 consecutive trading days are present.
-- Islands of consecutive trading days are partitioned using (trading_day_seq - row_num).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_moving_averages AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
islands AS (
    SELECT
        f.symbol_id,
        s.symbol,
        d.date AS trade_date,
        f.close,
        td.trading_day_seq,
        td.trading_day_seq - ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS island_id
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
),
windowed AS (
    SELECT
        symbol,
        trade_date,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol_id, island_id ORDER BY trading_day_seq) AS consecutive_days,
        AVG(close) OVER (
            PARTITION BY symbol_id, island_id ORDER BY trading_day_seq
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_ma_20d,
        AVG(close) OVER (
            PARTITION BY symbol_id, island_id ORDER BY trading_day_seq
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS rolling_ma_50d
    FROM islands
)
SELECT
    symbol,
    trade_date,
    close,
    CASE WHEN consecutive_days >= 20 THEN ROUND(rolling_ma_20d, 4) ELSE NULL END AS ma_20d,
    CASE WHEN consecutive_days >= 50 THEN ROUND(rolling_ma_50d, 4) ELSE NULL END AS ma_50d
FROM windowed;

-- ------------------------------------------------------------------------------
-- 3. gold.vw_volatility_20d
-- Computes 20-day rolling sample volatility (STDDEV_SAMP) over exactly 20 consecutive
-- daily returns. Because Day 1 has no daily return, 20 consecutive daily returns
-- require exactly 21 consecutive trading-day rows.
-- Returns NULL until at least 21 consecutive trading days exist in the current island.
-- Annualization factor: SQRT(252.0) applied downstream or in analytics.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_volatility_20d AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
islands AS (
    SELECT
        f.symbol_id,
        s.symbol,
        d.date AS trade_date,
        f.close,
        td.trading_day_seq,
        td.trading_day_seq - ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS island_id
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
),
returns_calc AS (
    SELECT
        symbol_id,
        symbol,
        trade_date,
        close,
        island_id,
        trading_day_seq,
        ROW_NUMBER() OVER (PARTITION BY symbol_id, island_id ORDER BY trading_day_seq) AS consecutive_days,
        (close - LAG(close, 1) OVER (PARTITION BY symbol_id, island_id ORDER BY trading_day_seq))
            / NULLIF(LAG(close, 1) OVER (PARTITION BY symbol_id, island_id ORDER BY trading_day_seq), 0) AS daily_ret
    FROM islands
),
windowed AS (
    SELECT
        symbol,
        trade_date,
        close,
        consecutive_days,
        ROUND(daily_ret, 6) AS daily_return,
        STDDEV_SAMP(daily_ret) OVER (
            PARTITION BY symbol_id, island_id ORDER BY trading_day_seq
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_20d
    FROM returns_calc
)
SELECT
    symbol,
    trade_date,
    close,
    daily_return,
    CASE WHEN consecutive_days >= 21 THEN ROUND(rolling_vol_20d, 6) ELSE NULL END AS volatility_20d
FROM windowed;

-- ------------------------------------------------------------------------------
-- 4. gold.vw_top_movers
-- Identifies top 5 gainers and top 5 losers for each active trading day
-- using consecutive trading-day returns (falling back to prev_close if first loaded day).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_top_movers AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
base AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.close,
        f.prev_close,
        td.trading_day_seq,
        LAG(f.close, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS lag_close,
        LAG(td.trading_day_seq, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS prev_seq
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
),
calc AS (
    SELECT
        symbol,
        trade_date,
        close,
        prev_close,
        ROUND(((close - prev_close) / NULLIF(prev_close, 0)) * 100.0, 2) AS return_pct,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date
            ORDER BY ((close - prev_close) / NULLIF(prev_close, 0)) DESC
        ) AS rank_gainers,
        ROW_NUMBER() OVER (
            PARTITION BY trade_date
            ORDER BY ((close - prev_close) / NULLIF(prev_close, 0)) ASC
        ) AS rank_losers
    FROM base
)
SELECT
    trade_date,
    CASE
        WHEN rank_gainers <= 5 THEN 'TOP_GAINER'
        WHEN rank_losers <= 5 THEN 'TOP_LOSER'
    END AS mover_type,
    CASE
        WHEN rank_gainers <= 5 THEN rank_gainers
        ELSE rank_losers
    END AS rank,
    symbol,
    close,
    prev_close,
    return_pct
FROM calc
WHERE rank_gainers <= 5 OR rank_losers <= 5
ORDER BY trade_date, mover_type, rank;

-- ------------------------------------------------------------------------------
-- 5. gold.vw_volume_spikes
-- Flags trading sessions where daily traded volume exceeds 2x the 20-day moving
-- average volume of PRECEDING consecutive trading days.
-- Excludes current row (ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING).
-- Requires at least 21 consecutive trading days in the island (20 preceding rows).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_volume_spikes AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
islands AS (
    SELECT
        f.symbol_id,
        s.symbol,
        d.date AS trade_date,
        f.volume,
        td.trading_day_seq,
        td.trading_day_seq - ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS island_id
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
),
windowed AS (
    SELECT
        symbol,
        trade_date,
        volume,
        ROW_NUMBER() OVER (PARTITION BY symbol_id, island_id ORDER BY trading_day_seq) AS consecutive_days,
        AVG(volume) OVER (
            PARTITION BY symbol_id, island_id ORDER BY trading_day_seq
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS prev_20d_avg_volume
    FROM islands
)
SELECT
    symbol,
    trade_date,
    volume,
    ROUND(prev_20d_avg_volume, 2) AS prev_20d_avg_volume,
    ROUND((volume / NULLIF(prev_20d_avg_volume, 0)), 2) AS volume_multiple
FROM windowed
WHERE consecutive_days >= 21
  AND volume > 2.0 * prev_20d_avg_volume
ORDER BY trade_date, volume_multiple DESC;

-- ------------------------------------------------------------------------------
-- 6. gold.vw_sector_performance
-- Aggregates daily market returns by industry sector (from gold.dim_symbol).
-- Symbols without a sector mapping are grouped as 'UNKNOWN'.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_sector_performance AS
SELECT
    COALESCE(s.sector, 'UNKNOWN') AS sector,
    d.date AS trade_date,
    COUNT(f.symbol_id) AS symbol_count,
    ROUND(AVG((f.close - f.prev_close) / NULLIF(f.prev_close, 0)), 6) AS avg_daily_return,
    ROUND(AVG((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) * 100.0, 4) AS avg_return_pct
FROM gold.fact_daily_price f
JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
JOIN gold.dim_date d ON f.date_id = d.date_id
GROUP BY COALESCE(s.sector, 'UNKNOWN'), d.date
ORDER BY trade_date, avg_daily_return DESC;

-- ------------------------------------------------------------------------------
-- 7. gold.vw_suspected_corporate_actions
-- Detects corporate action ex-dates (stock splits, bonus share issues, capital reductions).
--
-- Logic against real measured evidence (E1):
-- 1. Compares strictly CONSECUTIVE trading days (prev_trading_day_seq = trading_day_seq - 1).
--    This completely eliminates false positives caused by calendar/loading gaps (e.g. 270-day gap).
-- 2. On ex-dates:
--    - When exchange adjusts prev_close: |prev_close(t) - close(t-1)| / close(t-1) > 0.05 (5%).
--    - When exchange leaves prev_close unadjusted (measured in RELIANCE on 2024-10-28 1:1 bonus where
--      close dropped from 2655.70 to 1334.35 while prev_close remained unadjusted at 2655.70):
--      |close(t) - close(t-1)| / close(t-1) > 0.20 (20% price drop/gain).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_suspected_corporate_actions AS
WITH trading_days AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS trading_day_seq
    FROM gold.dim_date
    WHERE is_trading_day = TRUE
),
base AS (
    SELECT
        s.symbol,
        s.company_name,
        d.date AS trade_date,
        td.trading_day_seq,
        f.close,
        f.prev_close,
        LAG(f.close, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS lag_close,
        LAG(d.date, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS prev_trade_date,
        LAG(td.trading_day_seq, 1) OVER (PARTITION BY f.symbol_id ORDER BY td.trading_day_seq) AS prev_trading_day_seq
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
    JOIN trading_days td ON d.date = td.date
)
SELECT
    symbol,
    company_name,
    trade_date,
    lag_close AS unadjusted_yesterday_close,
    prev_close AS exchange_adjusted_prev_close,
    close AS current_close,
    ROUND(ABS(prev_close - lag_close) / NULLIF(lag_close, 0), 4) AS discrepancy_ratio,
    ROUND(((close - lag_close) / NULLIF(lag_close, 0)) * 100.0, 2) AS return_vs_lag_pct,
    ROUND(((close - prev_close) / NULLIF(prev_close, 0)) * 100.0, 2) AS return_vs_prev_close_pct,
    (trade_date - prev_trade_date) AS days_gap
FROM base
WHERE prev_trading_day_seq = trading_day_seq - 1
  AND lag_close IS NOT NULL
  AND (
      ABS(prev_close - lag_close) / NULLIF(lag_close, 0) > 0.05
      OR ABS(close - lag_close) / NULLIF(lag_close, 0) > 0.20
  )
ORDER BY trade_date, discrepancy_ratio DESC;
