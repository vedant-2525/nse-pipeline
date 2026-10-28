-- ==============================================================================
-- 05_analytics_views.sql: Financial analytical window views in Gold schema
-- ==============================================================================
-- These views compute per-symbol financial indicators over chronological trading
-- rows using PostgreSQL native window functions. All views are idempotent and
-- safe to re-run (CREATE OR REPLACE VIEW).
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. gold.vw_daily_returns
-- Computes daily price returns using both:
-- (a) return_vs_lag_close: close vs preceding row's close (sensitive to splits/bonuses)
-- (b) return_vs_prev_close: close vs official bhavcopy prev_close (exchange-adjusted)
-- Includes days_gap (calendar days between successive trading rows) to detect missing dates.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_daily_returns AS
WITH base AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.close,
        f.prev_close,
        LAG(f.close, 1) OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS lag_close,
        LAG(d.date, 1) OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS prev_trade_date
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
)
SELECT
    symbol,
    trade_date,
    close,
    lag_close AS prev_trading_close,
    ROUND((close - lag_close) / NULLIF(lag_close, 0), 6) AS return_vs_lag_close,
    ROUND((close - prev_close) / NULLIF(prev_close, 0), 6) AS return_vs_prev_close,
    (trade_date - prev_trade_date) AS days_gap
FROM base;

-- ------------------------------------------------------------------------------
-- 2. gold.vw_moving_averages
-- Computes 20-day and 50-day rolling simple moving averages of closing prices.
-- To prevent misleading values during history ramp-up, returns NULL until the
-- window contains the full number of required trading rows (20 and 50 respectively).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_moving_averages AS
WITH base AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.close,
        ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS row_num,
        AVG(f.close) OVER (
            PARTITION BY f.symbol_id ORDER BY d.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_ma_20d,
        AVG(f.close) OVER (
            PARTITION BY f.symbol_id ORDER BY d.date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS rolling_ma_50d
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
)
SELECT
    symbol,
    trade_date,
    close,
    CASE WHEN row_num >= 20 THEN ROUND(rolling_ma_20d, 4) ELSE NULL END AS ma_20d,
    CASE WHEN row_num >= 50 THEN ROUND(rolling_ma_50d, 4) ELSE NULL END AS ma_50d
FROM base;

-- ------------------------------------------------------------------------------
-- 3. gold.vw_volatility_20d
-- Computes 20-day rolling annualized/sample volatility (sample standard deviation)
-- of daily returns. Returns NULL until at least 20 daily returns exist.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_volatility_20d AS
WITH daily_ret AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.close,
        ROUND((f.close - f.prev_close) / NULLIF(f.prev_close, 0), 6) AS daily_return,
        ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS row_num,
        STDDEV_SAMP((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) OVER (
            PARTITION BY f.symbol_id ORDER BY d.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_20d
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
)
SELECT
    symbol,
    trade_date,
    close,
    daily_return,
    CASE WHEN row_num >= 20 THEN ROUND(rolling_vol_20d, 6) ELSE NULL END AS volatility_20d
FROM daily_ret;

-- ------------------------------------------------------------------------------
-- 4. gold.vw_top_movers
-- Identifies the top 5 gainers and top 5 losers for each active trading day
-- ordered by percentage daily return.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_top_movers AS
WITH ranked AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.close,
        f.prev_close,
        ROUND(((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) * 100.0, 2) AS return_pct,
        ROW_NUMBER() OVER (
            PARTITION BY d.date
            ORDER BY ((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) DESC
        ) AS rank_gainers,
        ROW_NUMBER() OVER (
            PARTITION BY d.date
            ORDER BY ((f.close - f.prev_close) / NULLIF(f.prev_close, 0)) ASC
        ) AS rank_losers
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
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
FROM ranked
WHERE rank_gainers <= 5 OR rank_losers <= 5
ORDER BY trade_date, mover_type, rank;

-- ------------------------------------------------------------------------------
-- 5. gold.vw_volume_spikes
-- Flags trading sessions where daily traded volume exceeds 2x the 20-day moving
-- average volume of PRECEDING rows.
-- Note: Excludes the current row from its own average (ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
-- to avoid biasing the baseline threshold with anomalous spike volume.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_volume_spikes AS
WITH volume_stats AS (
    SELECT
        s.symbol,
        d.date AS trade_date,
        f.volume,
        ROW_NUMBER() OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS row_num,
        AVG(f.volume) OVER (
            PARTITION BY f.symbol_id ORDER BY d.date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS prev_20d_avg_volume
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
)
SELECT
    symbol,
    trade_date,
    volume,
    ROUND(prev_20d_avg_volume, 2) AS prev_20d_avg_volume,
    ROUND((volume / NULLIF(prev_20d_avg_volume, 0)), 2) AS volume_multiple
FROM volume_stats
WHERE row_num > 20
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
-- Detects corporate action ex-dates (stock splits, bonus share issues, capital reductions)
-- by comparing the official exchange adjusted prev_close against the unadjusted LAG(close).
--
-- Why tolerance > 0.05 (5%):
-- Under normal continuous trading, official prev_close equals yesterday's close exactly (0.00% difference).
-- On ex-dates for stock splits (e.g. 2:1 = 50% drop, 5:1 = 80% drop, 10:1 = 90% drop) or bonus issues
-- (e.g. 1:1 = 50% drop), the exchange adjusts prev_close downward to prevent artificial price shocks.
-- A 5% relative threshold cleanly separates true corporate actions from normal zero-difference rows.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_suspected_corporate_actions AS
WITH price_lags AS (
    SELECT
        s.symbol,
        s.company_name,
        d.date AS trade_date,
        f.close,
        f.prev_close,
        LAG(f.close, 1) OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS lag_close,
        LAG(d.date, 1) OVER (PARTITION BY f.symbol_id ORDER BY d.date) AS prev_trade_date
    FROM gold.fact_daily_price f
    JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
    JOIN gold.dim_date d ON f.date_id = d.date_id
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
FROM price_lags
WHERE lag_close IS NOT NULL
  AND ABS(prev_close - lag_close) / NULLIF(lag_close, 0) > 0.05
ORDER BY trade_date, discrepancy_ratio DESC;
