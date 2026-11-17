-- ==============================================================================
-- examples.sql: Sample analytical queries demonstrating Gold window views
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Example 1: Last 10 trading session returns for a specific stock (e.g. RELIANCE)
-- Displays close, lagged close, return comparisons, and calendar days gap.
-- ------------------------------------------------------------------------------
SELECT
    symbol,
    trade_date,
    close,
    prev_trading_close,
    ROUND(return_vs_lag_close * 100.0, 2) AS return_vs_lag_pct,
    ROUND(return_vs_prev_close * 100.0, 2) AS return_vs_prev_pct,
    days_gap
FROM gold.vw_daily_returns
WHERE symbol = 'RELIANCE'
ORDER BY trade_date DESC
LIMIT 10;

-- ------------------------------------------------------------------------------
-- Example 2: Moving average trend and golden/death cross tracking (e.g. TCS)
-- Inspects 20-day vs 50-day moving averages once sufficient history is reached.
-- ------------------------------------------------------------------------------
SELECT
    symbol,
    trade_date,
    close,
    ma_20d,
    ma_50d,
    CASE
        WHEN ma_20d IS NOT NULL AND ma_50d IS NOT NULL AND ma_20d > ma_50d THEN 'BULLISH'
        WHEN ma_20d IS NOT NULL AND ma_50d IS NOT NULL AND ma_20d < ma_50d THEN 'BEARISH'
        ELSE 'INSUFFICIENT_HISTORY'
    END AS ma_trend
FROM gold.vw_moving_averages
WHERE symbol = 'TCS'
ORDER BY trade_date DESC
LIMIT 15;

-- ------------------------------------------------------------------------------
-- Example 3: 20-Day Rolling Price Volatility ranking across Nifty 50
-- Identifies highest-volatility and lowest-volatility stocks for the latest session.
-- ------------------------------------------------------------------------------
SELECT
    symbol,
    trade_date,
    close,
    daily_return,
    volatility_20d,
    ROUND(volatility_20d * SQRT(252.0) * 100.0, 2) AS annualized_volatility_pct
FROM gold.vw_volatility_20d
WHERE trade_date = (SELECT MAX(trade_date) FROM gold.vw_volatility_20d)
  AND volatility_20d IS NOT NULL
ORDER BY volatility_20d DESC;

-- ------------------------------------------------------------------------------
-- Example 4: Today's / Latest Session Top 5 Gainers and Top 5 Losers
-- ------------------------------------------------------------------------------
SELECT
    trade_date,
    mover_type,
    rank,
    symbol,
    close,
    prev_close,
    return_pct
FROM gold.vw_top_movers
WHERE trade_date = (SELECT MAX(trade_date) FROM gold.vw_top_movers)
ORDER BY mover_type, rank;

-- ------------------------------------------------------------------------------
-- Example 5: High-Conviction Volume Breakouts (Volume Spikes > 2x 20-day average)
-- Identifies unusual trading interest across the market.
-- ------------------------------------------------------------------------------
SELECT
    symbol,
    trade_date,
    volume,
    prev_20d_avg_volume,
    volume_multiple
FROM gold.vw_volume_spikes
WHERE trade_date >= (CURRENT_DATE - INTERVAL '3 months')
ORDER BY volume_multiple DESC
LIMIT 20;

-- ------------------------------------------------------------------------------
-- Example 6: Sectoral Performance for Latest Session
-- Compares average daily returns across market sectors.
-- ------------------------------------------------------------------------------
SELECT
    sector,
    trade_date,
    symbol_count,
    avg_return_pct
FROM gold.vw_sector_performance
WHERE trade_date = (SELECT MAX(trade_date) FROM gold.vw_sector_performance)
ORDER BY avg_return_pct DESC;

-- ------------------------------------------------------------------------------
-- Example 7: Suspected Corporate Actions Audit
-- Reveals symbols where file_prev_close diverges from previous loaded close or close moved >20%.
-- ------------------------------------------------------------------------------
SELECT
    symbol,
    company_name,
    trade_date,
    previous_loaded_close,
    file_prev_close,
    current_close,
    discrepancy_ratio,
    return_vs_lag_pct,
    return_vs_prev_close_pct
FROM gold.vw_suspected_corporate_actions
ORDER BY trade_date DESC;
