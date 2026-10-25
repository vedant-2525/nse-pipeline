-- ==============================================================================
-- 03_facts.sql: Daily price fact table in Gold star schema
-- ==============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_daily_price (
    symbol_id INT NOT NULL REFERENCES gold.dim_symbol(symbol_id),
    date_id INT NOT NULL REFERENCES gold.dim_date(date_id),
    open NUMERIC(18, 4) NOT NULL,
    high NUMERIC(18, 4) NOT NULL,
    low NUMERIC(18, 4) NOT NULL,
    close NUMERIC(18, 4) NOT NULL,
    last NUMERIC(18, 4) NOT NULL,
    prev_close NUMERIC(18, 4) NOT NULL,
    volume BIGINT NOT NULL,
    turnover NUMERIC(22, 2) NOT NULL,
    trades BIGINT,
    source_format VARCHAR(20) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_daily_price PRIMARY KEY (symbol_id, date_id),
    CONSTRAINT chk_fact_high_gte_low CHECK (high >= low),
    CONSTRAINT chk_fact_prices_positive CHECK (
        open > 0 AND high > 0 AND low > 0 AND close > 0 AND last > 0 AND prev_close > 0
    ),
    CONSTRAINT chk_fact_volume_non_negative CHECK (volume >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_price_date_id ON gold.fact_daily_price(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_daily_price_symbol_id ON gold.fact_daily_price(symbol_id);
