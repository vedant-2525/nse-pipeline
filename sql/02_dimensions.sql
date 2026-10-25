-- ==============================================================================
-- 02_dimensions.sql: Create dimensional tables in Gold schema
-- ==============================================================================

-- Equity Symbol Dimension
CREATE TABLE IF NOT EXISTS gold.dim_symbol (
    symbol_id SERIAL PRIMARY KEY,
    symbol VARCHAR(30) UNIQUE NOT NULL,
    isin VARCHAR(20),
    company_name VARCHAR(150),
    sector VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_symbol_ticker ON gold.dim_symbol(symbol);
CREATE INDEX IF NOT EXISTS idx_dim_symbol_sector ON gold.dim_symbol(sector);

-- Calendar & Trading Date Dimension
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id INT PRIMARY KEY, -- e.g. 20241011
    date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day_of_week INT NOT NULL, -- 1=Monday, 7=Sunday (ISO 8601)
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    is_trading_day BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_date ON gold.dim_date(date);
CREATE INDEX IF NOT EXISTS idx_dim_date_trading_day ON gold.dim_date(is_trading_day);
