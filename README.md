# NSE Market Data Pipeline

[![CI](https://github.com/placeholder/nse-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/placeholder/nse-pipeline/actions)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)
![Code style](https://img.shields.io/badge/code%20style-ruff-000000.svg)

An end-to-end batch data engineering pipeline that ingests daily National Stock Exchange of India (NSE) bhavcopy market files, standardizes and cleans the data using PySpark into Parquet lake storage, performs automated data quality gates, loads a dimensional star-schema warehouse in PostgreSQL, and serves market analytics via an interactive Streamlit dashboard—all orchestrated with Apache Airflow.

---

## Architecture Overview

```text
  NSE Bhavcopy (Official daily file)        Fallback: yfinance
             │                                      │
             ▼                                      ▼
      [1. INGEST]  Python: polite delays, exponential retries, raw validation
             │
             ▼
     data/bronze/trade_date=YYYY-MM-DD/<raw_file>  (Immutable)
             │
             ▼
   [2. TRANSFORM]  PySpark: auto-detect schema drift, clean, dedupe, cast
             │
             ├────────────────────────────────┐ (quarantine)
             ▼                                ▼
     data/silver/trade_date=YYYY-MM-DD/    data/quarantine/trade_date=YYYY-MM-DD/
     (Cleaned Parquet partitions)
             │
             ▼
 [3. QUALITY CHECKS]  Critical stop-the-line checks & warning checks -> ops.dq_results
             │
             ▼
       [4. LOAD]   Idempotent upsert into PostgreSQL Gold star-schema
             │
             ▼
    [5. ANALYTICS]  PostgreSQL Analytical Views (returns, rolling MAs, volatility)
             │
             ▼
    [6. DASHBOARD]  Streamlit interactive analytics & pipeline monitoring

 Orchestrated by Apache Airflow (scheduled weekdays at 19:00 IST post-market)
```

---

## Tech Stack & Design Rationale

| Layer | Tool / Technology | Why This Choice? |
|---|---|---|
| **Language** | Python 3.11+ | Industry standard for data engineering with deep ecosystem support |
| **Data Lake** | Local Parquet / Medallion (`bronze`, `silver`) | Columnar compression, partition pruning, easily swappable to S3 / MinIO |
| **Data Processing** | PySpark (Local mode) | High-performance distributed dataframe processing and robust schema handling |
| **Warehouse** | PostgreSQL | Robust relational storage with powerful window functions and constraint enforcement |
| **Orchestration** | Apache Airflow | DAG-based scheduling, dependency management, automatic retries, backfill support |
| **Data Quality** | Custom rule-based gates | Deterministic schema validation and business rule checks logged to `ops.dq_results` |
| **Dashboard** | Streamlit | Rapid, Python-native visual interface for metrics and pipeline health |
| **Quality & CI** | Ruff, Pytest, GitHub Actions | Ultra-fast linting, formatting, and automated regression testing on every commit |

---

## Quickstart & Local Setup

### Windows & Spark Prerequisites

Running PySpark 4 on Windows requires specific environment setup:
1. **Python 3.13 & PySpark 4.2.0**: PySpark 3.5.x python workers crash on Python 3.13 due to internal API changes. `pyspark==4.2.0` is strictly required.
2. **Path without spaces**: Clone the repository to a path without spaces (e.g. `C:\Projects\nse-pipeline`). Paths with spaces break Hadoop file URI resolution on Windows.
3. **Java JDK 17**: Install Eclipse Temurin 17 (or Oracle JDK 17) and ensure `JAVA_HOME` points to your JDK directory (e.g., `C:\Program Files\Eclipse Adoptium\jdk-17...`).
4. **Hadoop winutils (Hadoop 3.4.0)**:
   - Create directory `C:\hadoop\bin`.
   - Download Hadoop 3.4.0 `winutils.exe` and `hadoop.dll` into `C:\hadoop\bin`.
   - Set environment variable `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to `PATH`.
5. **PySpark Worker Environment Variables**:
   - `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` must point to your virtualenv's Python binary (e.g., `C:\Projects\nse-pipeline\.venv\Scripts\python.exe`). The pipeline's helper (`src/nse_pipeline/utils/spark.py`) configures this automatically upon initialization.

### 1. Clone & Set Up Virtual Environment

```bash
# Clone the repository
git clone https://github.com/your-username/nse-pipeline.git
cd nse-pipeline

# Create and activate virtual environment
python -m venv .venv

# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# On Linux/macOS:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
# Install core dependencies and development tools
pip install -r requirements.txt
pip install -e .
```

### 3. Configure Environment

```bash
# Copy example environment configuration
cp .env.example .env
# On Windows PowerShell:
Copy-Item .env.example .env
```

### 4. Running the Pipeline CLI

#### Phase 1: Ingestion (Bronze Layer)
```powershell
# Ingest modern UDiFF format
python -m nse_pipeline.cli ingest --date 2024-10-11

# Ingest legacy format
python -m nse_pipeline.cli ingest --date 2024-01-15
```

#### Phase 2: PySpark Transformation (Silver Layer)
```powershell
# Transform single date (default Nifty 50 universe)
python -m nse_pipeline.cli transform --date 2024-10-11

# Transform full equity market (all traded symbols)
python -m nse_pipeline.cli transform --date 2024-10-11 --all-symbols

# Transform date range (skips weekends and NSE holidays automatically)
python -m nse_pipeline.cli transform --from 2024-10-01 --to 2024-10-15

# Transform with fallback source explicitly permitted
python -m nse_pipeline.cli transform --date 2024-10-11 --allow-fallback
```

> [!NOTE]
> **Symbol Universe & Index Scope:**
> `config/symbols.yaml` contains 51 equity symbols as a fixed convenience approximation reflecting Nifty 50 constituents around September 2024 (including BEL, TRENT, and LTIM). It is not a point-in-time constituent index; real historical backtesting requires per-date constituent tracking.
> Passing `--all-symbols` processes all Cash Market Equities (Segment `CM`, Instrument `STK`, Series `EQ`) across the exchange (~1,800 to 1,900 symbols daily); non-equity series (`BE`, `BZ`, `GB`, `GS`, `TB`, debt) are excluded by design.

#### Phase 3: PostgreSQL Warehouse & Loading (Gold Layer)
```powershell
# 1. Start the PostgreSQL 16 container (bound strictly to 127.0.0.1 for local isolation)
docker compose up -d

# 2. Initialize database schemas (gold, ops) and seed dimensions (dim_symbol, dim_date)
python -m nse_pipeline.cli db init

# 3. Load single silver partition into gold fact table
python -m nse_pipeline.cli load --date 2024-10-11

# 4. Load date range (skips weekends and holidays automatically)
python -m nse_pipeline.cli load --from 2024-10-01 --to 2024-10-15

# 5. Load partition with fallback source explicitly permitted
python -m nse_pipeline.cli load --date 2024-10-11 --allow-fallback

# 6. Load partition created with --all-symbols ('all_eq' universe)
python -m nse_pipeline.cli load --date 2024-10-11 --include-all-symbols
```

#### Phase 4: Historical Backfill & Financial Analytical Views
```powershell
# 1. Run backfill across a date range (starts with bounded range: 5 trading days)
python -m nse_pipeline.cli backfill --from 2025-01-06 --to 2025-01-10

# 2. Re-run same range (demonstrates instant idempotency skipping of loaded days)
python -m nse_pipeline.cli backfill --from 2025-01-06 --to 2025-01-10

# 3. Force re-processing of a backfill range
python -m nse_pipeline.cli backfill --from 2025-01-06 --to 2025-01-10 --force
```

> [!NOTE]
> **Backfill Runtime Expectations & Best Practices:**
> - **New Trading Day:** ~15–25 seconds per date (polite HTTP download + single shared Spark session transform + atomic PostgreSQL COPY upsert).
> - **Skipped (Already Loaded) Day:** < 100 milliseconds per date (checks Bronze, Silver metadata, and Gold facts).
> - **Rate-Limit Defense:** If an HTTP 403 or 429 is encountered, the backfill stops immediately and marks all subsequent dates as `not_attempted` to prevent exchange IP banning.
> - **Audit Reports:** Every run writes a structured execution summary to `data/reports/backfill_report_YYYYMMDD_HHMMSS.json`.
> - **Recommendation:** Always test with a bounded range (e.g. 5 trading days) before initiating multi-month backfills.

#### Phase 4 Analytical Views in PostgreSQL
The database migration (`sql/05_analytics_views.sql`) creates seven financial window views in the `gold` schema:
- `gold.vw_daily_returns`: Close price, lagged previous-session close, return vs lagged close, return vs exchange-adjusted prev close, and calendar days gap.
- `gold.vw_moving_averages`: 20-day and 50-day rolling simple moving averages (NULL until full historical depth is reached).
- `gold.vw_volatility_20d`: 20-session rolling sample standard deviation of daily returns (NULL until 20 returns exist).
- `gold.vw_top_movers`: Top 5 daily gainers and top 5 daily losers per active trading session.
- `gold.vw_volume_spikes`: Sessions where traded volume exceeds 2x the 20-day preceding average volume (excluding current row).
- `gold.vw_sector_performance`: Daily average return aggregated by industry sector (with `'UNKNOWN'` fallback).
- `gold.vw_suspected_corporate_actions`: Identifies stock split and bonus ex-dates where exchange-adjusted `prev_close` diverges from yesterday's close by > 5%.

Sample queries demonstrating each view are provided in `sql/examples.sql`.

> [!TIP]
> **Database Password Rotation (Without Losing Volume Data):**
> PostgreSQL persistent volumes initialize credentials on first boot. If you need to rotate the database password:
> 1. In the running container, alter the user password directly:
>    ```bash
>    docker exec -it nse_postgres psql -U postgres -c "ALTER USER postgres PASSWORD 'new_password';"
>    ```
> 2. Update `POSTGRES_PASSWORD=new_password` in your `.env` file.
> 3. Restart the container without removing volumes:
>    ```bash
>    docker compose restart postgres
>    ```
> This preserves existing table data in the `postgres_data` volume while synchronizing client connection credentials.

### 5. Running Linting and Tests

- **Running Offline Unit Tests (No Database or Internet Required):**
  ```powershell
  .\.venv\Scripts\pytest.exe
  ```
- **Running Integration Tests against PostgreSQL Container:**
  ```powershell
  docker compose up -d
  .\.venv\Scripts\pytest.exe tests/test_loader.py -k integration
  ```
- **Linting & Code Formatting:**
  ```powershell
  .\.venv\Scripts\ruff.exe check .
  .\.venv\Scripts\ruff.exe format --check .
  ```

---

## Bronze to Silver Data Flow

```text
[Bronze Raw CSV]
  ├── Modern UDiFF: BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv (34 cols)
  └── Legacy: cmDDMMMYYYYbhav.csv (14 cols)
         │
         ▼
[1. Source Governance & Detection]
  ├── Verify source metadata: official NSE vs fallback (yfinance refused without --allow-fallback)
  ├── Inspect headers to detect "udiff" vs "legacy" (fails on unsupported delivery reports)
  └── Read with explicit String schema (no inferSchema)
         │
         ▼
[2. Normalization & Filtering]
  ├── Map columns to canonical names: trade_date, symbol, series, isin, OHLCV, turnover, trades
  ├── Trim whitespace & uppercase ticker/series
  └── Filter for Cash Market Equities: Sgmt='CM', FinInstrmTp='STK', Series='EQ'
         │
         ▼
[3. Resilient Type Casting & Quarantine]
  ├── Use try_cast(...) expressions (prevents ANSI mode NumberFormatException crashes)
  ├── Check price positivity, valid dates, volume >= 0, and high >= low bounds
  ├── Split valid rows from invalid rows
  └── Invalid rows -> Parquet at data/quarantine/equity_prices/trade_date=YYYY-MM-DD/
         │
         ▼
[4. Deduplication & Data Quality Gates]
  ├── Drop exact duplicate rows; enforce uniqueness on (trade_date, symbol, series)
  └── Hard validation gates (fails pipeline on violation):
        - high >= low
        - low <= open, close, last <= high
        - prices > 0, volume >= 0
        - trade_date matches partition date
        - row count within configured bounds
         │
         ▼
[5. Idempotent Silver Lake Output]
  ├── Write Parquet using dynamic partition overwrite:
  │     data/silver/equity_prices/trade_date=YYYY-MM-DD/
  └── Write partition audit metadata: _SUCCESS.meta.json
```

---

## Warehouse Star-Schema Architecture

```text
         [gold.dim_symbol]                        [gold.dim_date]
         ├── symbol_id (PK, SERIAL)               ├── date_id (PK, INT YYYYMMDD)
         ├── symbol (VARCHAR UNIQUE)              ├── date (DATE UNIQUE)
         ├── isin (VARCHAR)                       ├── year, quarter, month
         ├── company_name (VARCHAR)               ├── day_of_week, day_name
         └── sector (VARCHAR)                     └── is_weekend, is_holiday, is_trading_day
                     │                                         │
                     │  ┌───────────────────────────────────┐  │
                     └──┤      gold.fact_daily_price        ├──┘
                        ├── symbol_id (FK)                  │
                        ├── date_id (FK)                    │
                        │   (PK: symbol_id, date_id)        │
                        ├── open, high, low, close, last    │
                        ├── prev_close, volume, turnover    │
                        └── trades, source_format, source_type
```

---

## Repository Structure

```text
├── config/                   # Declarative pipeline configs
│   ├── settings.yaml         # Ingestion, retry, and path settings
│   ├── symbols.yaml          # Nifty 50 equity symbol universe
│   ├── sectors.csv           # Symbol to company name & sector mapping
│   └── holidays.csv          # NSE trading holidays seed data
├── src/nse_pipeline/         # Core application package
│   ├── ingest/               # Downloader & bhavcopy sources
│   ├── transform/            # PySpark cleaning & schema normalization
│   ├── quality/              # Data quality gates and runners
│   ├── load/                 # Idempotent warehouse loaders
│   └── utils/                # Logging, calendars, and configuration helpers
├── sql/                      # DDL schemas, dimensions, facts, and analytical views
├── airflow/dags/             # Daily and backfill workflow DAGs
├── dashboard/                # Streamlit dashboard application
├── tests/                    # Unit & integration tests with sample fixtures
├── docs/                     # Architecture Decision Records (ADRs) & Learning Notes
├── Makefile                  # Lifecycle automation commands
├── scripts/                  # Shell and PowerShell utility scripts
└── pyproject.toml            # Ruff, pytest, and project build definitions
```

---

## Project Status

- [x] **Phase 0: Scaffold** - Repository structure, config files, CI skeleton, ruff/pytest setup, and ADRs.
- [x] **Phase 1: Ingest** - `BhavcopySource` interface, official NSE source with fallback to `yfinance`, retry mechanism.
- [x] **Phase 2: Transform (PySpark)** - Canonical schema mapping, cleaning, date-partitioned Parquet generation.
- [x] **Phase 3: Warehouse & Load** - Star schema DDL, idempotent Postgres loader, pipeline run logging.
- [ ] **Phase 4: Analytics Views** - Moving averages, rolling volatility, top movers, sector returns.
- [ ] **Phase 5: Data Quality** - Stop-the-line validation gates and ops metadata logging.
- [ ] **Phase 6: Airflow Orchestration** - Daily scheduled DAG and backfill DAG with holiday skipping.
- [ ] **Phase 7: Dashboard & Packaging** - Streamlit dashboard, Docker Compose multi-service deployment.

---

## Disclaimer & Terms of Use

This project is created strictly for **educational and portfolio demonstration purposes**. Market data is sourced from public archives of the National Stock Exchange of India (NSE) and Yahoo Finance. All trademarks, logos, and market data belong to their respective owners. Do not use this pipeline for commercial purposes or high-frequency automated scraping in violation of exchange terms of service.

