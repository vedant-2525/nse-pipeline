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

### Prerequisites
- Python 3.11+ (Python 3.13 detected)
- Git
- Docker & Docker Compose (Required for Airflow and Postgres services in Phase 3+)

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

### 4. Running Linting and Tests

- **Using PowerShell (Windows):**
  ```powershell
  .\scripts\tasks.ps1 lint
  .\scripts\tasks.ps1 test
  ```
- **Using Make (Linux/macOS/WSL):**
  ```bash
  make lint
  make test
  ```
- **Direct CLI:**
  ```bash
  ruff check .
  ruff format --check .
  pytest
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
- [ ] **Phase 2: Transform (PySpark)** - Canonical schema mapping, cleaning, date-partitioned Parquet generation.
- [ ] **Phase 3: Warehouse & Load** - Star schema DDL, idempotent Postgres loader, pipeline run logging.
- [ ] **Phase 4: Analytics Views** - Moving averages, rolling volatility, top movers, sector returns.
- [ ] **Phase 5: Data Quality** - Stop-the-line validation gates and ops metadata logging.
- [ ] **Phase 6: Airflow Orchestration** - Daily scheduled DAG and backfill DAG with holiday skipping.
- [ ] **Phase 7: Dashboard & Packaging** - Streamlit dashboard, Docker Compose multi-service deployment.

---

## Disclaimer & Terms of Use

This project is created strictly for **educational and portfolio demonstration purposes**. Market data is sourced from public archives of the National Stock Exchange of India (NSE) and Yahoo Finance. All trademarks, logos, and market data belong to their respective owners. Do not use this pipeline for commercial purposes or high-frequency automated scraping in violation of exchange terms of service.

