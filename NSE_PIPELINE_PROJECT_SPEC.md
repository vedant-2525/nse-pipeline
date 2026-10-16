# NSE Market Data Pipeline: Project Spec for an AI Coding Agent

> Put this file in the root of an empty repo. Tell the agent: "Read PROJECT_SPEC.md fully. Work on ONE phase at a time. After each phase, stop, summarize what you built, show how to run and test it, and wait for my approval."

---

## 1. Goal

Build an end-to-end **batch data pipeline** for Indian stock market end-of-day data (NSE), as a portfolio project for **Data Engineer internship/entry-level applications**.

The pipeline must: ingest raw daily files, clean them with PySpark, load a star-schema warehouse in Postgres, compute analytics tables, run data quality checks, be orchestrated by Airflow, be served through a small dashboard, and run fully with `docker compose up`.

**Priority order:** correctness > reliability > clarity > features. A smaller project that works, is tested, and is well documented beats a large half-working one.

## 2. Rules for the agent

1. **One phase at a time.** Do not start the next phase until I approve.
2. **I am a student and must be able to explain every part in an interview.** Keep code simple and readable. Add short comments explaining *why*, not *what*. Do not use clever abstractions I would not understand.
3. After each phase, append a short entry to `docs/LEARNING_NOTES.md`: concepts used, why this design, common interview questions it relates to.
4. Record every significant design decision in `docs/DECISIONS.md` (one short entry: context, decision, alternatives, trade-off).
5. Use type hints, `ruff` for linting/formatting, `pytest` for tests. Every transform and parser needs unit tests with small fixture files.
6. No secrets in code. Use `.env` (git-ignored) and commit a `.env.example`.
7. Pin dependency versions in `requirements.txt` after verifying they work together. Check current stable, compatible versions yourself; do not guess.
8. Ask before adding any dependency not listed in this spec.
9. Detect my OS first. If I am on Windows, note that Airflow and Spark are easiest through Docker/WSL2 and adapt the instructions.
10. Make a git commit at the end of each phase with a clear message.
11. If something in this spec is wrong or the real data differs from what is described, tell me and propose a fix. Do not silently work around it.

## 3. Tech stack

| Layer | Choice |
|---|---|
| Language | Python 3.11+ |
| Processing | PySpark (local mode; needs a compatible JDK, so verify which) |
| Storage (lake) | Local filesystem folders `data/bronze`, `data/silver` (Parquet). Design paths so swapping to S3/MinIO later is easy |
| Warehouse | PostgreSQL (Docker) |
| Orchestration | Apache Airflow (Docker Compose, LocalExecutor) |
| Dashboard | Streamlit |
| Testing/Lint | pytest, ruff |
| CI | GitHub Actions (lint + unit tests) |
| Packaging | Docker Compose |

Do not add dbt, Kafka, Great Expectations, or cloud services in the core build. They are stretch goals (Phase 8).

## 4. Architecture

```
  NSE bhavcopy (daily file)         fallback: yfinance
            |                              |
            v                              v
   [1. INGEST]  Python, retries, polite delays
            |
            v
   data/bronze/trade_date=YYYY-MM-DD/<raw file, untouched>
            |
            v
   [2. TRANSFORM]  PySpark: parse, normalize schema, clean, dedupe
            |
            v
   data/silver/trade_date=YYYY-MM-DD/*.parquet
            |
            v
   [3. QUALITY CHECKS]  fail/warn, results logged
            |
            v
   [4. LOAD]  idempotent load into Postgres (gold star schema)
            |
            v
   [5. ANALYTICS]  SQL views/tables: returns, moving averages, movers
            |
            v
   [6. DASHBOARD]  Streamlit
   
   Orchestrated by Airflow (weekdays, after market close, IST)
```

## 5. Repository structure

```
nse-pipeline/
├── PROJECT_SPEC.md
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── pyproject.toml            # ruff + pytest config
├── .env.example
├── .gitignore
├── .github/workflows/ci.yml
├── config/
│   ├── settings.yaml         # paths, retry counts, delays, backfill range
│   ├── symbols.yaml          # symbol universe (start with Nifty 50)
│   ├── sectors.csv           # symbol -> company name, sector (seed data)
│   └── holidays.csv          # NSE trading holidays (seed data, maintained by hand)
├── src/nse_pipeline/
│   ├── ingest/
│   │   ├── base.py           # BhavcopySource interface
│   │   ├── nse_source.py     # primary source
│   │   ├── yfinance_source.py# fallback source
│   │   └── downloader.py     # retries, delays, saves to bronze
│   ├── transform/
│   │   ├── schema.py         # canonical schema + legacy/new column mappings
│   │   ├── clean.py          # PySpark cleaning functions
│   │   └── run_silver.py     # entrypoint: bronze -> silver for a date
│   ├── quality/
│   │   ├── checks.py         # individual checks
│   │   └── runner.py         # runs checks, logs results
│   ├── load/
│   │   ├── postgres.py       # connection + idempotent loaders
│   │   └── run_load.py       # entrypoint: silver -> gold for a date
│   ├── utils/
│   │   ├── logging.py
│   │   └── calendar.py       # trading-day / holiday helpers
│   └── cli.py                # `python -m nse_pipeline.cli run --date 2025-01-15`
├── sql/
│   ├── 01_schemas.sql
│   ├── 02_dimensions.sql
│   ├── 03_facts.sql
│   ├── 04_ops_tables.sql
│   └── 05_analytics_views.sql
├── airflow/dags/
│   ├── nse_daily_pipeline.py
│   └── nse_backfill.py
├── dashboard/app.py
├── tests/
│   ├── fixtures/             # tiny sample files: legacy format, new format, bad data
│   ├── test_ingest.py
│   ├── test_transform.py
│   ├── test_quality.py
│   └── test_load.py
└── docs/
    ├── DECISIONS.md
    ├── LEARNING_NOTES.md
    └── architecture.png      # exported diagram for the README
```

## 6. Data source details

**Primary: NSE bhavcopy**, the official end-of-day file (price, volume, turnover per symbol). It is generally published after market close (roughly 6:30 PM IST or later).

The agent must **research the current official bhavcopy download location and file format** and not rely on memory. NSE has changed formats over time, so:

- Implement a `BhavcopySource` interface with `fetch(trade_date) -> path_to_raw_file`.
- Implement `NseSource` (primary) and `YFinanceSource` (fallback, built on `yfinance`; it is unofficial, so mark it as best-effort).
- Use a maintained open-source bhavcopy library only if it is current and works. Otherwise use plain HTTP with `requests` (session, browser-like User-Agent, cookies if required).
- **Be polite:** 1-3 second delay between requests, never hit the server in parallel, retry with exponential backoff, and stop on repeated 403/429 responses. Read NSE's terms of use and note in the README that the data is for educational use.
- Commit **small real sample files** to `tests/fixtures/` (one per known format) so tests never need the network.

**Expected content (verify against a real file):** symbol, series, open, high, low, close, last price, previous close, total traded quantity, total traded value, trade count, ISIN, trade date. Older files use names like `SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN`. Newer files use different names (e.g. `TckrSymb`, `SctySrs`, `OpnPric`, `ClsPric`, `TtlTradgVol`, `TradDt`). Build a **column mapping per format** in `schema.py` that maps both to one canonical schema, and detect the format automatically from the header.

**Scope:** equity series `EQ` only, for the symbols in `config/symbols.yaml` (start with the Nifty 50; I will update the list since index membership changes). Filtering by the config list must be easy to turn off for a full-market run later.

## 7. Canonical schema (silver layer)

| Column | Type | Notes |
|---|---|---|
| trade_date | date | partition column |
| symbol | string | uppercase, trimmed |
| series | string | |
| isin | string | nullable |
| open, high, low, close, last_price, prev_close | decimal(12,2) | |
| volume | long | |
| turnover | decimal(20,2) | |
| trades | int | nullable |
| source_file | string | file name it came from |
| ingested_at | timestamp | UTC |

Cleaning rules: trim/uppercase symbols, cast types explicitly (fail loudly on bad casts, never silently null), drop exact duplicates, quarantine invalid rows to `data/quarantine/trade_date=.../` with a reason column instead of deleting them.

## 8. Warehouse model (gold layer, Postgres)

Schemas: `gold` (star schema), `ops` (pipeline metadata).

```sql
-- gold.dim_symbol
symbol_id SERIAL PK, symbol TEXT UNIQUE NOT NULL, isin TEXT,
company_name TEXT, sector TEXT, is_active BOOLEAN DEFAULT TRUE

-- gold.dim_date
date_id INT PK (YYYYMMDD), full_date DATE UNIQUE, year INT, quarter INT,
month INT, day INT, day_of_week INT, is_weekend BOOLEAN, is_trading_day BOOLEAN

-- gold.fact_daily_price
symbol_id INT FK, date_id INT FK,
open, high, low, close, last_price, prev_close NUMERIC(12,2),
volume BIGINT, turnover NUMERIC(20,2), trades INT,
source_file TEXT, loaded_at TIMESTAMPTZ DEFAULT now(),
PRIMARY KEY (symbol_id, date_id)

-- ops.pipeline_run_log
run_id UUID, trade_date DATE, stage TEXT, status TEXT,   -- success|failed|skipped
rows_in INT, rows_out INT, started_at, finished_at, error TEXT

-- ops.dq_results
run_id UUID, trade_date DATE, check_name TEXT, severity TEXT,  -- error|warn
passed BOOLEAN, details JSONB, checked_at TIMESTAMPTZ
```

Add indexes on `fact_daily_price(date_id)` and `(symbol_id, date_id)`.

**Idempotent load:** for a given `trade_date`, use `INSERT ... ON CONFLICT (symbol_id, date_id) DO UPDATE` (or delete-then-insert inside one transaction). Re-running a day must never create duplicates. Add a test that loads the same day twice and asserts row counts are unchanged.

## 9. Analytics layer (SQL views)

Write these in `sql/05_analytics_views.sql`, using window functions:

1. `gold.vw_daily_returns`: daily % return per symbol using `LAG(close)`.
2. `gold.vw_moving_averages`: 20-day and 50-day moving average of close (`AVG() OVER (... ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)`).
3. `gold.vw_volatility_20d`: rolling 20-day standard deviation of daily returns.
4. `gold.vw_top_movers`: top 5 gainers and losers per trading day.
5. `gold.vw_volume_spikes`: days where volume > 2x the symbol's 20-day average volume.
6. `gold.vw_sector_performance`: average daily return per sector per day.

Comment each view with what it computes and why.

## 10. Data quality checks

Run after the silver step and before loading. Log every result to `ops.dq_results`.

| Check | Severity |
|---|---|
| File exists and is not empty | error |
| Required columns present after mapping | error |
| No duplicate (symbol, trade_date) | error |
| `high >= low`, `high >= open/close`, `low <= open/close` | error |
| Prices and volume are not negative; prices > 0 | error |
| Row count within a sane range for the symbol universe (e.g. ≥ 90% of expected symbols) | error |
| Every configured symbol present for the day | warn |
| Daily price move > 20% vs previous close | warn (could be a corporate action) |
| Null rate per column below a threshold | warn |

If any `error` check fails, stop before the load step and mark the run failed. Warnings are logged but do not stop the run.

## 11. Orchestration (Airflow)

**`nse_daily_pipeline` DAG**
- Schedule: weekdays, 19:00 `Asia/Kolkata` (after the file is typically available).
- Tasks: `check_trading_day` -> `ingest` -> `transform_silver` -> `quality_checks` -> `load_gold` -> `refresh_analytics`.
- `check_trading_day` consults `config/holidays.csv` and skips downstream tasks on holidays (status `skipped`, not failed).
- If the file is not yet published, retry with a delay (e.g. 5 retries, 15-minute gaps) before failing.
- Tasks must be idempotent so retries and re-runs are safe.
- Use `execution_date`-based logic so the DAG is backfill-safe.

**`nse_backfill` DAG:** a manual trigger with parameters `start_date` and `end_date`. It loops over trading days and runs the same stages. Default range: the last ~2 years, configurable.

## 12. Dashboard (Streamlit)

Read only from Postgres. Pages/sections:
1. **Overview:** latest trading date, number of symbols loaded, last run status, last DQ result.
2. **Stock explorer:** pick a symbol and date range, show a price chart with 20/50-day moving averages and a volume bar chart.
3. **Market movers:** top gainers and losers for a chosen date.
4. **Sector performance:** average return by sector.
5. **Pipeline health:** recent runs from `ops.pipeline_run_log` and DQ failures.

Keep it simple; the dashboard is a window onto the data, not the main deliverable.

## 13. Docker Compose services

`postgres` (warehouse), `airflow-webserver`, `airflow-scheduler` (with its own metadata DB), `dashboard`, and a `pipeline` image that contains Python + PySpark + JDK for the processing tasks. Mount `./data` as a volume. Provide a `Makefile` (or simple scripts) with: `make up`, `make down`, `make test`, `make lint`, `make backfill`, `make dashboard`.

## 14. Testing and CI

- **Unit tests:** column mapping for each format, cleaning rules, each DQ check (pass and fail cases), idempotent load, trading-day logic.
- **Integration test:** run one fixture day end to end against a temporary Postgres (Docker) and assert gold row counts.
- **CI:** GitHub Actions running `ruff check`, `ruff format --check`, and `pytest` (unit tests only) on every push.

## 15. Phases and acceptance criteria

**Phase 0: Scaffold.** Repo structure, config files, `.gitignore`, `.env.example`, ruff/pytest setup, README stub, CI skeleton.
*Done when:* `make lint` and `make test` run (even with placeholder tests) and CI is green.

**Phase 1: Ingest.** `BhavcopySource` interface, `NseSource`, `YFinanceSource`, downloader with retries and delays, raw files saved to bronze, CLI command to fetch one date.
*Done when:* I can run one command to download a real recent trading day into `data/bronze/`, and tests pass using fixtures only.

**Phase 2: Transform (PySpark).** Format detection, canonical schema mapping, cleaning, quarantine, Parquet written to silver partitioned by date.
*Done when:* both fixture formats produce identical-schema Parquet, and a re-run overwrites the same partition without duplicates.

**Phase 3: Warehouse + load.** SQL DDL, dimension seeding (`dim_date` generated, `dim_symbol` from `sectors.csv`), idempotent loader, ops tables, run logging.
*Done when:* loading the same day twice leaves row counts unchanged, and I can query prices in Postgres.

**Phase 4: Analytics views.** All six views with comments, plus a few sample queries in `sql/examples.sql`.
*Done when:* views return sensible results on at least one month of data.

**Phase 5: Data quality.** All checks, results logged, error checks block the load.
*Done when:* a deliberately corrupted fixture is caught and stops the run, with the failure visible in `ops.dq_results`.

**Phase 6: Airflow.** Daily DAG and backfill DAG, holiday handling, retries.
*Done when:* I can trigger a backfill for a one-week range from the Airflow UI and see all tasks succeed (holidays skipped).

**Phase 7: Dashboard, Docker, docs.** Streamlit app, full Compose setup, final README, architecture diagram, screenshots.
*Done when:* a fresh clone plus `cp .env.example .env && docker compose up` gives a working system.

**Phase 8 (stretch, only if I ask):** corporate-action adjustment (split/bonus factors table and adjusted close), dbt models for the gold layer, Great Expectations, S3/MinIO storage, a streaming extension with Kafka.

## 16. README requirements (written in Phase 7, drafted earlier)

1. One-paragraph summary and an architecture diagram.
2. Tech stack and why each piece was chosen.
3. How to run it (prerequisites, three commands).
4. Data model diagram (ERD).
5. Pipeline stages explained, including idempotency, schema drift handling, and holiday logic.
6. Data quality checks table.
7. Screenshots of the dashboard and the Airflow DAG.
8. Known limitations, e.g. unofficial data sources may break, no corporate-action adjustment yet, Nifty 50 list is manually maintained.
9. "What I'd change at scale": S3 + partitioned Parquet, Spark on a cluster, a managed warehouse, a managed orchestrator, monitoring and alerting.

## 17. Non-goals and guardrails

- No trading signals, predictions, or investment advice. This is a data engineering project.
- No scraping behavior that ignores rate limits, terms of use, or blocking responses.
- No paid APIs or cloud accounts required to run the core project.
- Do not store credentials in the repo.

## 18. Kickoff prompt (paste this to the agent first)

> Read `PROJECT_SPEC.md` completely. Confirm my OS and which tools I have installed (Python, Java, Docker, Git) before writing code. Then do **Phase 0 only**. When finished, summarize what you created, how to run it, and what you'd do in Phase 1, then wait for my approval.
