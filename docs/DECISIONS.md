# Architecture & Design Decisions Log

This document records architectural and design decisions made throughout the project lifecycle.
Format: **Context**, **Decision**, **Alternatives Considered**, **Trade-offs**.

---

## ADR 001: Project Structure, Tooling, and Windows Adaptation (Phase 0)

- **Date:** 2026-10-16
- **Status:** Accepted

### Context
We are starting an end-to-end batch data pipeline for Indian stock market data (NSE) intended for a Data Engineering portfolio. The host machine is running Windows 11 with Python 3.13 and Git, but lacks native `make` and has no Docker daemon currently installed. Airflow and Spark can be challenging to run natively on Windows due to OS-level POSIX assumptions and `winutils` dependencies.

### Decision
1. Organize the repository into clear layered modules under `src/nse_pipeline/` (`ingest`, `transform`, `quality`, `load`, `utils`).
2. Use `pyproject.toml` as the single source of truth for tool configurations (`ruff`, `pytest`, packaging).
3. Use `ruff` for ultra-fast linting and code formatting, and `pytest` for unit testing.
4. Provide both a standard `Makefile` (for CI/Linux/WSL) and a native PowerShell script `scripts/tasks.ps1` for Windows local execution.
5. Store environment-specific variables and secrets outside source code using `.env` (with `.env.example` committed).

### Alternatives Considered
- *Using Flake8 + Black + isort:* Rejected because `ruff` unifies all three with 10x-100x higher speed and simpler unified configuration in `pyproject.toml`.
- *Pure Makefile:* Without MinGW / GnuWin32, native Windows users cannot execute `make`. Adding `scripts/tasks.ps1` ensures out-of-the-box ease on Windows.

### Trade-offs
- Maintaining both `Makefile` and `scripts/tasks.ps1` requires keeping target names in sync, but drastically simplifies local development across environments.

---

## ADR 002: Ingestion Architecture, Rate Limiting, and Fallback Strategy (Phase 1)

- **Date:** 2026-10-18
- **Status:** Accepted

### Context
Official NSE market files are published daily post-market (~6:30 PM IST). On July 8, 2024, NSE migrated from the legacy file naming standard (`cmDDMMMYYYYbhav.csv.zip`) to the UDiFF standard (`BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip`). Furthermore, NSE servers enforce bot protection and rate limits, returning 403 or 429 when hit aggressively or without browser headers. The pipeline requires high reliability for both historical backfills and ongoing daily runs.

### Decision
1. Implement a `BhavcopySource` abstract interface with `fetch(trade_date, output_dir) -> Path` to decouple source fetching from downstream processing.
2. Build `NseSource` with date-based URL resolution: dates on or after 2024-07-08 use the UDiFF endpoints, while prior dates use legacy historical endpoints.
3. Enforce ethical data engineering practices in `BhavcopyDownloader`:
   - Mandatory polite delays (1-3 seconds).
   - Idempotent lake writes in Hive-style partitions (`data/bronze/trade_date=YYYY-MM-DD/`).
   - Stop retrying immediately upon receiving HTTP 403 or 429 to avoid IP banning.
4. Provide `YFinanceSource` as an automated best-effort fallback when the primary NSE server is unreachable or historical files are missing.
5. Store small real fixture slices in `tests/fixtures/` and mock all HTTP/API calls during unit testing to ensure tests run 100% offline.

### Alternatives Considered
- *Using third-party unofficial scraping libraries:* Rejected because unmaintained scrapers break frequently when NSE alters headers or endpoints. Plain HTTP with `requests.Session` gives direct control over headers, cookies, and retry mechanisms.
- *Failing immediately without fallback:* Rejected because backfills across multi-year historical spans would halt whenever a single day is archived in an alternate format.

### Trade-offs
- The `yfinance` fallback does not supply ISIN or total trade counts, but preserves price continuity (OHLCV) so downstream analytics are unblocked.

---

## ADR 003: Scope Definition for Auxiliary `sec_bhavdata_full` Files (Phase 1 Refinement)

- **Date:** 2026-10-20
- **Status:** Accepted

### Context
During ingestion exploration, an auxiliary NSE reporting file `sec_bhavdata_full_{DDMMYYYY}.csv` was identified. This file focuses primarily on security delivery quantities and delivery percentages (`DELIV_QTY`, `DELIV_PER`), and lacks standard attributes present in official CM Bhavcopies (such as ISIN in earlier years or standardized turnover representations).

### Decision
1. `sec_bhavdata_full_*.csv` is ruled **out of scope** for the canonical Silver market price schema.
2. The core pipeline targets official Capital Market (CM) Bhavcopy files:
   - Modern UDiFF: `BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv`
   - Legacy Bhavcopy: `cmDDMMMYYYYbhav.csv`
   - yfinance fallback: `yfinance_fallback_YYYYMMDD.csv`
3. The Silver transformation parser must actively inspect incoming Bronze headers. If `sec_bhavdata_full` or any unrecognized format is passed, it must fail fast with a clear `UnsupportedFormatError` rather than silently misparsing data or producing null columns.

### Alternatives Considered
- *Creating a widened union schema:* Rejected because delivery metrics are not required by the star-schema warehouse specification, and mixing security delivery logs with standard market trade summaries complicates deduplication and schema consistency.

### Trade-offs
- Delivery data is not ingested into the Gold layer, but the core pipeline schema remains clean, predictable, and strictly conforming to official CM Bhavcopy standards.

---

## ADR 004: Silver Layer Schema Normalization, PySpark Architecture, and Quarantine Design (Phase 2)

- **Date:** 2026-10-20
- **Status:** Accepted

### Context & Real File Study (Step 0 Findings)
We conducted a deep inspection of real historical Bronze files downloaded from NSE:
1. **Modern UDiFF format (`BhavCopy_NSE_CM_0_0_0_20241011_F_0000.csv`):**
   - 34 columns: `TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4`.
   - Date format: `YYYY-MM-DD` (e.g. `2024-10-11`).
   - Turnover unit (`TtlTrfVal`): Exact Indian Rupees (INR) (e.g., M&M volume 1,652,856 @ close 3134.35 = 5,204,226,016.40 INR).
   - Segments: Segment `Sgmt=CM` and `FinInstrmTp=STK` denote equity stocks. Total rows: 2,852; Series `EQ` rows: 1,893. Other series present: `BE`, `SM`, `ST`, `GB`, `GS`, `TB`, `N5-N6`.
2. **Legacy Bhavcopy format (`cm15JAN2024bhav.csv`):**
   - 14 columns (13 named columns + 1 trailing empty column due to a trailing comma in the header: `ISIN,`):
     `SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,`.
   - Date format: `DD-MMM-YYYY` (e.g. `15-JAN-2024`).
   - Turnover unit (`TOTTRDVAL`): Exact Indian Rupees (INR).
   - Total rows: 2,682; Series `EQ` rows: 1,795.
3. **Key Observations & Invariants:**
   - Both formats have 100% unique primary keys on `(trade_date, symbol, series)` in their raw state.
   - For series `EQ`, all records strictly satisfy `low <= open, close, last <= high` and `high >= low`. Non-equity series (e.g. debt series `AT`) occasionally have zeroes in `LAST`, confirming that filtering for equity cash series (`EQ`) avoids false data anomalies.
   - Leading/trailing whitespace occurs in symbols and series across CSV files, requiring trimming and uppercasing.

### Decision
1. **Canonical Silver Schema:**
   Normalise both modern UDiFF and legacy formats into one uniform schema:
   - `trade_date` (DateType): Standard ISO date `YYYY-MM-DD`.
   - `symbol` (StringType): Uppercase, trimmed equity ticker.
   - `series` (StringType): Uppercase, trimmed series code (e.g., `EQ`).
   - `isin` (StringType, nullable): International Securities Identification Number.
   - `open`, `high`, `low`, `close`, `last`, `prev_close` (DecimalType(18, 4)): High-precision fixed-point decimal to avoid IEEE-754 floating point imprecision.
   - `volume` (LongType): Traded share quantity.
   - `turnover` (DecimalType(22, 2)): Exact turnover in Indian Rupees (INR).
   - `trades` (IntegerType, nullable): Total transaction count.
   - `source_format` (StringType): `"udiff"`, `"legacy"`, or `"yfinance"`.
   - `source_file` (StringType): Originating Bronze filename.
   - `ingested_at` (TimestampType): Pipeline processing timestamp (UTC).
2. **Schema Enforcement & Quarantine Pattern:**
   - Ingest CSVs with explicit string schemas (no schema inference).
   - Cast strings explicitly. Any rows with unparseable values, non-positive prices, negative volumes, or inverted ranges (`high < low` or `open/close/last` outside `[low, high]`) are quarantined into `data/quarantine/equity_prices/trade_date=YYYY-MM-DD/` with a descriptive `reject_reason` column.
   - Bad rows are never silently discarded.
3. **Data Quality Gate (Hard Failures):**
   A transformation run immediately halts with a non-zero exit code if:
   - Duplicate keys exist in clean output on `(trade_date, symbol, series)`.
   - Any clean row violates `high >= low` or price positivity (`price > 0`).
   - Any clean row contains `open`, `close`, or `last` outside `[low, high]`.
   - Clean row count is outside configured plausible bounds (default full market `[500, 3000]`; Nifty 50 `[40, 55]`).
   - Output `trade_date` does not match the partition date.
4. **Source Awareness:**
   - The Silver layer strictly requires official NSE Bronze files by default.
   - Fallback files (`yfinance`) are rejected unless the caller explicitly passes `--allow-fallback`.
5. **Idempotent Parquet Lake Storage:**
   - Parquet partitions are written to `data/silver/equity_prices/trade_date=YYYY-MM-DD/` with Spark's `dynamic` partition overwrite mode. Re-running a date replaces only that partition and leaves other dates untouched.
   - A `_SUCCESS` partition metadata JSON is written with run statistics (read, kept, rejected, deduped counts).

### Alternatives Considered
- *Using DoubleType for prices:* Rejected because floating-point representation causes precision loss (e.g., `0.1 + 0.2 != 0.3`). `Decimal(18, 4)` is the financial engineering standard.
- *Inferring CSV schema:* Rejected because Spark schema inference reads the entire file twice and can misinterpret sparse columns. Explicit string schema reading with typed casting guarantees full control and quarantine interception.

### Trade-offs
- Using PySpark adds JVM startup overhead (~2-3 seconds locally), but scales seamlessly to multi-year backfills and distributed cluster execution.

---

## ADR 005: Silver Layout Hardening, Stage Reconciliation Invariants, and Fallback Governance (Phase 2 Refinement)

- **Date:** 2026-10-21
- **Status:** Accepted

### Context
Code review of Phase 2 revealed critical operational issues:
1. **Silver Layout Defect:** Writing to per-day directories (`.parquet(silver_dir / trade_date=...)`) stored `trade_date` both in the partition directory name and inside the Parquet payload, causing PyArrow/Pandas base-directory reads (`pd.read_parquet('data/silver/equity_prices')`) to fail with schema merge conflicts (`date32[day]` vs `dictionary<values=string...>`).
2. **Missing Reconciliation Traceability:** When transforming with the default universe, non-universe rows were dropped silently without stage-by-stage accounting.
3. **Unnecessary Disabling of Spark ANSI Mode:** ANSI mode was disabled globally to avoid cast errors, but `try_cast` and `try_to_date` safely return NULL on corrupted strings under ANSI mode without hiding overflow or divide-by-zero errors.
4. **yfinance Latency on Closed Dates:** Sequential querying of 51 symbols when markets were closed took ~65s.
5. **Symbol Universe Discrepancy:** `config/symbols.yaml` contains 51 symbols instead of 50.

### Decision
1. **Standard Hive Partitioning via Base Directory Overwrite:**
   - Write to the base path `data/silver/equity_prices` with `.partitionBy("trade_date")` and `mode("overwrite")`.
   - In dynamic overwrite mode, Spark replaces only the targeted `trade_date=YYYY-MM-DD` directory and strips `trade_date` from internal file columns, enabling seamless multi-partition reads via PyArrow and DuckDB.
   - `_SUCCESS.meta.json` resides inside the partition folder and is ignored by PyArrow readers due to the leading underscore.
2. **Deterministic Stage Reconciliation Gate:**
   - Enforce an exact balance invariant:
     `read_count == non_equity_filtered + universe_filtered + rejected + deduped + row_count`.
   - Any unaccounted drop halts the pipeline immediately with a `DataQualityError`.
3. **PySpark ANSI Mode Re-enabled:**
   - Set `spark.sql.ansi.enabled = true`.
   - Use `try_cast` for numeric columns and `try_to_date` for dates. Malformed values produce NULL and are diverted into quarantine without crashing.
4. **Reject-Rate Quality Gate:**
   - Fail the transformation if quarantined rows exceed `max_reject_rate_pct` (default 2.0% in `config/settings.yaml`) of evaluated equity records.
5. **Symbol Universe Scope & 51-Constituent Clarification:**
   - The discrepancy of 51 entries in `config/symbols.yaml` stems from index rebalancing around September 2024: Bharat Electronics (`BEL`) and Trent (`TRENT`) were added to Nifty 50, but `LTIMindtree` (`LTIM`) was retained alongside them in the static list.
   - This list represents a fixed operational universe (convenience approximation) rather than point-in-time historical index membership. Real-world financial backtesting requires point-in-time constituent schedules.
   - The `--all-symbols` flag refers specifically to all series `EQ` cash equity securities listed on NSE, explicitly excluding debt, warrants, and derivative series (`BE`, `BZ`, `GB`, `GS`, etc.).
6. **yfinance Price Distortions & Fast Anchor Probe:**
   - Yahoo Finance historical prices are split- and bonus-adjusted even when requesting `auto_adjust=False`. Consequently, fallback prices may diverge from official NSE bhavcopy traded prices. Silver retains strict source governance refusing fallback data by default.
   - To avoid ~65s sequential timeouts on closed market dates, an anchor probe (`RELIANCE.NS`) is evaluated first. If no data is returned, `FileNotFoundError` is raised immediately without retrying or querying the remaining 50 symbols.
7. **Database Driver Modernization:**
   - Upgraded to modern `psycopg` 3 (`psycopg[binary]`), removing unused legacy drivers (`psycopg2-binary`, `sqlalchemy`, `streamlit`).

### Alternatives Considered
- *Allowing partial row drops without reconciliation:* Rejected because silent row drops are a primary cause of undetected data pipeline decay in production.
- *Reading partition folders individually in downstream loaders:* Rejected because standard analytics engines (PyArrow, Spark, DuckDB, Trino) expect valid Hive directory partitioning.

### Trade-offs
- Writing to the base path requires explicit directory isolation during unit tests (`tmp_path`), but standardizes the lake architecture.

---

## ADR 006: PostgreSQL Dimensional Star Schema, PyArrow Loader, and Atomic Upsert Boundaries (Phase 3)

- **Date:** 2026-10-25
- **Status:** Accepted

### Context
Phase 3 requires ingesting cleaned Silver Parquet market data into a PostgreSQL relational warehouse for fast downstream querying, financial window analytics, and business reporting. The design must guarantee data integrity, zero partial writes, idempotency (re-running a date must not duplicate facts), and operational traceability.

### Decision
1. **Dimensional Star-Schema Architecture:**
   - **`gold.dim_symbol`**: Surrogate key `symbol_id`, unique ticker `symbol`, `isin`, company name, and sector (seeded from `config/sectors.csv` and auto-extended upon encountering new equities).
   - **`gold.dim_date`**: Integer key `date_id` (e.g. `20241011`), calendar attributes (year, quarter, month, day of week, day name, `is_weekend`, `is_holiday`, `is_trading_day`). Populated for 2020-01-01 to 2030-12-31.
   - **`gold.fact_daily_price`**: Primary key `(symbol_id, date_id)`, foreign keys to dimensions, `NUMERIC(18, 4)` prices, `NUMERIC(22, 2)` turnover in INR, `BIGINT` volume and trades, source metadata, and check constraints (`high >= low`, prices > 0, volume >= 0).
   - **`ops.pipeline_run_log`** & **`ops.dq_results`**: Operational audit tables tracking run status, row throughput, error messages, and gate validation details.
2. **PyArrow In-Memory Reader instead of PySpark for Loading:**
   - Daily Silver partitions range from 51 rows (~50 KB) to ~1,900 rows (~1.5 MB).
   - PyArrow reads Parquet partitions in milliseconds with minimal memory footprint and zero JVM startup overhead, decoupling the warehouse loading process from Apache Spark.
3. **Staging Pattern + Atomic Upsert in a Single Transaction:**
   - Silver rows are loaded into a temporary table (`staging_price`) using `psycopg 3`'s streaming `COPY` protocol (`cursor.copy()`).
   - Unknown symbols are dynamically inserted into `gold.dim_symbol`.
   - Facts are inserted into `gold.fact_daily_price` using `ON CONFLICT (symbol_id, date_id) DO UPDATE`.
   - All steps execute inside a single transaction (`with conn.transaction():`). If an error or constraint violation occurs mid-load, the entire transaction rolls back cleanly, leaving the fact table untouched.
4. **Independent Operational Logging:**
   - Operational logs (`ops.pipeline_run_log`, `ops.dq_results`) are committed via an independent connection/transaction, ensuring that execution failures are permanently logged even when the core data transaction rolls back.
5. **Pre-Load Metadata & Governance Gating:**
   - Before executing SQL operations, the loader inspects `_SUCCESS.meta.json`.
   - If `source_type != 'official'` and `--allow-fallback` is absent, loading is refused with `PermissionError`.
   - If row-count reconciliation fields are missing or imbalanced, loading is aborted with `PartitionValidationError`.
6. **Holiday Coverage & Date Dimension Behavior:**
   - `config/holidays.csv` contains verified NSE trading holidays for calendar years 2024 and 2025.
   - For dates outside 2024-2025, holiday dates are not invented; `is_holiday` evaluates to `False`, and `is_trading_day` treats all non-weekend weekdays as trading days under "weekday, holiday data unavailable" semantics.

### Alternatives Considered
- *Using SQLAlchemy ORM:* Rejected in favor of native `psycopg 3` streaming COPY. ORM row insertion incurs substantial CPU and serialization overhead compared to bulk COPY + set-based SQL upserts.
- *Loading directly with PySpark JDBC:* Rejected because JVM JDBC writers require Spark executor coordination and lack fine-grained single-transaction DDL/staging control.

### Trade-offs
- PyArrow loads data into client memory before streaming COPY; for massive historical backfills, batching by date partition maintains low memory consumption (< 50 MB RAM).

---

## ADR 007: Universe Guard and Symbol Filtering Boundaries

- **Date:** 2026-10-25
- **Status:** Accepted

### Context
`transform --all-symbols` processes all ~1,800-1,900 cash equities in the market, while default `transform` filters down to the ~50 symbols configured in `config/symbols.yaml` (e.g. NIFTY 50). Because both wrote into the same partitioned directory layout (`data/silver/equity_prices/trade_date=YYYY-MM-DD/`), subsequent warehouse loads would ingest whichever dataset was produced last. This created ambiguity and risked loading full market data into a curated star schema inadvertently.

### Decision
1. **Explicit Universe Metadata**:
   - `_SUCCESS.meta.json` records `universe` (`"default_list"` or `"all_eq"`) and `symbol_count`.
2. **Pre-Load Universe Guard**:
   - The warehouse loader actively inspects `universe`.
   - If `universe == "all_eq"`, loading is refused with `PermissionError` unless `--include-all-symbols` is explicitly passed.
   - Partitions lacking the `universe` field (older schema layout) are rejected with `PartitionValidationError`, directing the user to re-run the transformation.

### Alternatives Considered
- **Store all EQ rows in Silver & Gold and flag curated universe in `dim_symbol` (e.g. `is_nifty50` boolean):**
  - *Why not chosen yet:* While architecturally sound for an enterprise-scale warehouse, storing all 1,900 equities across years of daily data expands storage 40x and increases local Spark transformation and PostgreSQL COPY runtimes during development iterations. Retaining explicit partitioning governance guarantees safety and predictable local performance, while documenting full-universe storage as an evolutionary step for Phase 5+.

### Trade-offs
- Switching between full-market and curated-universe analysis requires explicit transform re-runs or isolated silver directories, but prevents unexpected fact-table inflation and guarantees deterministic loading.

---

## ADR 008: Analytical Window Views, Corporate Actions Strategy, and Backfill Design (Phase 4)

- **Date:** 2026-10-28
- **Status:** Accepted

### Context
Phase 4 requires computing rolling financial analytics (daily returns, 20d/50d moving averages, 20d volatility, top daily movers, volume spikes, and sector aggregations) on historical warehouse price series, as well as orchestrating multi-day batch backfills with rate-limiting defenses. Stock markets exhibit calendar discontinuities (weekends, holidays) and corporate actions (splits, bonus issues) that distort naive time-series arithmetic.

### Decisions
1. **Trading-Row Windowing vs Calendar-Day Windows:**
   - Technical market indicators (e.g. 20-day SMA, 20-day volatility) must operate over active trading rows (`ROWS BETWEEN 19 PRECEDING AND CURRENT ROW`) rather than calendar day spans (`INTERVAL '20 days'`).
   - Calendar intervals produce variable sample sizes (12 to 15 sessions) due to weekends and holidays, corrupting statistical variance and moving average comparability.
2. **Corporate Action Detection & Raw Price Integrity:**
   - Raw bhavcopy prices reflect actual executed cash settlement prices (auditable historical truth).
   - Real-world observation on corporate actions: On ex-dates, some exchanges/sources adjust `prev_close` downward to match post-split parity, while yesterday's executed close (`lag_close`) remains unadjusted. However, real measured data from NSE (e.g. RELIANCE on 2024-10-28 1:1 bonus issue, and DRREDDY on 2024-10-28 1:5 stock split) demonstrates that NSE's bhavcopy `prev_close` can also remain unadjusted (equal to unadjusted close $2655.70), while the close drops dramatically (-49.76% and -79.87%).
   - `gold.vw_suspected_corporate_actions` detects ex-dates strictly across consecutive trading sessions (`prev_trading_day_seq = trading_day_seq - 1`) using two complementary conditions:
     a) `|prev_close - lag_close| / lag_close > 0.05` (exchange-adjusted `prev_close` divergence > 5%), OR
     b) `|close - lag_close| / lag_close > 0.20` (unadjusted `prev_close` with > 20% single-day price movement).
   - We consciously do NOT silently mutate historical prices. Full retrospective backward chain-adjustment is deferred to downstream portfolio analytics.
3. **Database Views vs Materialized Tables:**
   - Analytical views (`gold.vw_*`) compute dynamic indicators on-the-fly without storage duplication or cache staleness.
   - For curated universes (Nifty 50), queries execute in sub-15ms using underlying B-tree indexes on `(symbol_id, date_id)` and `date_id`.
4. **Backfill Orchestration Architecture:**
   - **Per-Day Isolation**: Execution loops across dates individually, ensuring a failure on day $T$ does not rollback or compromise days $T-1$ or $T+1$.
   - **Single Spark Session**: A single `SparkSession` is created once and reused across all dates, eliminating 20s of JVM startup overhead per day.
   - **Hard Halt on Rate Limits (HTTP 403/429)**: Backfill halts immediately upon encountering exchange rate limiting, marking subsequent dates as `not_attempted` and exiting non-zero to protect exchange access.
   - **Resumability**: Days meeting all three criteria (official bronze file, valid silver metadata with `default_list`, and loaded gold facts) are skipped in milliseconds.

### Alternatives Considered
- *Using DuckDB or Pandas for analytics views:* Rejected because serving views directly from PostgreSQL allows standard BI tools, reporting dashboards, and SQL clients to query pre-computed indicators without secondary ingestion pipelines.
- *Materialized Views:* Rejected because `REFRESH MATERIALIZED VIEW` requires explicit post-load orchestration and exclusive table locks, which adds complexity unnecessary for current data volumes.



