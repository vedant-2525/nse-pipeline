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

