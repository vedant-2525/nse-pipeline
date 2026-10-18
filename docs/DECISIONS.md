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

