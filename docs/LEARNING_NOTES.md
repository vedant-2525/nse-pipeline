# Data Engineering Learning Notes

A companion study guide for interviews, explaining the concepts, design rationales, and common interview questions for each phase of the project.

---

## Phase 0: Project Scaffolding & Engineering Best Practices

### Concepts Used
1. **Separation of Concerns in Pipeline Layers:**
   - Raw ingestion (`bronze`), cleaned canonical data (`silver`), and dimensional modeling (`gold`).
   - Clean subpackages (`ingest`, `transform`, `quality`, `load`, `utils`) avoid monolithic scripts and enable test isolation.
2. **Environment Isolation & Dependency Pinning:**
   - Using virtual environments prevents cross-project pollution.
   - Exact version pinning (`==` in `requirements.txt`) guarantees reproducibility across environments (local development, Docker containers, CI runners).
3. **Configuration & Secrets Management (12-Factor App):**
   - Non-sensitive parameters (file paths, symbol lists, holidays) live in declarative YAML/CSV files under `config/`.
   - Sensitive or environment-specific values (database credentials, ports) live in `.env` (git-ignored) with a documented `.env.example`.
4. **Automated Continuous Integration (CI):**
   - Enforcing lint checks (`ruff`), format checks (`ruff format --check`), and unit tests (`pytest`) before merging ensures code quality remains high.

### Why This Design?
- **Medallion Architecture Foundation:** Starting with predefined bronze, silver, and gold target locations ensures data lineage is preserved. If a downstream transformation fails or requirements change, the untouched raw data in bronze allows full reprocessing without re-scraping external APIs.
- **Fail Fast via Linting and Types:** Data engineering pipelines fail painfully when typos or unexpected types appear after hours of execution. Linters and type hints catch syntax, import, and type bugs before code reaches production or Airflow.

### Common Interview Questions
1. **Q: Why do you pin dependencies in production pipelines instead of using `>=`?**
   *Answer:* Breaking changes or deprecations in minor/patch releases of upstream libraries (e.g., Pandas or PySpark) can break production data pipelines unpredictably. Pinning guarantees identical builds across CI, staging, and production.
2. **Q: What is the Medallion Architecture and why not directly transform and load raw data into Postgres?**
   *Answer:* Medallion architecture (Bronze -> Silver -> Gold) isolates raw, immutable files from downstream business logic. If business requirements change, upstream schemas evolve, or data is corrupted, having immutable raw bronze files allows re-running transformations without re-ingesting from external providers.
3. **Q: How do you handle configuration across different environments (dev, test, prod)?**
   *Answer:* Following 12-Factor App principles: static, environment-agnostic config (e.g., symbol lists, retry thresholds) goes into tracked config files, while environment-dependent credentials (DB passwords, endpoints) are injected via environment variables (`.env`).

---

## Phase 1: Robust Data Ingestion & Fallback Strategies

### Concepts Used
1. **Strategy Pattern & Interface Decoupling:**
   - Defining `BhavcopySource` as an Abstract Base Class (`ABC`) establishes a contract (`fetch(trade_date, output_dir) -> Path`).
   - Downstream pipeline stages never care whether data was downloaded from official NSE archives, unzipped from UDiFF, or pulled via `yfinance`.
2. **Hive-Style Partitioning in Data Lakes:**
   - Files are stored under `data/bronze/trade_date=YYYY-MM-DD/`.
   - Partitioning by date allows downstream tools (PySpark, Hive, Presto/Trino, DuckDB) to perform **partition pruning**, reading only the required partitions instead of scanning the entire lake.
3. **Idempotent Ingestion:**
   - Before hitting the network, `BhavcopyDownloader` inspects whether the target date partition already exists with a valid raw file.
   - If present, it returns the existing path immediately. This makes DAG retries and backfills completely safe and eliminates redundant network calls.
4. **Exponential Backoff & Rate-Limit Handling:**
   - Transient failures (network timeouts, 502/503 gateway drops) are retried with exponential sleep (`delay * (2 ^ attempt)`).
   - Rate limits (403/429) trigger an immediate abort instead of endless retries, preventing IP blacklisting.
5. **Offline Unit Testing with Fixtures:**
   - Unit tests run 100% offline using small, committed sample files in `tests/fixtures/` and mocked HTTP sessions.
   - This ensures CI pipelines run in seconds and never fail due to external exchange outages.

### Why This Design?
- **Immutability of Bronze:** Raw files are saved untouched. If schema detection or cleaning logic contains bugs, the raw files remain intact on disk for re-processing without needing another exchange download.
- **Polite Scraping:** External exchange servers are public utilities. Respecting them with 1-3 second pauses, browser-like headers, and immediate backoff on 429/403 ensures ethical, durable data pipelines.

### Common Interview Questions
1. **Q: What is idempotency and why is it critical in data pipelines?**
   *Answer:* An operation is idempotent if running it once produces the exact same state as running it multiple times with the same input. In batch ingestion, running a day twice must not duplicate files, corrupt storage, or hit external APIs redundantly. This enables safe automatic retries in Airflow without side effects.
2. **Q: How do you handle schema drift or changing file formats at the ingestion boundary?**
   *Answer:* Ingest raw files as-is into Bronze without attempting to normalize columns at download time. This decouples ingestion (fetching bits over the wire) from transformation (interpreting structure). Schema variations (such as NSE's legacy vs UDiFF formats) are then handled by dedicated schema-mapping adapters in the Silver transform layer.
3. **Q: Why should unit tests never make external HTTP calls?**
   *Answer:* Network calls introduce non-determinism, slow down test suites, break when offline, and leak credentials or breach rate limits on external services. Tests should mock network boundaries and validate parsing logic using static fixtures.

