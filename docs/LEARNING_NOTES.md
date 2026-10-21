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

---

## Phase 2: PySpark Silver Layer, Schema Normalization & Quality Gates

### Concepts Used
1. **Schema Normalization across Drifted Formats:**
   - NSE transitioned from a legacy 14-column format (`cmDDMMMYYYYbhav.csv`) to a 34-column UDiFF format (`BhavCopy_NSE_CM_...`).
   - Rather than separate pipelines, format detection inspects headers dynamically and maps both sources to a single canonical Silver schema.
2. **Explicit Schemas vs Schema Inference:**
   - Production PySpark pipelines should avoid `.option("inferSchema", "true")`. Inference incurs a full second pass over files and can misclassify sparse or edge-case columns.
   - Reading all columns as explicit strings first, followed by explicit casting, ensures full control over parse failures and data quarantine.
3. **The Quarantine Pattern (Zero Silent Data Loss):**
   - Corrupt or invalid rows (e.g. malformed numbers, inverted `high < low`, prices out of range) are split from clean data and saved to `data/quarantine/equity_prices/` with a `reject_reason` column.
   - Clean data flows to Silver; quarantined rows are preserved for auditability and replay without crashing the batch run.
4. **ANSI SQL Mode & `try_cast` in PySpark 4.x:**
   - Starting in Spark 4.0, ANSI SQL mode is enabled by default (`spark.sql.ansi.enabled=true`). Under ANSI mode, invalid casts throw runtime exceptions (`NumberFormatException`) instead of producing `null`.
   - To support resilient data validation and quarantine routing, we use `try_cast(col as type)` expressions, safely returning `null` on corrupt inputs for quarantine tagging.
5. **Dynamic Partition Overwrites:**
   - Using `spark.sql.sources.partitionOverwriteMode = dynamic` ensures writing to `data/silver/equity_prices/trade_date=YYYY-MM-DD/` only overwrites the target date partition, leaving all historical date partitions untouched.
   - This guarantees idempotent re-runs: running the same trade date twice replaces the partition cleanly without duplicates.
6. **Fixed-Point Financial Arithmetic:**
   - Equity prices are stored as `Decimal(18, 4)` and turnover as `Decimal(22, 2)`.
   - Binary floating-point types (`DoubleType`, `FloatType`) introduce rounding artifacts (e.g., `0.1 + 0.2 = 0.30000000000000004`) which corrupt financial reconciliation.
7. **Windows Hadoop & Spark Environment Initialization:**
   - Windows lacks native POSIX filesystem tools. PySpark requires `winutils.exe` in `%HADOOP_HOME%\bin` for local directory management.
   - Spark worker processes require explicit `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` environment variables pointing to `sys.executable` to avoid spawning incorrect system Python binaries.

### Why This Design?
- **Hard Quality Invariant Gates:** Data pipelines should fail fast and loudly when unexpected domain violations occur (e.g. duplicate primary keys, future dates, negative prices). Halting before the warehouse load prevents polluted data from corrupting downstream models and executive reports.
- **Source Governance:** The Silver layer enforces that official exchange files are processed by default. Fallback sources (e.g. Yahoo Finance) are refused unless explicitly permitted via `--allow-fallback` to protect warehouse integrity.

### Common Interview Questions
1. **Q: Why should you avoid `inferSchema` in production Spark pipelines?**
   *Answer:* `inferSchema` requires a preliminary scan of the entire dataset before actual processing begins, doubling I/O costs. Furthermore, if a numeric column contains empty strings or unexpected symbols later in the file, inference may choose the wrong type or fail unexpectedly across batches.
2. **Q: How do you handle dirty or malformed records without failing the entire batch?**
   *Answer:* Use the Quarantine Pattern. Parse values using safe conversion (such as `try_cast`), identify violating rows via validation rules, and route them to a separate partitioned quarantine dataset with reason codes (`reject_reason`). This allows valid rows to proceed downstream while corrupt rows are preserved for alerting, debugging, and backfilling.
3. **Q: What is the difference between static and dynamic partition overwrite in Spark?**
   *Answer:* In static partition overwrite mode (the default), Spark deletes the entire dataset directory before writing the new partition data, wiping out all other partitions if not scoped correctly. Dynamic partition overwrite (`partitionOverwriteMode = dynamic`) overwrites only the specific partition folders corresponding to the data present in the DataFrame being written.
4. **Q: Why use `DecimalType` instead of `DoubleType` for financial market data?**
   *Answer:* `DoubleType` uses IEEE-754 64-bit floating point representation, which cannot precisely represent many base-10 decimal fractions (e.g., 0.05). In financial systems, cumulative rounding errors alter traded values, portfolio balances, and audit reports. `DecimalType` preserves exact fixed-point precision.
5. **Q: How does PySpark handle Windows local execution differences?**
   *Answer:* Windows requires native Hadoop binaries (`winutils.exe` and `hadoop.dll`) because Spark uses Hadoop's FileUtil class to manage file permissions and temporary directories. Without `HADOOP_HOME` and `winutils.exe`, Spark operations like Parquet writes or shuffle spilling fail with `UnsatisfiedLinkError` or `NullPointerException`.


