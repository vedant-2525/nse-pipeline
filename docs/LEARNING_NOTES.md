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
