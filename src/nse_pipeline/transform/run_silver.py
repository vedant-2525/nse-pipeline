"""Silver layer entrypoint: transforms Bronze raw files to validated Silver Parquet.

Why this design:
1. Strict Source Governance: Rejects fallback files by default unless explicitly allowed.
2. Explicit String Schemas: Prevents silent data corruption or schema inference crashes.
3. Complete Quarantine Traceability: Every rejected row is preserved in data/quarantine with a reason.
4. Dynamic Partition Overwrites: Ensures re-runs are idempotent and replace only the target partition.
5. Audit Metadata: Writes a _SUCCESS.meta.json containing complete stage metrics.
"""

import json
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nse_pipeline.ingest.metadata import read_file_metadata
from nse_pipeline.transform.clean import (
    deduplicate_records,
    filter_cash_equities,
    filter_universe,
    normalize_columns,
    validate_and_split,
)
from nse_pipeline.transform.quality import (
    validate_reconciliation_gate,
    validate_reject_rate_gate,
    validate_silver_quality,
)
from nse_pipeline.transform.schema import (
    detect_format,
    get_raw_schema_for_format,
)
from nse_pipeline.utils.logging import logger
from nse_pipeline.utils.spark import get_spark_session


def find_bronze_file_for_date(
    bronze_base_dir: Path,
    trade_date: date,
    allow_fallback: bool = False,
) -> tuple[Path, str, str]:
    """Locates the raw Bronze file for trade_date, verifying source provenance.

    Returns:
        tuple (file_path, source_format, source_type)
    """
    date_str = trade_date.strftime("%Y-%m-%d")
    partition_dir = bronze_base_dir / f"trade_date={date_str}"

    if not partition_dir.is_dir():
        raise FileNotFoundError(
            f"No Bronze partition found for date {trade_date} at '{partition_dir}'. "
            f"Please run ingestion first: python -m nse_pipeline.cli ingest --date {date_str}"
        )

    csv_files = [f for f in partition_dir.glob("*.csv") if f.stat().st_size > 0]
    if not csv_files:
        raise FileNotFoundError(
            f"No non-empty CSV raw files found in Bronze partition '{partition_dir}'."
        )

    # Inspect source metadata
    official_files: list[Path] = []
    fallback_files: list[Path] = []

    for f in csv_files:
        meta = read_file_metadata(f)
        source_name = meta.get("source_name") if meta else ""
        if "yfinance" in source_name or "fallback" in f.name.lower():
            fallback_files.append(f)
        else:
            official_files.append(f)

    if official_files:
        selected = official_files[0]
        source_type = "official"
    elif fallback_files:
        if not allow_fallback:
            raise PermissionError(
                f"Only fallback data ({fallback_files[0].name}) exists for {trade_date}.\n"
                "By default, the Silver pipeline refuses fallback data to maintain warehouse integrity.\n"
                "To explicitly process fallback data, re-run with the flag: --allow-fallback"
            )
        selected = fallback_files[0]
        source_type = "fallback"
    else:
        selected = csv_files[0]
        source_type = "official"

    with open(selected, encoding="utf-8") as f:
        header_line = f.readline()

    source_format = detect_format(header_line, selected)
    logger.info(
        "Found Bronze file: %s (format: %s, source: %s)", selected.name, source_format, source_type
    )
    return selected, source_format, source_type


def run_silver_transform(
    trade_date: date,
    bronze_dir: Path | str = "data/bronze",
    silver_dir: Path | str = "data/silver/equity_prices",
    quarantine_dir: Path | str = "data/quarantine/equity_prices",
    allowed_series: list[str] | None = None,
    symbol_universe: list[str] | None = None,
    allow_fallback: bool = False,
    spark: SparkSession | None = None,
    min_row_count: int | None = None,
    max_row_count: int | None = None,
    max_reject_rate_pct: float = 2.0,
) -> dict[str, Any]:
    """Executes the Bronze-to-Silver transformation pipeline for a single trade date.

    Returns:
        dict containing metrics (read_count, non_equity_filtered, universe_filtered, rejected, deduped, row_count, etc.)
    """
    start_time = time.time()
    bronze_path = Path(bronze_dir)
    silver_path = Path(silver_dir)
    quarantine_path = Path(quarantine_dir)
    date_str = trade_date.strftime("%Y-%m-%d")

    logger.info("====================================================================")
    logger.info("Starting Silver transformation for trade date: %s", date_str)
    logger.info("====================================================================")

    # 1. Locate bronze file with source governance
    raw_file, source_format, source_type = find_bronze_file_for_date(
        bronze_path, trade_date, allow_fallback=allow_fallback
    )

    # 2. Get Spark session
    spark_session = spark or get_spark_session(app_name=f"silver-transform-{date_str}")

    # 3. Read raw CSV using explicit String schema
    raw_schema = get_raw_schema_for_format(source_format)
    raw_df = (
        spark_session.read.option("header", "true")
        .schema(raw_schema)
        .csv(raw_file.resolve().as_posix())
    )
    read_count = raw_df.count()
    logger.info("Read %d raw records from Bronze file '%s'.", read_count, raw_file.name)

    # 4. Standardize columns
    normalized_df = normalize_columns(
        raw_df, source_format=source_format, source_file=raw_file.name
    )

    # 5. Stage 1 Filter: Cash Equity segment (series EQ, etc.)
    cash_equity_df = filter_cash_equities(
        normalized_df,
        source_format=source_format,
        allowed_series=allowed_series or ["EQ"],
    )
    cash_equity_count = cash_equity_df.count()
    non_equity_filtered = read_count - cash_equity_count
    logger.info(
        "Stage 1 [Cash Equity Filter]: Excluded %d non-equity records; retained %d cash equity records (series %s).",
        non_equity_filtered,
        cash_equity_count,
        allowed_series or ["EQ"],
    )

    # 6. Stage 2 Filter: Symbol Universe filtering
    universe_df = filter_universe(cash_equity_df, symbol_universe=symbol_universe)
    universe_count = universe_df.count()
    universe_filtered = cash_equity_count - universe_count
    if symbol_universe:
        logger.info(
            "Stage 2 [Universe Filter]: Excluded %d non-universe records; retained %d records matching target universe (%d symbols).",
            universe_filtered,
            universe_count,
            len(symbol_universe),
        )
    else:
        logger.info(
            "Stage 2 [Universe Filter]: Processed all symbols (--all-symbols active); 0 excluded, retained all %d records.",
            universe_count,
        )

    # 7. Stage 3: Type casting and quarantine invalid rows
    clean_candidates_df, quarantine_df = validate_and_split(universe_df)
    quarantine_count = quarantine_df.count()
    clean_count = clean_candidates_df.count()
    logger.info(
        "Stage 3 [Validation & Quarantine]: Validated %d records; quarantined %d invalid records.",
        clean_count,
        quarantine_count,
    )

    if quarantine_count > 0:
        logger.warning(
            "Quarantined %d invalid records for date %s (saved to %s).",
            quarantine_count,
            date_str,
            quarantine_path,
        )
        quarantine_path.mkdir(parents=True, exist_ok=True)
        # Ensure trade_date is non-null for partition column
        quarantine_to_write = quarantine_df.withColumn(
            "trade_date", F.coalesce(F.col("trade_date"), F.lit(trade_date))
        )
        quarantine_to_write.write.mode("overwrite").partitionBy("trade_date").parquet(
            quarantine_path.resolve().as_posix()
        )

    # 8. Stage 4: Deduplicate records
    deduped_df, deduped_count = deduplicate_records(clean_candidates_df)
    row_count = deduped_df.count()
    logger.info(
        "Stage 4 [Deduplication]: Dropped %d duplicate keys; retained %d final clean records.",
        deduped_count,
        row_count,
    )

    # 9. Dynamic bounds checking based on symbol universe vs full market
    if min_row_count is None:
        min_row_count = 40 if symbol_universe else 500
    if max_row_count is None:
        max_row_count = 60 if symbol_universe else 3500

    # 10. Data Quality Gates:
    # 10a. Critical quality invariants on clean output
    validate_silver_quality(
        deduped_df,
        expected_date=trade_date,
        min_row_count=min_row_count,
        max_row_count=max_row_count,
    )

    # 10b. Reject rate gate
    validate_reject_rate_gate(
        quarantine_count=quarantine_count,
        equity_evaluated_count=universe_count,
        max_reject_rate_pct=max_reject_rate_pct,
    )

    # 10c. Row-count reconciliation gate: read_count == non_equity + universe + rejected + deduped + row_count
    validate_reconciliation_gate(
        read_count=read_count,
        non_equity_filtered=non_equity_filtered,
        universe_filtered=universe_filtered,
        rejected=quarantine_count,
        deduped=deduped_count,
        row_count=row_count,
    )

    # 11. Write clean Parquet partition using dynamic partition overwrite at base path
    silver_path.mkdir(parents=True, exist_ok=True)
    deduped_df.write.mode("overwrite").partitionBy("trade_date").parquet(
        silver_path.resolve().as_posix()
    )
    silver_partition = silver_path / f"trade_date={date_str}"
    logger.info(
        "SUCCESS: Wrote %d clean Parquet records to partition: %s", row_count, silver_partition
    )

    duration = round(time.time() - start_time, 2)

    universe_type = "default_list" if symbol_universe is not None else "all_eq"
    symbol_count_val = len(symbol_universe) if symbol_universe is not None else row_count
    reject_rate_pct = (quarantine_count / universe_count * 100.0) if universe_count > 0 else 0.0
    accounted_sum = non_equity_filtered + universe_filtered + quarantine_count + deduped_count + row_count

    checks_meta = [
        {
            "name": "row_count_bounds",
            "passed": True,
            "details": f"Record count {row_count} within bounds [{min_row_count}, {max_row_count}]",
        },
        {
            "name": "partition_date_match",
            "passed": True,
            "details": f"All records match requested partition trade_date {date_str}",
        },
        {
            "name": "duplicate_keys",
            "passed": True,
            "details": f"Zero duplicate keys on (trade_date, symbol, series) out of {row_count} records",
        },
        {
            "name": "high_low_invariant",
            "passed": True,
            "details": "High >= Low satisfied across all records",
        },
        {
            "name": "price_bounds",
            "passed": True,
            "details": "Open, Close, Last strictly within [Low, High] for all records",
        },
        {
            "name": "positive_prices",
            "passed": True,
            "details": "All prices strictly positive (> 0)",
        },
        {
            "name": "volume_non_negative",
            "passed": True,
            "details": "All volume metrics non-negative (>= 0)",
        },
        {
            "name": "reject_rate_gate",
            "passed": True,
            "details": f"Quarantined {quarantine_count} of {universe_count} rows ({reject_rate_pct:.2f}% <= {max_reject_rate_pct:.2f}%)",
        },
        {
            "name": "row_reconciliation_gate",
            "passed": True,
            "details": (
                f"Balanced: read {read_count} = non_equity {non_equity_filtered} + "
                f"universe {universe_filtered} + rejected {quarantine_count} + "
                f"deduped {deduped_count} + kept {row_count} (sum={accounted_sum})"
            ),
        },
    ]

    # 12. Write _SUCCESS-style partition metadata JSON inside partition folder
    success_meta = {
        "trade_date": date_str,
        "read_count": read_count,
        "non_equity_filtered": non_equity_filtered,
        "universe_filtered": universe_filtered,
        "rejected": quarantine_count,
        "deduped": deduped_count,
        "row_count": row_count,
        "universe": universe_type,
        "symbol_count": symbol_count_val,
        "source_file": raw_file.name,
        "source_format": source_format,
        "source_type": source_type,
        "duration_seconds": duration,
        "created_at": datetime.now(UTC).isoformat(),
        "checks": checks_meta,
    }
    success_file = silver_partition / "_SUCCESS.meta.json"
    with open(success_file, "w", encoding="utf-8") as f:
        json.dump(success_meta, f, indent=2)

    metrics = {
        "trade_date": trade_date,
        "read_count": read_count,
        "non_equity_filtered": non_equity_filtered,
        "universe_filtered": universe_filtered,
        "rejected": quarantine_count,
        "rejected_count": quarantine_count,
        "deduped": deduped_count,
        "deduped_count": deduped_count,
        "row_count": row_count,
        "kept_count": row_count,
        "universe": universe_type,
        "symbol_count": symbol_count_val,
        "source_format": source_format,
        "source_type": source_type,
        "duration_seconds": duration,
        "parquet_path": silver_partition,
    }
    return metrics
