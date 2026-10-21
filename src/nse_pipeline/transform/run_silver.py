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

from nse_pipeline.ingest.metadata import read_file_metadata
from nse_pipeline.transform.clean import (
    deduplicate_records,
    filter_equities,
    normalize_columns,
    validate_and_split,
)
from nse_pipeline.transform.quality import validate_silver_quality
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
) -> dict[str, Any]:
    """Executes the Bronze-to-Silver transformation pipeline for a single trade date.

    Returns:
        dict containing metrics (read_count, kept_count, rejected_count, deduped_count, etc.)
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

    # 5. Filter for Cash Equity segment
    equity_df = filter_equities(
        normalized_df,
        source_format=source_format,
        allowed_series=allowed_series or ["EQ"],
        symbol_universe=symbol_universe,
    )
    equity_count = equity_df.count()
    logger.info(
        "Filtered %d cash equity records (series %s).", equity_count, allowed_series or ["EQ"]
    )

    # 6. Apply type casting and quarantine invalid rows
    clean_candidates_df, quarantine_df = validate_and_split(equity_df)
    quarantine_count = quarantine_df.count()

    if quarantine_count > 0:
        logger.warning("Quarantined %d invalid records for date %s.", quarantine_count, date_str)
        # Write quarantined records to Parquet
        quarantine_partition = quarantine_path / f"trade_date={date_str}"
        quarantine_partition.mkdir(parents=True, exist_ok=True)
        quarantine_df.write.mode("overwrite").parquet(quarantine_partition.resolve().as_posix())
        logger.info("Saved quarantine Parquet dataset to: %s", quarantine_partition)

    # 7. Deduplicate records
    deduped_df, deduped_count = deduplicate_records(clean_candidates_df)
    kept_count = deduped_df.count()
    logger.info("Retained %d clean records after validation and deduplication.", kept_count)

    # 8. Dynamic bounds checking based on symbol universe vs full market
    if min_row_count is None:
        min_row_count = 40 if symbol_universe else 500
    if max_row_count is None:
        max_row_count = 60 if symbol_universe else 3500

    # 9. Hard Data Quality Gates
    validate_silver_quality(
        deduped_df,
        expected_date=trade_date,
        min_row_count=min_row_count,
        max_row_count=max_row_count,
    )

    # 10. Write clean Parquet partition
    silver_partition = silver_path / f"trade_date={date_str}"
    silver_partition.mkdir(parents=True, exist_ok=True)
    deduped_df.write.mode("overwrite").parquet(silver_partition.resolve().as_posix())
    logger.info("SUCCESS: Wrote %d clean Parquet records to: %s", kept_count, silver_partition)

    duration = round(time.time() - start_time, 2)

    # 11. Write _SUCCESS-style partition metadata JSON
    success_meta = {
        "trade_date": date_str,
        "row_count": kept_count,
        "read_count": read_count,
        "equity_filtered_count": equity_count,
        "rejected_count": quarantine_count,
        "deduped_count": deduped_count,
        "source_file": raw_file.name,
        "source_format": source_format,
        "source_type": source_type,
        "duration_seconds": duration,
        "created_at": datetime.now(UTC).isoformat(),
    }
    success_file = silver_partition / "_SUCCESS.meta.json"
    with open(success_file, "w", encoding="utf-8") as f:
        json.dump(success_meta, f, indent=2)

    metrics = {
        "trade_date": trade_date,
        "read_count": read_count,
        "kept_count": kept_count,
        "rejected_count": quarantine_count,
        "deduped_count": deduped_count,
        "source_format": source_format,
        "source_type": source_type,
        "duration_seconds": duration,
        "parquet_path": silver_partition,
    }
    return metrics
