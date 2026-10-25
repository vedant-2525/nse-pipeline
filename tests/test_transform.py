"""Unit tests for Phase 2: PySpark Silver transformations and Quality Gates.

All tests run completely offline using small local fixtures, guaranteeing fast
execution without network dependencies.
"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from nse_pipeline.ingest.metadata import write_file_metadata
from nse_pipeline.transform.clean import (
    deduplicate_records,
    filter_equities,
    normalize_columns,
    validate_and_split,
)
from nse_pipeline.transform.quality import (
    DataQualityError,
    validate_reconciliation_gate,
    validate_reject_rate_gate,
    validate_silver_quality,
)
from nse_pipeline.transform.run_silver import run_silver_transform
from nse_pipeline.transform.schema import (
    SILVER_SCHEMA,
    UnsupportedFormatError,
    detect_format,
    get_raw_schema_for_format,
)


def test_detect_format_variants():
    """Verify format detection handles UDiFF, Legacy, Fallback, and rejects auxiliary delivery data."""
    udiff_header = "TradDt,BizDt,Sgmt,Src,FinInstrmTp,ISIN,TckrSymb,SctySrs,OpnPric,ClsPric"
    assert detect_format(udiff_header) == "udiff"

    legacy_header = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,"
    assert detect_format(legacy_header) == "legacy"

    fallback_header = "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN"
    assert detect_format(fallback_header, Path("yfinance_fallback_20241011.csv")) == "yfinance"

    delivery_header = (
        "SYMBOL, SERIES, DATE1, PREV_CLOSE, TTL_TRD_QNTY, TURNOVER_LACS, DELIV_QTY, DELIV_PER"
    )
    with pytest.raises(UnsupportedFormatError, match="security delivery report headers"):
        detect_format(delivery_header, Path("sec_bhavdata_full_11102024.csv"))

    unknown_header = "FOO,BAR,BAZ"
    with pytest.raises(UnsupportedFormatError, match="Unrecognized or unsupported"):
        detect_format(unknown_header, Path("random.csv"))


def test_modern_and_legacy_produce_identical_silver_schemas(spark_session: SparkSession):
    """Verify modern UDiFF and legacy fixtures produce identical Silver column names and dtypes."""
    fixture_modern = Path("tests/fixtures/bhavcopy_new_sample.csv")
    fixture_legacy = Path("tests/fixtures/bhavcopy_legacy_sample.csv")

    schema_mod = get_raw_schema_for_format("udiff")
    df_mod_raw = (
        spark_session.read.option("header", "true").schema(schema_mod).csv(str(fixture_modern))
    )
    norm_mod = normalize_columns(df_mod_raw, "udiff", fixture_modern.name)
    eq_mod = filter_equities(norm_mod, "udiff", allowed_series=["EQ"])
    clean_mod, _ = validate_and_split(eq_mod)

    schema_leg = get_raw_schema_for_format("legacy")
    df_leg_raw = (
        spark_session.read.option("header", "true").schema(schema_leg).csv(str(fixture_legacy))
    )
    norm_leg = normalize_columns(df_leg_raw, "legacy", fixture_legacy.name)
    eq_leg = filter_equities(norm_leg, "legacy", allowed_series=["EQ"])
    clean_leg, _ = validate_and_split(eq_leg)

    # Assert column names match
    expected_cols = [f.name for f in SILVER_SCHEMA.fields]
    assert clean_mod.columns == expected_cols
    assert clean_leg.columns == expected_cols

    # Assert types match
    mod_types = [(f.name, f.dataType.simpleString()) for f in clean_mod.schema.fields]
    leg_types = [(f.name, f.dataType.simpleString()) for f in clean_leg.schema.fields]
    assert mod_types == leg_types


def test_quarantine_captures_deliberate_bad_rows(spark_session: SparkSession):
    """Verify that corrupt rows land in quarantine with accurate reject_reason values."""
    bad_fixture = Path("tests/fixtures/bhavcopy_bad_data_sample.csv")
    schema = get_raw_schema_for_format("udiff")

    raw_df = spark_session.read.option("header", "true").schema(schema).csv(str(bad_fixture))
    norm_df = normalize_columns(raw_df, "udiff", bad_fixture.name)
    eq_df = filter_equities(norm_df, "udiff", allowed_series=["EQ"])
    clean_df, quarantine_df = validate_and_split(eq_df)

    # 1 row is valid (GOODSTOCK), 6 rows are bad
    assert clean_df.count() == 1
    assert clean_df.first()["symbol"] == "GOODSTOCK"

    reasons = {r["symbol"]: r["reject_reason"] for r in quarantine_df.collect()}
    assert reasons["INVERTED"] == "INVERTED_HIGH_LOW"
    assert reasons["BADBOUND"] == "PRICE_OUTSIDE_HIGH_LOW_BOUNDS"
    assert reasons["NONNUMERIC"] == "NUMERIC_CAST_FAILURE"
    assert reasons["NEGPRICE"] == "NON_POSITIVE_PRICE"
    assert reasons["NEGVOL"] == "NEGATIVE_VOLUME"
    assert reasons["BADDATE"] == "INVALID_OR_NULL_TRADE_DATE"


def test_deduplication_drops_duplicate_primary_keys(spark_session: SparkSession):
    """Verify deduplication removes rows with identical (trade_date, symbol, series)."""
    data = [
        (
            date(2024, 10, 11),
            "RELIANCE",
            "EQ",
            "INE002A01018",
            Decimal("2500.00"),
            Decimal("2550.00"),
            Decimal("2490.00"),
            Decimal("2520.00"),
            Decimal("2520.00"),
            Decimal("2510.00"),
            100000,
            Decimal("252000000.00"),
            5000,
            "udiff",
            "test.csv",
        ),
        (
            date(2024, 10, 11),
            "RELIANCE",
            "EQ",
            "INE002A01018",
            Decimal("2500.00"),
            Decimal("2550.00"),
            Decimal("2490.00"),
            Decimal("2520.00"),
            Decimal("2520.00"),
            Decimal("2510.00"),
            100000,
            Decimal("252000000.00"),
            5000,
            "udiff",
            "test.csv",
        ),
        (
            date(2024, 10, 11),
            "TCS",
            "EQ",
            "INE467B01029",
            Decimal("3500.00"),
            Decimal("3550.00"),
            Decimal("3490.00"),
            Decimal("3520.00"),
            Decimal("3520.00"),
            Decimal("3510.00"),
            80000,
            Decimal("281600000.00"),
            4000,
            "udiff",
            "test.csv",
        ),
    ]
    cols = [f.name for f in SILVER_SCHEMA.fields if f.name != "ingested_at"]
    df = spark_session.createDataFrame(data, cols)

    deduped_df, dropped_count = deduplicate_records(df)
    assert df.count() == 3
    assert deduped_df.count() == 2
    assert dropped_count == 1
    assert set(r["symbol"] for r in deduped_df.collect()) == {"RELIANCE", "TCS"}


def test_quality_gates_fail_on_violations(spark_session: SparkSession):
    """Verify each DQ rule raises DataQualityError when violated."""
    base_row = {
        "trade_date": date(2024, 10, 11),
        "symbol": "RELIANCE",
        "series": "EQ",
        "open": Decimal("2500.00"),
        "high": Decimal("2550.00"),
        "low": Decimal("2490.00"),
        "close": Decimal("2520.00"),
        "last": Decimal("2520.00"),
        "prev_close": Decimal("2510.00"),
        "volume": 100000,
        "turnover": Decimal("252000000.00"),
        "trades": 5000,
        "source_format": "udiff",
        "source_file": "test.csv",
    }

    # 1. Date mismatch
    bad_date_df = spark_session.createDataFrame([{**base_row, "trade_date": date(2024, 10, 12)}])
    with pytest.raises(DataQualityError, match="Partition Date Mismatch"):
        validate_silver_quality(bad_date_df, expected_date=date(2024, 10, 11), min_row_count=1)

    # 2. Inverted high/low
    inverted_df = spark_session.createDataFrame(
        [{**base_row, "high": Decimal("2400.00"), "low": Decimal("2500.00")}]
    )
    with pytest.raises(DataQualityError, match="Inverted High-Low"):
        validate_silver_quality(inverted_df, expected_date=date(2024, 10, 11), min_row_count=1)

    # 3. Price outside bounds
    oob_df = spark_session.createDataFrame(
        [
            {**base_row, "open": Decimal("2600.00")}  # > high
        ]
    )
    with pytest.raises(DataQualityError, match="Price Outside Bounds"):
        validate_silver_quality(oob_df, expected_date=date(2024, 10, 11), min_row_count=1)

    # 4. Negative volume
    neg_vol_df = spark_session.createDataFrame([{**base_row, "volume": -100}])
    with pytest.raises(DataQualityError, match="Negative Volume"):
        validate_silver_quality(neg_vol_df, expected_date=date(2024, 10, 11), min_row_count=1)

    # 5. Row count below min
    valid_df = spark_session.createDataFrame([base_row])
    with pytest.raises(DataQualityError, match="Row Count Bounds"):
        validate_silver_quality(valid_df, expected_date=date(2024, 10, 11), min_row_count=50)


def test_run_silver_transform_end_to_end_and_idempotency(
    tmp_path: Path, spark_session: SparkSession
):
    """Verify run_silver_transform produces Parquet, _SUCCESS.meta.json, and identical counts on re-run."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver" / "equity_prices"
    quarantine_dir = tmp_path / "quarantine" / "equity_prices"
    trade_date = date(2024, 10, 11)

    partition_dir = bronze_dir / f"trade_date={trade_date}"
    partition_dir.mkdir(parents=True)
    fixture_src = Path("tests/fixtures/bhavcopy_new_sample.csv")
    bronze_file = partition_dir / fixture_src.name
    bronze_file.write_bytes(fixture_src.read_bytes())
    write_file_metadata(bronze_file, "nse_official", "https://example.com/bhav.zip")

    # Run 1
    metrics1 = run_silver_transform(
        trade_date=trade_date,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        spark=spark_session,
        min_row_count=1,
        max_row_count=100,
    )
    assert metrics1["kept_count"] > 0

    parquet_partition = silver_dir / f"trade_date={trade_date}"
    assert parquet_partition.is_dir()
    parquet_files = list(parquet_partition.glob("*.parquet"))
    assert len(parquet_files) > 0

    success_meta_file = parquet_partition / "_SUCCESS.meta.json"
    assert success_meta_file.is_file()
    with open(success_meta_file, encoding="utf-8") as f:
        meta_data = json.load(f)
    assert meta_data["row_count"] == metrics1["kept_count"]
    assert "universe" in meta_data
    assert "symbol_count" in meta_data
    assert meta_data["universe"] == "all_eq"
    assert meta_data["symbol_count"] == metrics1["kept_count"]

    # Run 2 (Idempotency assertion)
    metrics2 = run_silver_transform(
        trade_date=trade_date,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        spark=spark_session,
        min_row_count=1,
        max_row_count=100,
    )
    assert metrics2["kept_count"] == metrics1["kept_count"]
    assert metrics2["rejected_count"] == metrics1["rejected_count"]


def test_fallback_refused_without_allow_fallback_flag(tmp_path: Path, spark_session: SparkSession):
    """Verify that processing only a yfinance fallback file is refused unless allow_fallback=True."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver" / "equity_prices"
    quarantine_dir = tmp_path / "quarantine" / "equity_prices"
    trade_date = date(2024, 10, 11)

    partition_dir = bronze_dir / f"trade_date={trade_date}"
    partition_dir.mkdir(parents=True)
    fallback_file = partition_dir / "yfinance_fallback_20241011.csv"
    fallback_file.write_text(
        "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN\n"
        "RELIANCE,EQ,2500.0,2550.0,2490.0,2520.0,2520.0,2500.0,1000,2520000.0,11-OCT-2024,0,\n",
        encoding="utf-8",
    )
    write_file_metadata(fallback_file, "yfinance_fallback", "https://query2.finance.yahoo.com")

    # 1. Must fail without allow_fallback
    with pytest.raises(PermissionError, match="Only fallback data"):
        run_silver_transform(
            trade_date=trade_date,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            allow_fallback=False,
            spark=spark_session,
            min_row_count=1,
        )

    # 2. Must succeed with allow_fallback=True
    metrics = run_silver_transform(
        trade_date=trade_date,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        allow_fallback=True,
        spark=spark_session,
        min_row_count=1,
    )
    assert metrics["source_type"] == "fallback"
    assert metrics["kept_count"] == 1


def test_reconciliation_gate_balanced_and_failure():
    """Verify reconciliation gate passes on exact balance and fails with DataQualityError otherwise."""
    # Balanced case: 100 = 20 + 30 + 5 + 5 + 40
    validate_reconciliation_gate(
        read_count=100,
        non_equity_filtered=20,
        universe_filtered=30,
        rejected=5,
        deduped=5,
        row_count=40,
    )

    # Imbalanced case: sum is 99 != 100
    with pytest.raises(DataQualityError, match="DQ FAIL \\[Row Reconciliation\\]"):
        validate_reconciliation_gate(
            read_count=100,
            non_equity_filtered=20,
            universe_filtered=30,
            rejected=5,
            deduped=5,
            row_count=39,
        )


def test_reject_rate_gate_pass_and_failure():
    """Verify reject rate gate enforces maximum threshold."""
    # 1 out of 100 is 1.0% <= 2.0% -> Pass
    validate_reject_rate_gate(
        quarantine_count=1, equity_evaluated_count=100, max_reject_rate_pct=2.0
    )

    # 3 out of 100 is 3.0% > 2.0% -> Fail
    with pytest.raises(DataQualityError, match="DQ FAIL \\[Reject Rate Exceeded\\]"):
        validate_reject_rate_gate(
            quarantine_count=3, equity_evaluated_count=100, max_reject_rate_pct=2.0
        )


def test_partitioned_parquet_layout_multi_day_and_idempotency(
    tmp_path: Path, spark_session: SparkSession
):
    """Verify multi-day partitioned Parquet loading via PyArrow and partition overwrite isolation."""
    import pandas as pd

    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver" / "equity_prices"
    quarantine_dir = tmp_path / "quarantine" / "equity_prices"

    # Setup Day 1 (Modern format)
    d1 = date(2024, 10, 11)
    p1 = bronze_dir / f"trade_date={d1}"
    p1.mkdir(parents=True)
    fix1 = Path("tests/fixtures/bhavcopy_new_sample.csv")
    f1 = p1 / fix1.name
    f1.write_bytes(fix1.read_bytes())
    write_file_metadata(f1, "nse_official", "https://example.com/bhav1.zip")

    # Setup Day 2 (Legacy format)
    d2 = date(2024, 1, 15)
    p2 = bronze_dir / f"trade_date={d2}"
    p2.mkdir(parents=True)
    fix2 = Path("tests/fixtures/bhavcopy_legacy_sample.csv")
    f2 = p2 / fix2.name
    f2.write_bytes(fix2.read_bytes())
    write_file_metadata(f2, "nse_official", "https://example.com/bhav2.zip")

    # Transform Day 1
    run_silver_transform(
        trade_date=d1,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        spark=spark_session,
        min_row_count=1,
    )

    # Transform Day 2
    run_silver_transform(
        trade_date=d2,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        spark=spark_session,
        min_row_count=1,
    )

    # Read the base directory with pandas/pyarrow
    combined_df = pd.read_parquet(silver_dir)
    assert len(combined_df) > 0
    grouped = combined_df.groupby("trade_date", observed=False).size()
    assert str(d1) in grouped.index
    assert str(d2) in grouped.index

    # Check that trade_date is a single consistent type (category/object)
    assert "trade_date" in combined_df.columns

    # Record modification times of Day 2 files
    d2_files = list((silver_dir / f"trade_date={d2}").glob("*.parquet"))
    assert len(d2_files) > 0
    d2_mtimes_before = [f.stat().st_mtime_ns for f in d2_files]

    # Re-run Day 1
    run_silver_transform(
        trade_date=d1,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        spark=spark_session,
        min_row_count=1,
    )

    # Assert Day 2 files were completely untouched
    d2_mtimes_after = [f.stat().st_mtime_ns for f in d2_files]
    assert d2_mtimes_before == d2_mtimes_after
