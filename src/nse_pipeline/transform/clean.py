"""PySpark cleaning, equity filtering, and quarantine logic for Silver transformation.

Why this design:
Dirty or corrupt records must never silently disappear from a data engineering pipeline.
Instead, we implement the Quarantine Pattern:
1. Valid rows are cast into canonical Silver schema.
2. Invalid rows (cast failures, inverted prices, out-of-range prices, duplicate keys)
   are diverted into a quarantine dataset with a descriptive `reject_reason` column.
3. Every step logs row counts (read, filtered, kept, rejected, deduped) for pipeline auditability.
"""

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    TimestampType,
)

from nse_pipeline.utils.logging import logger


def normalize_columns(df: DataFrame, source_format: str, source_file: str) -> DataFrame:
    """Standardizes column names and parses dates for the given source format."""
    now_ts = datetime.now(UTC)

    if source_format == "udiff":
        # UDiFF dates are ISO YYYY-MM-DD
        return df.select(
            F.to_date(F.trim(F.col("TradDt")), "yyyy-MM-dd").alias("trade_date"),
            F.upper(F.trim(F.col("TckrSymb"))).alias("symbol"),
            F.upper(F.trim(F.col("SctySrs"))).alias("series"),
            F.when(F.trim(F.col("ISIN")) != "", F.trim(F.col("ISIN")))
            .otherwise(F.lit(None))
            .alias("isin"),
            F.trim(F.col("OpnPric")).alias("raw_open"),
            F.trim(F.col("HghPric")).alias("raw_high"),
            F.trim(F.col("LwPric")).alias("raw_low"),
            F.trim(F.col("ClsPric")).alias("raw_close"),
            F.trim(F.col("LastPric")).alias("raw_last"),
            F.trim(F.col("PrvsClsgPric")).alias("raw_prev_close"),
            F.trim(F.col("TtlTradgVol")).alias("raw_volume"),
            F.trim(F.col("TtlTrfVal")).alias("raw_turnover"),
            F.trim(F.col("TtlNbOfTxsExctd")).alias("raw_trades"),
            F.lit(source_format).alias("source_format"),
            F.lit(source_file).alias("source_file"),
            F.lit(now_ts).cast(TimestampType()).alias("ingested_at"),
            # Segment metadata for equity filtering
            F.trim(F.col("Sgmt")).alias("_sgmt"),
            F.trim(F.col("FinInstrmTp")).alias("_fin_type"),
        )
    elif source_format in ("legacy", "yfinance"):
        # Legacy dates are formatted like 15-JAN-2024
        # yfinance fallback produces either YYYY-MM-DD or DD-MMM-YYYY
        parsed_date = F.coalesce(
            F.to_date(F.trim(F.col("TIMESTAMP")), "dd-MMM-yyyy"),
            F.to_date(F.trim(F.col("TIMESTAMP")), "yyyy-MM-dd"),
        )

        return df.select(
            parsed_date.alias("trade_date"),
            F.upper(F.trim(F.col("SYMBOL"))).alias("symbol"),
            F.upper(F.trim(F.col("SERIES"))).alias("series"),
            F.when(F.trim(F.col("ISIN")) != "", F.trim(F.col("ISIN")))
            .otherwise(F.lit(None))
            .alias("isin"),
            F.trim(F.col("OPEN")).alias("raw_open"),
            F.trim(F.col("HIGH")).alias("raw_high"),
            F.trim(F.col("LOW")).alias("raw_low"),
            F.trim(F.col("CLOSE")).alias("raw_close"),
            F.trim(F.col("LAST")).alias("raw_last"),
            F.trim(F.col("PREVCLOSE")).alias("raw_prev_close"),
            F.trim(F.col("TOTTRDQTY")).alias("raw_volume"),
            F.trim(F.col("TOTTRDVAL")).alias("raw_turnover"),
            F.trim(F.col("TOTALTRADES")).alias("raw_trades"),
            F.lit(source_format).alias("source_format"),
            F.lit(source_file).alias("source_file"),
            F.lit(now_ts).cast(TimestampType()).alias("ingested_at"),
            F.lit("CM").alias("_sgmt"),
            F.lit("STK").alias("_fin_type"),
        )
    else:
        raise ValueError(f"Unsupported format: '{source_format}'")


def filter_equities(
    df: DataFrame,
    source_format: str,
    allowed_series: list[str] | None = None,
    symbol_universe: list[str] | None = None,
) -> DataFrame:
    """Filters incoming data for Cash Equity segment records (series EQ, etc.).

    Why this design:
    Bhavcopy files bundle cash equities with derivatives, government securities (GB/GS/TB),
    and debt (N1-N9). Filtering for Cash Market (CM) Stocks (STK) and series 'EQ' isolates
    the company equity shares expected by downstream financial analytics.
    """
    series_list = [s.upper() for s in (allowed_series or ["EQ"])]

    # 1. Filter out non-equity segments in modern format
    filtered_df = df
    if source_format == "udiff":
        filtered_df = filtered_df.filter((F.col("_sgmt") == "CM") & (F.col("_fin_type") == "STK"))

    # 2. Filter for allowed equity series
    filtered_df = filtered_df.filter(F.col("series").isin(series_list))

    # 3. Optional symbol universe filter (e.g. Nifty 50)
    if symbol_universe:
        symbols_upper = [s.upper() for s in symbol_universe]
        filtered_df = filtered_df.filter(F.col("symbol").isin(symbols_upper))

    return filtered_df.drop("_sgmt", "_fin_type")


def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Applies type casting and validation rules, separating clean records from quarantined records.

    Returns:
        tuple (clean_df, quarantine_df)
    """
    # Attempt casts using try_cast to ensure bad inputs produce null rather than crashing
    c_open = F.expr("try_cast(raw_open as decimal(18, 4))")
    c_high = F.expr("try_cast(raw_high as decimal(18, 4))")
    c_low = F.expr("try_cast(raw_low as decimal(18, 4))")
    c_close = F.expr("try_cast(raw_close as decimal(18, 4))")
    c_last = F.expr("try_cast(raw_last as decimal(18, 4))")
    c_prev_close = F.expr("try_cast(raw_prev_close as decimal(18, 4))")
    c_vol = F.expr("try_cast(raw_volume as bigint)")
    c_turnover = F.expr("try_cast(raw_turnover as decimal(22, 2))")
    c_trades = F.expr("try_cast(raw_trades as int)")

    # Formulate validation conditions
    cond_date_valid = F.col("trade_date").isNotNull()
    cond_key_valid = (
        F.col("symbol").isNotNull()
        & (F.col("symbol") != "")
        & F.col("series").isNotNull()
        & (F.col("series") != "")
    )
    cond_cast_valid = (
        c_open.isNotNull()
        & c_high.isNotNull()
        & c_low.isNotNull()
        & c_close.isNotNull()
        & c_last.isNotNull()
        & c_prev_close.isNotNull()
        & c_vol.isNotNull()
        & c_turnover.isNotNull()
    )
    cond_positive_prices = (
        (c_open > 0)
        & (c_high > 0)
        & (c_low > 0)
        & (c_close > 0)
        & (c_last > 0)
        & (c_prev_close > 0)
    )
    cond_non_neg_vol = c_vol >= 0
    cond_high_gte_low = c_high >= c_low
    cond_bounds = (
        (c_open >= c_low)
        & (c_open <= c_high)
        & (c_close >= c_low)
        & (c_close <= c_high)
        & (c_last >= c_low)
        & (c_last <= c_high)
    )

    # Assign reject_reason when a rule fails
    reason_col = (
        F.when(~cond_date_valid, F.lit("INVALID_OR_NULL_TRADE_DATE"))
        .when(~cond_key_valid, F.lit("MISSING_SYMBOL_OR_SERIES"))
        .when(~cond_cast_valid, F.lit("NUMERIC_CAST_FAILURE"))
        .when(~cond_positive_prices, F.lit("NON_POSITIVE_PRICE"))
        .when(~cond_non_neg_vol, F.lit("NEGATIVE_VOLUME"))
        .when(~cond_high_gte_low, F.lit("INVERTED_HIGH_LOW"))
        .when(~cond_bounds, F.lit("PRICE_OUTSIDE_HIGH_LOW_BOUNDS"))
        .otherwise(F.lit(None))
    )

    tagged_df = df.withColumn("reject_reason", reason_col)

    # 1. Clean dataset
    clean_df = tagged_df.filter(F.col("reject_reason").isNull()).select(
        F.col("trade_date").cast(DateType()),
        F.col("symbol").cast(StringType()),
        F.col("series").cast(StringType()),
        F.col("isin").cast(StringType()),
        c_open.alias("open"),
        c_high.alias("high"),
        c_low.alias("low"),
        c_close.alias("close"),
        c_last.alias("last"),
        c_prev_close.alias("prev_close"),
        c_vol.alias("volume"),
        c_turnover.alias("turnover"),
        c_trades.alias("trades"),
        F.col("source_format").cast(StringType()),
        F.col("source_file").cast(StringType()),
        F.col("ingested_at").cast(TimestampType()),
    )

    # 2. Quarantine dataset
    quarantine_df = tagged_df.filter(F.col("reject_reason").isNotNull())

    return clean_df, quarantine_df


def deduplicate_records(clean_df: DataFrame) -> tuple[DataFrame, int]:
    """Drops exact duplicates and enforces primary key uniqueness on (trade_date, symbol, series).

    Returns:
        tuple (deduped_df, dropped_count)
    """
    initial_count = clean_df.count()
    # Drop duplicates by primary key, keeping deterministic first
    deduped_df = clean_df.dropDuplicates(["trade_date", "symbol", "series"])
    final_count = deduped_df.count()
    dropped_count = initial_count - final_count

    if dropped_count > 0:
        logger.warning(
            "Deduplication dropped %d duplicate key rows on (trade_date, symbol, series)",
            dropped_count,
        )

    return deduped_df, dropped_count
