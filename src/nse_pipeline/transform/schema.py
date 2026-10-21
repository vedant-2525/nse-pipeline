"""Canonical Silver schema definitions and Bronze format detection.

Why this design:
Explicit schemas are required for Bronze ingestion into PySpark. Schema inference reads
files twice and can misinterpret sparse columns. By defining explicit StringType schemas
for each raw format, we read incoming raw files deterministically and delegate type casting
and error interception to the cleaning pipeline.
"""

from pathlib import Path

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class UnsupportedFormatError(Exception):
    """Raised when an unrecognized or auxiliary format (e.g. sec_bhavdata_full) is encountered."""

    pass


# ==============================================================================
# CANONICAL SILVER SCHEMA
# ==============================================================================

SILVER_SCHEMA = StructType(
    [
        StructField("trade_date", DateType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("series", StringType(), nullable=False),
        StructField("isin", StringType(), nullable=True),
        StructField("open", DecimalType(18, 4), nullable=False),
        StructField("high", DecimalType(18, 4), nullable=False),
        StructField("low", DecimalType(18, 4), nullable=False),
        StructField("close", DecimalType(18, 4), nullable=False),
        StructField("last", DecimalType(18, 4), nullable=False),
        StructField("prev_close", DecimalType(18, 4), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("turnover", DecimalType(22, 2), nullable=False),
        StructField("trades", IntegerType(), nullable=True),
        StructField("source_format", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

# ==============================================================================
# BRONZE RAW SCHEMAS (Explicit String Schemas for Safe Ingestion)
# ==============================================================================

# Modern UDiFF Format (post July 2024): 34 columns
UDIFF_RAW_COLUMNS = [
    "TradDt",
    "BizDt",
    "Sgmt",
    "Src",
    "FinInstrmTp",
    "FinInstrmId",
    "ISIN",
    "TckrSymb",
    "SctySrs",
    "XpryDt",
    "FininstrmActlXpryDt",
    "StrkPric",
    "OptnTp",
    "FinInstrmNm",
    "OpnPric",
    "HghPric",
    "LwPric",
    "ClsPric",
    "LastPric",
    "PrvsClsgPric",
    "UndrlygPric",
    "SttlmPric",
    "OpnIntrst",
    "ChngInOpnIntrst",
    "TtlTradgVol",
    "TtlTrfVal",
    "TtlNbOfTxsExctd",
    "SsnId",
    "NewBrdLotQty",
    "Rmks",
    "Rsvd1",
    "Rsvd2",
    "Rsvd3",
    "Rsvd4",
]

UDIFF_RAW_SCHEMA = StructType(
    [StructField(col_name, StringType(), nullable=True) for col_name in UDIFF_RAW_COLUMNS]
)

# Legacy CM Bhavcopy Format (prior to July 2024): 13 named columns + trailing empty column from trailing comma
LEGACY_RAW_COLUMNS = [
    "SYMBOL",
    "SERIES",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "LAST",
    "PREVCLOSE",
    "TOTTRDQTY",
    "TOTTRDVAL",
    "TIMESTAMP",
    "TOTALTRADES",
    "ISIN",
    "_c13",  # Trailing empty column caused by 'ISIN,' trailing comma in raw header
]

LEGACY_RAW_SCHEMA = StructType(
    [StructField(col_name, StringType(), nullable=True) for col_name in LEGACY_RAW_COLUMNS]
)

# Fallback format produced by yfinance
FALLBACK_RAW_COLUMNS = [
    "SYMBOL",
    "SERIES",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "LAST",
    "PREVCLOSE",
    "TOTTRDQTY",
    "TOTTRDVAL",
    "TIMESTAMP",
    "TOTALTRADES",
    "ISIN",
]

FALLBACK_RAW_SCHEMA = StructType(
    [StructField(col_name, StringType(), nullable=True) for col_name in FALLBACK_RAW_COLUMNS]
)


def detect_format(header_line: str, file_path: Path | str | None = None) -> str:
    """Detects the Bronze file format based on header signature and filename.

    Args:
        header_line: The raw first line (header) of the CSV file.
        file_path: Optional path to the file (used to detect fallback naming).

    Returns:
        One of: 'udiff', 'legacy', 'yfinance'

    Raises:
        UnsupportedFormatError: If the file is an auxiliary format (like sec_bhavdata_full)
                                or an unknown schema.
    """
    clean_header = header_line.upper().replace('"', "").strip()
    headers_set = {col.strip() for col in clean_header.split(",") if col.strip()}

    # 1. Check for unsupported auxiliary format (sec_bhavdata_full)
    if "DELIV_QTY" in headers_set or "DELIV_PER" in headers_set or "TURNOVER_LACS" in headers_set:
        file_name = Path(file_path).name if file_path else "unknown"
        raise UnsupportedFormatError(
            f"File '{file_name}' contains security delivery report headers ('DELIV_QTY' / 'DELIV_PER'). "
            "The sec_bhavdata_full format is out of scope for the silver equity price schema."
        )

    # 2. Check for Modern UDiFF standard
    if "TCKRSYMB" in headers_set or "TRADDT" in headers_set:
        return "udiff"

    # 3. Check for Legacy or Fallback standard
    if "TOTTRDQTY" in headers_set or "TOTTRDVAL" in headers_set or "TIMESTAMP" in headers_set:
        if file_path and (
            "yfinance" in str(file_path).lower() or "fallback" in str(file_path).lower()
        ):
            return "yfinance"
        return "legacy"

    file_name = Path(file_path).name if file_path else "unknown"
    raise UnsupportedFormatError(
        f"Unrecognized or unsupported Bronze CSV schema in '{file_name}'. "
        f"Header was: {header_line[:120]}..."
    )


def get_raw_schema_for_format(source_format: str) -> StructType:
    """Returns the explicit PySpark string schema matching the detected source format."""
    if source_format == "udiff":
        return UDIFF_RAW_SCHEMA
    elif source_format == "legacy":
        return LEGACY_RAW_SCHEMA
    elif source_format == "yfinance":
        return FALLBACK_RAW_SCHEMA
    else:
        raise UnsupportedFormatError(f"No raw schema configured for format: '{source_format}'")
