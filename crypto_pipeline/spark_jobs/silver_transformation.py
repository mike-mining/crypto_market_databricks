"""
spark_jobs/silver_transformation.py
-------------------------------------
Silver Layer — Cleansing, deduplication, DQ checks, and schema promotion.

Responsibilities:
  - Read only the relevant Bronze partition (processing_date predicate pushdown).
  - Map raw coin_id → canonical ticker symbol via the centralised COIN_SYMBOL_MAP.
  - Enforce non-nullable constraints on business-critical columns.
  - Remove duplicates (last-write-wins on coin_id within a processing_date).
  - Run data quality checks before any write; fail fast rather than land bad data.
  - Write clean records to Silver Delta table via replaceWhere.

DQ checks (configurable thresholds in config/settings.py):
  - Null current_price fraction must be <= MAX_NULL_PRICE_PCT.
  - Zero tolerance for negative prices.
  - Every coin in TRACKED_COINS must appear in the output (completeness check).

Idempotency guarantee:
    .option("replaceWhere", f"processing_date = '{processing_date}'")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import (
    BRONZE_TABLE,
    CATALOG,
    COIN_SYMBOL_MAP,
    MAX_NEGATIVE_PRICE_PCT,
    MAX_NULL_PRICE_PCT,
    SILVER_PATH,
    SILVER_SCHEMA,
    SILVER_TABLE,
    TRACKED_COINS,
)
from config.spark_utils import ensure_schemas_exist, get_processing_date, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Silver schema — strict, non-nullable on business keys.
# ---------------------------------------------------------------------------
SILVER_SCHEMA_DEF = StructType([
    StructField("coin_id",               StringType(),    nullable=False),
    StructField("ticker_symbol",         StringType(),    nullable=False),  # canonical, e.g. "BTC"
    StructField("name",                  StringType(),    nullable=True),
    StructField("current_price_usd",     DoubleType(),    nullable=False),
    StructField("market_cap_usd",        DoubleType(),    nullable=True),
    StructField("total_volume_usd",      DoubleType(),    nullable=True),
    StructField("high_24h_usd",          DoubleType(),    nullable=True),
    StructField("low_24h_usd",           DoubleType(),    nullable=True),
    StructField("price_change_24h_usd",  DoubleType(),    nullable=True),
    StructField("price_change_pct_24h",  DoubleType(),    nullable=True),
    StructField("circulating_supply",    DoubleType(),    nullable=True),
    StructField("total_supply",          DoubleType(),    nullable=True),
    StructField("last_updated_api",      TimestampType(), nullable=True),
    StructField("ingested_at",           TimestampType(), nullable=False),
    StructField("transformed_at",        TimestampType(), nullable=False),
    StructField("processing_date",       StringType(),    nullable=False),
])


# ---------------------------------------------------------------------------
# Transformation logic
# ---------------------------------------------------------------------------

def read_bronze(spark: SparkSession, processing_date: str) -> DataFrame:
    """
    Reads only the relevant Bronze partition. Spark's predicate pushdown will
    restrict file scanning to the single partition directory — no full table scan.
    """
    logger.info("Reading Bronze partition: processing_date=%s", processing_date)
    df = (
        spark.read.format("delta")
        .table(BRONZE_TABLE)
        .filter(F.col("processing_date") == processing_date)
    )
    count = df.count()
    if count == 0:
        raise ValueError(
            f"Bronze partition is empty for processing_date={processing_date}. "
            "Ensure bronze_ingestion completed successfully before running silver."
        )
    logger.info("Bronze records read: %d", count)
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Within a processing_date partition, keep only the most recently ingested
    record per coin_id (last-write-wins). Handles edge case of multiple API
    calls landing in the same Bronze partition.
    """
    window = Window.partitionBy("coin_id", "processing_date").orderBy(F.col("ingested_at").desc())
    return (
        df
        .withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )


def apply_symbol_mapping(df: DataFrame) -> DataFrame:
    """
    Maps raw coin_id values to canonical ticker symbols using the centralised
    COIN_SYMBOL_MAP. Coins not in the map will produce a null ticker_symbol
    and will be caught by the downstream DQ check.
    """
    # Build a Spark map literal from the Python dict for a single-pass column derivation.
    mapping_expr = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in COIN_SYMBOL_MAP.items()] for item in pair]
    )
    return df.withColumn("ticker_symbol", mapping_expr[F.col("coin_id")])


def rename_and_cast(df: DataFrame, transformed_at: datetime) -> DataFrame:
    """Renames columns to Silver naming conventions and casts types."""
    return df.select(
        F.col("coin_id"),
        F.col("ticker_symbol"),
        F.col("name"),
        F.col("current_price").cast(DoubleType()).alias("current_price_usd"),
        F.col("market_cap").cast(DoubleType()).alias("market_cap_usd"),
        F.col("total_volume").cast(DoubleType()).alias("total_volume_usd"),
        F.col("high_24h").cast(DoubleType()).alias("high_24h_usd"),
        F.col("low_24h").cast(DoubleType()).alias("low_24h_usd"),
        F.col("price_change_24h").cast(DoubleType()).alias("price_change_24h_usd"),
        F.col("price_change_pct_24h").cast(DoubleType()),
        F.col("circulating_supply").cast(DoubleType()),
        F.col("total_supply").cast(DoubleType()),
        F.col("last_updated_api"),
        F.col("ingested_at"),
        F.lit(transformed_at).alias("transformed_at"),
        F.col("processing_date"),
    )


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------

def run_dq_checks(df: DataFrame, processing_date: str) -> None:
    """
    Executes all data quality checks. Raises DataQualityError on any failure.
    Fail fast: no Silver data is written if checks do not pass.
    """
    total = df.count()
    if total == 0:
        raise DataQualityError("Silver DataFrame is empty after transformations.")

    # --- Null price check ---
    null_price_count = df.filter(F.col("current_price_usd").isNull()).count()
    null_price_pct = null_price_count / total
    if null_price_pct > MAX_NULL_PRICE_PCT:
        raise DataQualityError(
            f"DQ FAIL: {null_price_pct:.1%} of rows have null current_price_usd "
            f"(threshold: {MAX_NULL_PRICE_PCT:.1%})."
        )
    logger.info("DQ pass — null price fraction: %.2f%%", null_price_pct * 100)

    # --- Negative price check ---
    neg_price_count = df.filter(F.col("current_price_usd") < 0).count()
    neg_price_pct = neg_price_count / total
    if neg_price_pct > MAX_NEGATIVE_PRICE_PCT:
        raise DataQualityError(
            f"DQ FAIL: {neg_price_count} rows have negative current_price_usd."
        )
    logger.info("DQ pass — negative price count: %d", neg_price_count)

    # --- Completeness check: all tracked coins must be present ---
    present_coins = {row["coin_id"] for row in df.select("coin_id").distinct().collect()}
    missing_coins = set(TRACKED_COINS) - present_coins
    if missing_coins:
        # Warning only — a coin may be temporarily delisted; don't block the pipeline.
        logger.warning(
            "DQ WARNING: Missing coins in Silver for %s: %s",
            processing_date,
            sorted(missing_coins),
        )

    # --- Symbol mapping completeness ---
    unmapped = df.filter(F.col("ticker_symbol").isNull()).count()
    if unmapped > 0:
        raise DataQualityError(
            f"DQ FAIL: {unmapped} rows have no ticker_symbol mapping. "
            "Update COIN_SYMBOL_MAP in config/settings.py."
        )
    logger.info("DQ pass — all coins have ticker_symbol mappings.")

    logger.info("All DQ checks passed for processing_date=%s", processing_date)


class DataQualityError(Exception):
    """Raised when a Silver-layer data quality check fails."""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(spark: SparkSession, processing_date: str) -> None:
    logger.info("Silver transformation starting — processing_date=%s", processing_date)

    ensure_schemas_exist(spark, CATALOG, [SILVER_SCHEMA])

    transformed_at = datetime.now(tz=timezone.utc)

    # 1. Read Bronze partition
    bronze_df = read_bronze(spark, processing_date)

    # 2. Deduplicate within partition
    deduped_df = deduplicate(bronze_df)

    # 3. Map coin_id → canonical ticker symbol
    mapped_df = apply_symbol_mapping(deduped_df)

    # 4. Rename / cast to Silver schema
    silver_df = rename_and_cast(mapped_df, transformed_at)

    # 5. Data quality gate — fail fast, no partial writes
    run_dq_checks(silver_df, processing_date)

    logger.info("Writing Silver data to: %s", SILVER_TABLE)

    # 6. Idempotent partition-scoped overwrite
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"processing_date = '{processing_date}'")
        .partitionBy("processing_date")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )

    logger.info("Silver transformation complete — processing_date=%s", processing_date)


if __name__ == "__main__":
    _spark = get_spark("crypto_silver_transformation")
    _processing_date = get_processing_date(_spark)
    run(_spark, _processing_date)
