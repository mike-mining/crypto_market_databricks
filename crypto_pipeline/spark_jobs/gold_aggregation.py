"""
spark_jobs/gold_aggregation.py
--------------------------------
Gold Layer — Business-level daily OHLCV aggregations.

Responsibilities:
  - Read the relevant Silver partition (predicate pushdown, single partition scan).
  - Produce daily OHLCV (Open/High/Low/Close/Volume) per ticker symbol.
  - Include market cap and supply metrics for portfolio analytics consumers.
  - Write to Gold Delta table via replaceWhere (idempotent, partition-scoped).

Gold table is the serving layer — optimised for downstream BI / analytics
tools (Databricks SQL, PowerBI, etc.) and is NOT updated incrementally;
the full partition is atomically replaced on each run.

Consumers of this table should reference processing_date as the business date
and should NOT assume intraday tick data (CoinGecko free tier is daily snapshots).

Idempotency guarantee:
    .option("replaceWhere", f"processing_date = '{processing_date}'")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import (
    CATALOG,
    GOLD_PATH,
    GOLD_SCHEMA,
    GOLD_TABLE,
    SILVER_TABLE,
)
from config.spark_utils import ensure_schemas_exist, get_processing_date, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Gold schema
# ---------------------------------------------------------------------------
GOLD_SCHEMA_DEF = StructType([
    StructField("processing_date",        StringType(),    nullable=False),  # partition key
    StructField("business_date",          DateType(),      nullable=False),  # typed date for BI tools
    StructField("ticker_symbol",          StringType(),    nullable=False),
    StructField("coin_id",                StringType(),    nullable=False),
    StructField("name",                   StringType(),    nullable=True),
    # Price OHLCV — 'Open' and 'Close' here refer to the daily snapshot price bounds
    # since CoinGecko free-tier provides a single daily price point, not tick data.
    # high_24h / low_24h are used as the H/L; current_price is the Close equivalent.
    StructField("price_close_usd",        DoubleType(),    nullable=True),
    StructField("price_high_usd",         DoubleType(),    nullable=True),
    StructField("price_low_usd",          DoubleType(),    nullable=True),
    StructField("price_change_24h_usd",   DoubleType(),    nullable=True),
    StructField("price_change_pct_24h",   DoubleType(),    nullable=True),
    # Volume & market metrics
    StructField("volume_usd",             DoubleType(),    nullable=True),
    StructField("market_cap_usd",         DoubleType(),    nullable=True),
    StructField("circulating_supply",     DoubleType(),    nullable=True),
    StructField("total_supply",           DoubleType(),    nullable=True),
    # Derived metrics
    StructField("volume_to_market_cap",   DoubleType(),    nullable=True),   # liquidity proxy
    # Audit
    StructField("aggregated_at",          TimestampType(), nullable=False),
    StructField("source_record_count",    LongType(),      nullable=False),   # input row count per coin
])


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def read_silver(spark: SparkSession, processing_date: str) -> DataFrame:
    """Reads only the relevant Silver partition."""
    logger.info("Reading Silver partition: processing_date=%s", processing_date)
    df = (
        spark.read.format("delta")
        .table(SILVER_TABLE)
        .filter(F.col("processing_date") == processing_date)
    )
    count = df.count()
    if count == 0:
        raise ValueError(
            f"Silver partition is empty for processing_date={processing_date}. "
            "Ensure silver_transformation completed successfully before running gold."
        )
    logger.info("Silver records read: %d", count)
    return df


def build_gold_aggregations(df: DataFrame, processing_date: str, aggregated_at: datetime) -> DataFrame:
    """
    Produces one Gold row per coin per processing_date.

    CoinGecko's free-tier markets endpoint returns a single daily snapshot
    (not intraday ticks). The 24h high/low fields represent the true intraday
    range as reported by CoinGecko. current_price is the Close equivalent.
    """
    gold_df = df.groupBy(
        "processing_date",
        "ticker_symbol",
        "coin_id",
        "name",
    ).agg(
        # Price metrics — last() gives the most recent snapshot if multiple rows exist per coin.
        F.last("current_price_usd",   ignorenulls=True).alias("price_close_usd"),
        F.last("high_24h_usd",        ignorenulls=True).alias("price_high_usd"),
        F.last("low_24h_usd",         ignorenulls=True).alias("price_low_usd"),
        F.last("price_change_24h_usd",ignorenulls=True).alias("price_change_24h_usd"),
        F.last("price_change_pct_24h",ignorenulls=True).alias("price_change_pct_24h"),
        # Market metrics
        F.last("total_volume_usd",    ignorenulls=True).alias("volume_usd"),
        F.last("market_cap_usd",      ignorenulls=True).alias("market_cap_usd"),
        F.last("circulating_supply",  ignorenulls=True).alias("circulating_supply"),
        F.last("total_supply",        ignorenulls=True).alias("total_supply"),
        # Audit
        F.count("*").alias("source_record_count"),
    )

    # Derived metrics
    gold_df = gold_df.withColumn(
        "volume_to_market_cap",
        F.when(
            F.col("market_cap_usd").isNotNull() & (F.col("market_cap_usd") > 0),
            F.col("volume_usd") / F.col("market_cap_usd"),
        ).otherwise(F.lit(None).cast(DoubleType())),
    )

    # Typed business_date column for BI tool date filtering
    gold_df = gold_df.withColumn(
        "business_date",
        F.to_date(F.col("processing_date"), "yyyy-MM-dd"),
    )

    # Audit timestamp
    gold_df = gold_df.withColumn("aggregated_at", F.lit(aggregated_at).cast(TimestampType()))

    # Column order aligned to GOLD_SCHEMA_DEF
    return gold_df.select(
        "processing_date",
        "business_date",
        "ticker_symbol",
        "coin_id",
        "name",
        "price_close_usd",
        "price_high_usd",
        "price_low_usd",
        "price_change_24h_usd",
        "price_change_pct_24h",
        "volume_usd",
        "market_cap_usd",
        "circulating_supply",
        "total_supply",
        "volume_to_market_cap",
        "aggregated_at",
        "source_record_count",
    )


def validate_gold(df: DataFrame, processing_date: str) -> None:
    """Lightweight Gold DQ: ensure no duplicate ticker_symbols in the output."""
    total = df.count()
    distinct = df.select("ticker_symbol").distinct().count()
    if total != distinct:
        dupes = total - distinct
        raise ValueError(
            f"Gold validation failed: {dupes} duplicate ticker_symbol rows detected "
            f"for processing_date={processing_date}. Investigate Silver deduplication."
        )
    logger.info("Gold DQ pass — %d unique tickers in output.", distinct)


def ensure_gold_table_properties(spark: SparkSession) -> None:
    """
    Applies Delta table optimisations after first creation:
      - ZORDER on ticker_symbol for fast per-coin queries.
      - data skipping statistics on business_date.
    These are idempotent — safe to call on every run.
    """
    try:
        spark.sql(f"""
            ALTER TABLE {GOLD_TABLE}
            SET TBLPROPERTIES (
                'delta.dataSkippingNumIndexedCols' = '4',
                'delta.autoOptimize.optimizeWrite'  = 'true',
                'delta.autoOptimize.autoCompact'    = 'true'
            )
        """)
        logger.info("Gold table properties set on %s", GOLD_TABLE)
    except Exception as exc:
        # Non-fatal: table may not exist yet on very first run before saveAsTable.
        logger.warning("Could not set Gold table properties (may be first run): %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(spark: SparkSession, processing_date: str) -> None:
    logger.info("Gold aggregation starting — processing_date=%s", processing_date)

    ensure_schemas_exist(spark, CATALOG, [GOLD_SCHEMA])

    aggregated_at = datetime.now(tz=timezone.utc)

    # 1. Read Silver partition
    silver_df = read_silver(spark, processing_date)

    # 2. Build aggregations
    gold_df = build_gold_aggregations(silver_df, processing_date, aggregated_at)

    # 3. Validate output
    validate_gold(gold_df, processing_date)

    logger.info("Writing Gold data to: %s", GOLD_TABLE)

    # 4. Idempotent partition-scoped overwrite
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"processing_date = '{processing_date}'")
        .partitionBy("processing_date")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )

    # 5. Set Delta table optimisation properties (idempotent)
    ensure_gold_table_properties(spark)

    logger.info("Gold aggregation complete — processing_date=%s", processing_date)


if __name__ == "__main__":
    _spark = get_spark("crypto_gold_aggregation")
    _processing_date = get_processing_date(_spark)
    run(_spark, _processing_date)
