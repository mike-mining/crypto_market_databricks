"""
spark_jobs/bronze_ingestion.py
-------------------------------
Bronze Layer — Raw ingestion from CoinGecko API.

Responsibilities:
  - Fetch raw market data for all tracked coins via CoinGecko /coins/markets.
  - Enforce a minimal Bronze schema (no business transformations here).
  - Write to Delta Lake partitioned by processing_date using replaceWhere for
    idempotent, partition-scoped overwrites (safe re-runs / backfills).
  - Preserve a raw_payload column (JSON string) for full auditability.

Idempotency guarantee:
    .option("replaceWhere", f"processing_date = '{processing_date}'")
    Re-running for the same date atomically replaces only that partition.

Usage:
    spark-submit spark_jobs/bronze_ingestion.py 2026-03-17
    # Or via Airflow DatabricksSubmitRunOperator with notebook_params={"processing_date": "{{ ds }}"}
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Local imports — works when spark-submitted with --py-files or in a Databricks repo.
from config.settings import (
    API_PAGE_SIZE,
    API_RETRY_ATTEMPTS,
    API_RETRY_BACKOFF_S,
    BRONZE_PATH,
    BRONZE_SCHEMA,
    BRONZE_TABLE,
    CATALOG,
    COINGECKO_COINS_ENDPOINT,
    CURRENCY,
    TRACKED_COINS,
)
from config.spark_utils import ensure_schemas_exist, get_processing_date, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bronze schema — deliberately permissive; Silver enforces business rules.
# All numeric fields are nullable to survive API quirks without data loss.
# ---------------------------------------------------------------------------
BRONZE_SCHEMA_DEF = StructType([
    StructField("coin_id",                StringType(),    nullable=False),
    StructField("symbol",                 StringType(),    nullable=True),
    StructField("name",                   StringType(),    nullable=True),
    StructField("current_price",          DoubleType(),    nullable=True),
    StructField("market_cap",             DoubleType(),    nullable=True),
    StructField("total_volume",           DoubleType(),    nullable=True),
    StructField("high_24h",               DoubleType(),    nullable=True),
    StructField("low_24h",                DoubleType(),    nullable=True),
    StructField("price_change_24h",       DoubleType(),    nullable=True),
    StructField("price_change_pct_24h",   DoubleType(),    nullable=True),
    StructField("circulating_supply",     DoubleType(),    nullable=True),
    StructField("total_supply",           DoubleType(),    nullable=True),
    StructField("last_updated_api",       TimestampType(), nullable=True),
    StructField("raw_payload",            StringType(),    nullable=False),  # full JSON audit trail
    StructField("ingested_at",            TimestampType(), nullable=False),
    StructField("processing_date",        StringType(),    nullable=False),  # partition key
])


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def fetch_coin_data(coin_ids: list[str]) -> list[dict[str, Any]]:
    """
    Fetches market data for all tracked coins from CoinGecko.
    Handles pagination and exponential-backoff retries.
    Returns a flat list of raw API response dicts.
    """
    all_records: list[dict[str, Any]] = []
    ids_param = ",".join(coin_ids)

    params = {
        "vs_currency":           CURRENCY,
        "ids":                   ids_param,
        "order":                 "market_cap_desc",
        "per_page":              API_PAGE_SIZE,
        "page":                  1,
        "sparkline":             "false",
        "price_change_percentage": "24h",
    }

    for attempt in range(1, API_RETRY_ATTEMPTS + 1):
        try:
            logger.info("CoinGecko request (attempt %d/%d)", attempt, API_RETRY_ATTEMPTS)
            response = requests.get(
                COINGECKO_COINS_ENDPOINT,
                params=params,
                timeout=30,
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError(f"Unexpected CoinGecko response shape: {type(data)}")

            all_records.extend(data)
            logger.info("Fetched %d coin records from CoinGecko.", len(data))
            return all_records

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else "unknown"
            # 429 = rate limited; 5xx = server error — both worth retrying.
            if attempt < API_RETRY_ATTEMPTS and status in (429, 500, 502, 503, 504):
                wait = API_RETRY_BACKOFF_S ** attempt
                logger.warning("HTTP %s — retrying in %.1fs...", status, wait)
                time.sleep(wait)
                continue
            raise
        except requests.exceptions.RequestException as exc:
            if attempt < API_RETRY_ATTEMPTS:
                wait = API_RETRY_BACKOFF_S ** attempt
                logger.warning("Request error: %s — retrying in %.1fs...", exc, wait)
                time.sleep(wait)
                continue
            raise

    # Should be unreachable, but satisfies type checkers.
    raise RuntimeError("Exhausted all retry attempts fetching CoinGecko data.")


# ---------------------------------------------------------------------------
# Record normalisation
# ---------------------------------------------------------------------------

def normalise_record(raw: dict[str, Any], ingested_at: datetime, processing_date: str) -> dict[str, Any]:
    """
    Flattens a raw CoinGecko API dict into a Bronze-layer row.
    Stores the original JSON in raw_payload for full auditability.
    """
    def _ts(value: Any) -> datetime | None:
        """Parse ISO timestamp string to datetime, return None on failure."""
        if value is None:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    return {
        "coin_id":               raw.get("id"),
        "symbol":                raw.get("symbol"),
        "name":                  raw.get("name"),
        "current_price":         raw.get("current_price"),
        "market_cap":            raw.get("market_cap"),
        "total_volume":          raw.get("total_volume"),
        "high_24h":              raw.get("high_24h"),
        "low_24h":               raw.get("low_24h"),
        "price_change_24h":      raw.get("price_change_24h"),
        "price_change_pct_24h":  raw.get("price_change_percentage_24h"),
        "circulating_supply":    raw.get("circulating_supply"),
        "total_supply":          raw.get("total_supply"),
        "last_updated_api":      _ts(raw.get("last_updated")),
        "raw_payload":           json.dumps(raw),
        "ingested_at":           ingested_at,
        "processing_date":       processing_date,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(spark: SparkSession, processing_date: str) -> None:
    logger.info("Bronze ingestion starting — processing_date=%s", processing_date)

    ensure_schemas_exist(spark, CATALOG, [BRONZE_SCHEMA])

    # 1. Fetch
    raw_records = fetch_coin_data(TRACKED_COINS)
    if not raw_records:
        raise RuntimeError("CoinGecko returned zero records — aborting to avoid data loss.")

    ingested_at = datetime.now(tz=timezone.utc)

    # 2. Normalise
    rows = [normalise_record(r, ingested_at, processing_date) for r in raw_records]

    # 3. Create Spark DataFrame with enforced schema
    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA_DEF)

    # 4. Validate coin_id is never null (primary identifier)
    null_ids = df.filter(F.col("coin_id").isNull()).count()
    if null_ids > 0:
        raise ValueError(f"Bronze validation failed: {null_ids} rows have null coin_id.")

    logger.info("Writing %d rows to Bronze Delta table: %s", df.count(), BRONZE_TABLE)

    # 5. Idempotent partition-scoped overwrite via replaceWhere
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"processing_date = '{processing_date}'")
        .partitionBy("processing_date")
        .option("path", BRONZE_PATH)
        .saveAsTable(BRONZE_TABLE)
    )

    logger.info("Bronze ingestion complete — processing_date=%s", processing_date)


if __name__ == "__main__":
    _spark = get_spark("crypto_bronze_ingestion")
    _processing_date = get_processing_date(_spark)
    run(_spark, _processing_date)
