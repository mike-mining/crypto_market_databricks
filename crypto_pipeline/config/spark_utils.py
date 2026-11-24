"""
config/spark_utils.py
---------------------
Shared Spark session factory and helpers for all pipeline jobs.
Centralises Unity Catalog config, shuffle tuning, and Databricks widget handling.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime
from typing import Optional

from pyspark.sql import SparkSession

from config.settings import CATALOG, SPARK_SHUFFLE_PARTITIONS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def get_spark(app_name: str) -> SparkSession:
    """
    Returns an active SparkSession configured for Unity Catalog.

    On Databricks, getOrCreate() picks up the pre-initialised cluster session;
    the config calls are additive and safe to re-apply.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Unity Catalog is the default catalog — all three-part names resolve correctly.
        .config("spark.sql.defaultCatalog", CATALOG)
        # Adaptive Query Execution: let Spark coalesce shuffle partitions dynamically.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        # Delta Lake optimised writes and auto-compaction for small-file mitigation.
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialised: %s", app_name)
    return spark


# ---------------------------------------------------------------------------
# Databricks Widget helpers
# ---------------------------------------------------------------------------

def get_processing_date(spark: SparkSession, cli_args: Optional[list[str]] = None) -> str:
    """
    Resolves the processing_date parameter with the following priority:

      1. Databricks Widget  (notebook / Airflow DatabricksSubmitRunOperator notebook_params)
      2. CLI positional arg  (spark-submit job.py 2026-03-17)
      3. Today's date        (local dev fallback)

    Returns an ISO-8601 date string: 'YYYY-MM-DD'.
    """
    # --- Try Databricks widget first ---
    try:
        dbutils = _get_dbutils(spark)
        raw = dbutils.widgets.get("processing_date").strip()
        if raw:
            _validate_date(raw)
            logger.info("processing_date from Databricks widget: %s", raw)
            return raw
    except Exception:
        pass  # Not running in a notebook context — fall through.

    # --- CLI arg ---
    args = cli_args if cli_args is not None else sys.argv[1:]
    if args:
        raw = args[0].strip()
        _validate_date(raw)
        logger.info("processing_date from CLI arg: %s", raw)
        return raw

    # --- Fallback: today ---
    fallback = date.today().isoformat()
    logger.warning("processing_date not provided; defaulting to today: %s", fallback)
    return fallback


def _validate_date(value: str) -> None:
    """Raises ValueError if value is not a valid ISO date."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid processing_date '{value}'. Expected format: YYYY-MM-DD."
        ) from exc


def _get_dbutils(spark: SparkSession):
    """
    Returns dbutils inside a Databricks runtime without importing it at module level
    (which would break local Spark / CI environments).
    """
    from pyspark.dbutils import DBUtils  # noqa: PLC0415 — intentional late import
    return DBUtils(spark)


# ---------------------------------------------------------------------------
# Schema creation guard
# ---------------------------------------------------------------------------

def ensure_schemas_exist(spark: SparkSession, catalog: str, schemas: list[str]) -> None:
    """Idempotently creates Unity Catalog schemas if they do not exist."""
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        logger.info("Schema ensured: %s.%s", catalog, schema)
