"""
tests/test_silver_transformation.py
--------------------------------------
Unit tests for Silver layer transformation and DQ logic.
Uses chispa for DataFrame equality and pytest-mock for API stubs.

Run with:
    pytest tests/ -v
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from spark_jobs.silver_transformation import (
    DataQualityError,
    apply_symbol_mapping,
    deduplicate,
    run_dq_checks,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_silver")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestDeduplicate:

    def test_keeps_latest_ingested_record(self, spark):
        t1 = datetime(2026, 3, 17, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 3, 17, 12, 0, 0, tzinfo=timezone.utc)

        schema = StructType([
            StructField("coin_id",         StringType(),    False),
            StructField("current_price",   DoubleType(),    True),
            StructField("ingested_at",     TimestampType(), False),
            StructField("processing_date", StringType(),    False),
        ])

        df = spark.createDataFrame([
            ("bitcoin", 65000.0, t1, "2026-03-17"),
            ("bitcoin", 66000.0, t2, "2026-03-17"),  # latest — should survive
        ], schema=schema)

        result = deduplicate(df)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["current_price"] == 66000.0

    def test_preserves_distinct_coins(self, spark):
        t = datetime(2026, 3, 17, 10, 0, 0, tzinfo=timezone.utc)
        schema = StructType([
            StructField("coin_id",         StringType(),    False),
            StructField("current_price",   DoubleType(),    True),
            StructField("ingested_at",     TimestampType(), False),
            StructField("processing_date", StringType(),    False),
        ])
        df = spark.createDataFrame([
            ("bitcoin",  65000.0, t, "2026-03-17"),
            ("ethereum",  3400.0, t, "2026-03-17"),
        ], schema=schema)

        result = deduplicate(df)
        assert result.count() == 2


# ---------------------------------------------------------------------------
# Symbol mapping tests
# ---------------------------------------------------------------------------

class TestApplySymbolMapping:

    def test_known_coin_maps_correctly(self, spark):
        df = spark.createDataFrame([("bitcoin",)], ["coin_id"])
        result = apply_symbol_mapping(df)
        row = result.collect()[0]
        assert row["ticker_symbol"] == "BTC"

    def test_unknown_coin_produces_null_symbol(self, spark):
        df = spark.createDataFrame([("unknowncoin",)], ["coin_id"])
        result = apply_symbol_mapping(df)
        row = result.collect()[0]
        assert row["ticker_symbol"] is None


# ---------------------------------------------------------------------------
# DQ check tests
# ---------------------------------------------------------------------------

class TestDQChecks:

    def _make_silver_df(self, spark, rows):
        schema = StructType([
            StructField("coin_id",           StringType(), False),
            StructField("ticker_symbol",     StringType(), True),
            StructField("current_price_usd", DoubleType(), True),
        ])
        return spark.createDataFrame(rows, schema=schema)

    def test_passes_clean_data(self, spark):
        df = self._make_silver_df(spark, [
            ("bitcoin",  "BTC", 65000.0),
            ("ethereum", "ETH",  3400.0),
        ])
        # Should not raise
        run_dq_checks(df, "2026-03-17")

    def test_fails_on_too_many_null_prices(self, spark):
        df = self._make_silver_df(spark, [
            ("bitcoin",  "BTC", None),
            ("ethereum", "ETH", None),
            ("ripple",   "XRP", None),
            ("solana",   "SOL", 100.0),
        ])
        with pytest.raises(DataQualityError, match="null current_price_usd"):
            run_dq_checks(df, "2026-03-17")

    def test_fails_on_negative_prices(self, spark):
        df = self._make_silver_df(spark, [
            ("bitcoin", "BTC", -1.0),
        ])
        with pytest.raises(DataQualityError, match="negative current_price_usd"):
            run_dq_checks(df, "2026-03-17")

    def test_fails_on_unmapped_symbol(self, spark):
        df = self._make_silver_df(spark, [
            ("bitcoin",     "BTC",  65000.0),
            ("unknowncoin", None,   0.001),
        ])
        with pytest.raises(DataQualityError, match="ticker_symbol"):
            run_dq_checks(df, "2026-03-17")
