"""
config/settings.py
------------------
Single source of truth for catalog, schema, table, and path config.
All Spark jobs import from here — no magic strings scattered across jobs.
"""

from dataclasses import dataclass, field
from typing import Final

# ---------------------------------------------------------------------------
# Unity Catalog topology
# ---------------------------------------------------------------------------
CATALOG: Final[str] = "crypto"

BRONZE_SCHEMA: Final[str] = "bronze"
SILVER_SCHEMA: Final[str] = "silver"
GOLD_SCHEMA: Final[str] = "gold"

BRONZE_TABLE: Final[str] = f"{CATALOG}.{BRONZE_SCHEMA}.raw_market_data"
SILVER_TABLE: Final[str] = f"{CATALOG}.{SILVER_SCHEMA}.market_data"
GOLD_TABLE: Final[str] = f"{CATALOG}.{GOLD_SCHEMA}.daily_ohlcv"

# ---------------------------------------------------------------------------
# External storage (ADLS Gen2 / S3 external location registered in UC)
# ---------------------------------------------------------------------------
EXTERNAL_LOCATION: Final[str] = "abfss://crypto@<storage_account>.dfs.core.windows.net"

BRONZE_PATH: Final[str] = f"{EXTERNAL_LOCATION}/bronze/raw_market_data"
SILVER_PATH: Final[str] = f"{EXTERNAL_LOCATION}/silver/market_data"
GOLD_PATH: Final[str] = f"{EXTERNAL_LOCATION}/gold/daily_ohlcv"

# ---------------------------------------------------------------------------
# CoinGecko
# ---------------------------------------------------------------------------
COINGECKO_BASE_URL: Final[str] = "https://api.coingecko.com/api/v3"
COINGECKO_COINS_ENDPOINT: Final[str] = f"{COINGECKO_BASE_URL}/coins/markets"

# Coins to track — extend here, nowhere else
TRACKED_COINS: Final[list[str]] = [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "ripple",
    "cardano",
    "solana",
    "polkadot",
    "dogecoin",
    "avalanche-2",
    "chainlink",
]

# Canonical coin ID → ticker symbol mapping
COIN_SYMBOL_MAP: Final[dict[str, str]] = {
    "bitcoin":       "BTC",
    "ethereum":      "ETH",
    "binancecoin":   "BNB",
    "ripple":        "XRP",
    "cardano":       "ADA",
    "solana":        "SOL",
    "polkadot":      "DOT",
    "dogecoin":      "DOGE",
    "avalanche-2":   "AVAX",
    "chainlink":     "LINK",
}

# ---------------------------------------------------------------------------
# Pipeline behaviour
# ---------------------------------------------------------------------------
CURRENCY: Final[str] = "usd"
API_PAGE_SIZE: Final[int] = 250          # CoinGecko max per page
API_RETRY_ATTEMPTS: Final[int] = 3
API_RETRY_BACKOFF_S: Final[float] = 2.0  # exponential base

# Airflow / Spark job parallelism
SPARK_SHUFFLE_PARTITIONS: Final[int] = 8  # low-volume crypto data; tune up for larger sets

# ---------------------------------------------------------------------------
# Data quality thresholds
# ---------------------------------------------------------------------------
MAX_NULL_PRICE_PCT: Final[float] = 0.05   # fail Silver if >5 % current_price nulls
MAX_NEGATIVE_PRICE_PCT: Final[float] = 0.0  # zero tolerance for negative prices
