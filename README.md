# Crypto Market Medallion Pipeline in Databricks

Bronze → Silver → Gold pipeline for daily cryptocurrency market data,
built on Databricks (Unity Catalog + Delta Lake) orchestrated by Apache Airflow.

---

## Project Structure

```
crypto_pipeline/
├── config/
│   ├── settings.py          # Single source of truth: catalog, schema, paths, thresholds
│   └── spark_utils.py       # SparkSession factory, widget helpers, schema guards
├── spark_jobs/
│   ├── bronze_ingestion.py  # CoinGecko API → Bronze Delta table
│   ├── silver_transformation.py  # Cleanse, dedup, DQ checks → Silver Delta table
│   └── gold_aggregation.py  # Daily OHLCV aggregations → Gold Delta table
├── dags/
│   └── crypto_medallion_pipeline.py  # Airflow DAG (DatabricksSubmitRunOperator)
├── tests/
│   └── test_silver_transformation.py  # PySpark unit tests
└── requirements.txt
```

---

## Unity Catalog Topology

| Layer  | Catalog | Schema  | Table                         |
|--------|---------|---------|-------------------------------|
| Bronze | crypto  | bronze  | crypto.bronze.raw_market_data |
| Silver | crypto  | silver  | crypto.silver.market_data     |
| Gold   | crypto  | gold    | crypto.gold.daily_ohlcv       |

---

## Prerequisites

### 1. Unity Catalog Setup

```sql
-- Run once in Databricks SQL or a notebook with ACCOUNT ADMIN privileges
CREATE CATALOG IF NOT EXISTS crypto;

-- Grant pipeline service principal access
GRANT USE CATALOG ON CATALOG crypto TO `data-engineering-sp`;
GRANT CREATE SCHEMA ON CATALOG crypto TO `data-engineering-sp`;
```

### 2. External Location (ADLS Gen2 / S3)

Register your storage path as a UC External Location before first run.
Update `EXTERNAL_LOCATION` in `config/settings.py` to match your registered location.

### 3. Databricks Repos

Upload this repository to Databricks Repos:
```
Workspace → Repos → Add Repo → (your Git remote)
```

Set `DATABRICKS_REPO_PATH` in the Airflow Variable or env var:
```
/Repos/production/crypto_pipeline
```

### 4. Airflow Connection

In Airflow UI → Admin → Connections → Add:
```
Conn Id:   databricks_default
Conn Type: Databricks
Host:      https://<your-workspace>.azuredatabricks.net
Token:     <personal-access-token or service-principal secret>
```

### 5. Environment Variables (Airflow worker)

```bash
export DATABRICKS_CLUSTER_NODE_TYPE=Standard_DS3_v2   # or your preferred node
export DATABRICKS_SPARK_VERSION=14.3.x-scala2.12
export DATABRICKS_REPO_PATH=/Repos/production/crypto_pipeline
```

## Running Tests

```bash
pytest tests/ -v
```

---

## Idempotency & Re-runs

Every Spark job uses partition-scoped overwrite:

```python
.option("replaceWhere", f"processing_date = '{processing_date}'")
```

**Re-running any job for the same date is always safe.** It atomically replaces
only the target partition without touching any other dates.

Trigger a backfill from Airflow:

```bash
airflow dags backfill crypto_medallion_pipeline \
  --start-date 2026-01-01 \
  --end-date 2026-03-17
```

---

## Configuration Reference (`config/settings.py`)

| Variable               | Purpose                                        |
|------------------------|------------------------------------------------|
| `CATALOG`              | Unity Catalog name                             |
| `TRACKED_COINS`        | List of CoinGecko coin IDs to ingest           |
| `COIN_SYMBOL_MAP`      | coin_id → ticker symbol mapping                |
| `MAX_NULL_PRICE_PCT`   | Silver DQ: max fraction of null prices (0.05)  |
| `MAX_NEGATIVE_PRICE_PCT` | Silver DQ: max fraction of negative prices (0.0) |
| `EXTERNAL_LOCATION`    | ADLS/S3 path for Delta table storage           |

To add a new coin:
1. Add the CoinGecko ID to `TRACKED_COINS`.
2. Add the `coin_id → ticker_symbol` entry to `COIN_SYMBOL_MAP`.
3. No schema changes required — all layers handle dynamic coin sets.

---

## Data Quality

Silver-layer DQ checks run **before any write**. The job fails and leaves the
previous partition untouched if any check fails:

| Check                  | Threshold      | Action on Failure |
|------------------------|----------------|-------------------|
| Null `current_price`   | > 5%           | Raise & abort     |
| Negative `current_price` | > 0%         | Raise & abort     |
| Unmapped `ticker_symbol` | Any          | Raise & abort     |
| Missing tracked coins  | Any            | Warning only      |

---

## Operational Runbook

### Pipeline failed at Silver — how to recover

1. Check the Airflow task log for the DQ error message.
2. If it's a transient API issue, clear the Bronze task and re-run from Bronze.
3. If it's a data mapping issue (e.g., new coin not in `COIN_SYMBOL_MAP`),
   update `config/settings.py`, commit, and re-run Silver only (Bronze partition is intact).

### Gold shows stale data

Gold reads from Silver. If Silver re-ran successfully, trigger Gold manually:
```bash
airflow tasks run crypto_medallion_pipeline gold_aggregation 2026-03-17
```

### Add a new tracked coin mid-backfill

1. Update `TRACKED_COINS` and `COIN_SYMBOL_MAP` in `config/settings.py`.
2. Re-run Bronze from the date the new coin should appear.
3. Re-run Silver and Gold for the same date range.
   Historical dates without the coin will have a DQ warning (not a failure).
