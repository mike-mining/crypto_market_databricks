"""
dags/crypto_medallion_pipeline.py
------------------------------------
Airflow DAG — Crypto Data Medallion Pipeline

Orchestrates the Bronze → Silver → Gold Spark jobs on Databricks using
DatabricksSubmitRunOperator (runs via the Databricks Jobs API, no persistent
cluster required — uses job clusters for cost efficiency).

Design decisions:
  - `schedule_interval="@daily"` with `start_date` in the past triggers
    backfill from day one; `catchup=True` is explicit.
  - `{{ ds }}` (execution date, not run date) is passed as processing_date
    so backfill replays are deterministic and idempotent.
  - Each task uses an independent job cluster definition (new_cluster) so
    failures in one layer do not block cluster teardown.
  - Email alerts on failure via `email_on_failure=True` in default_args;
    configure SMTP in your Airflow environment.
  - `trigger_rule=TriggerRule.ALL_SUCCESS` is the default but stated
    explicitly on Silver and Gold tasks for clarity.

Prerequisites:
  - Airflow connection `databricks_default` configured (host + token).
  - The wheel/egg for config/ module must be installed on the job cluster,
    OR the repo files must be accessible via Databricks Repos and
    `spark_python_task` paths must reference the repo-relative paths.
  - Set DATABRICKS_CLUSTER_NODE_TYPE and DATABRICKS_SPARK_VERSION env vars
    (or hardcode below for your workspace).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Cluster spec — externalised so it can be overridden per environment.
# For production, consider using an existing all-purpose cluster ID via
# `existing_cluster_id` to avoid cold-start latency, or a Databricks Job
# cluster policy for governance.
# ---------------------------------------------------------------------------
_NODE_TYPE    = os.getenv("DATABRICKS_CLUSTER_NODE_TYPE", "Standard_DS3_v2")
_SPARK_VER    = os.getenv("DATABRICKS_SPARK_VERSION",    "14.3.x-scala2.12")
_REPO_PATH    = os.getenv("DATABRICKS_REPO_PATH",        "/Repos/production/crypto_pipeline")
_PYTHON_VER   = "3.11"

# Reusable job cluster definition — one cluster spec per task keeps cost
# predictable and prevents cross-task resource contention.
def _job_cluster(num_workers: int = 1) -> dict:
    return {
        "new_cluster": {
            "spark_version":    _SPARK_VER,
            "node_type_id":     _NODE_TYPE,
            "num_workers":      num_workers,
            "spark_conf": {
                # Unity Catalog requires the Databricks Runtime, not OSS Spark.
                "spark.databricks.cluster.profile": "singleNode" if num_workers == 0 else "serverless",
            },
            "azure_attributes": {
                # Spot instances for Silver/Gold (tolerate preemption via Airflow retry).
                "first_on_demand":  1,
                "availability":     "SPOT_WITH_FALLBACK_AZURE",
            },
            "spark_env_vars": {
                "PYSPARK_PYTHON": f"/databricks/python3/bin/python{_PYTHON_VER}",
            },
        }
    }


def _python_task(script_relative_path: str) -> dict:
    """Builds a spark_python_task referencing a file in the Databricks Repo."""
    return {
        "spark_python_task": {
            "python_file": f"{_REPO_PATH}/{script_relative_path}",
            # processing_date injected via parameters; picked up by get_processing_date()
            # via dbutils.widgets.get("processing_date") inside each job.
            "parameters": ["--processing_date", "{{ ds }}"],
        }
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
_DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,                    # each day's run is independent
    "start_date":       datetime(2026, 1, 1),
    "email":            ["data-engineering@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,            # 5m, 10m on consecutive failures
}

with DAG(
    dag_id="crypto_medallion_pipeline",
    default_args=_DEFAULT_ARGS,
    description="Daily Bronze→Silver→Gold crypto market data pipeline on Databricks.",
    schedule_interval="0 1 * * *",  # 01:00 UTC — after CoinGecko data is settled
    catchup=True,                   # enable backfill
    max_active_runs=1,              # prevent concurrent date partitions colliding
    tags=["crypto", "medallion", "databricks", "delta-lake"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Bronze — raw ingestion from CoinGecko API
    # ------------------------------------------------------------------
    bronze_ingestion = DatabricksSubmitRunOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        run_name="crypto_bronze_{{ ds }}",
        **_job_cluster(num_workers=1),
        **_python_task("spark_jobs/bronze_ingestion.py"),
        polling_period_seconds=30,
    )

    # ------------------------------------------------------------------
    # Task 2: Silver — cleansing, DQ, symbol mapping
    # ------------------------------------------------------------------
    silver_transformation = DatabricksSubmitRunOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        run_name="crypto_silver_{{ ds }}",
        trigger_rule=TriggerRule.ALL_SUCCESS,    # explicit: only if Bronze succeeded
        **_job_cluster(num_workers=1),
        **_python_task("spark_jobs/silver_transformation.py"),
        polling_period_seconds=30,
    )

    # ------------------------------------------------------------------
    # Task 3: Gold — business aggregations
    # ------------------------------------------------------------------
    gold_aggregation = DatabricksSubmitRunOperator(
        task_id="gold_aggregation",
        databricks_conn_id="databricks_default",
        run_name="crypto_gold_{{ ds }}",
        trigger_rule=TriggerRule.ALL_SUCCESS,    # explicit: only if Silver succeeded
        **_job_cluster(num_workers=1),
        **_python_task("spark_jobs/gold_aggregation.py"),
        polling_period_seconds=30,
    )

    # ------------------------------------------------------------------
    # DAG dependency chain: Bronze → Silver → Gold
    # ------------------------------------------------------------------
    bronze_ingestion >> silver_transformation >> gold_aggregation
