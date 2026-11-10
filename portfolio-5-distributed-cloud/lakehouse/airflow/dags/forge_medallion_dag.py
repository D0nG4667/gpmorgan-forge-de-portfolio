"""Airflow DAG: orchestrates the Gold layer of the GPMorgan Forge Medallion Lakehouse.

Pattern: continuous streaming (Debezium -> Kafka -> Spark Structured Streaming -> Silver Iceberg) is
a long-running service and is NOT orchestrated; Airflow owns the *batch* Gold build:

    check_silver_freshness -> dbt_build_gold (run + test) -> reconcile_gold_to_silver

dbt tests (not_null, unique, relationships, BCBS 239 reconciliation) gate the DAG, and the
apache-airflow-providers-openlineage integration emits OpenLineage run events per task
(configured through the OPENLINEAGE_URL / AIRFLOW__OPENLINEAGE__* environment variables).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

DBT_DIR = "/opt/forge/dbt"
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"


@dag(
    dag_id="forge_medallion_gold_build",
    description="Silver freshness gate, dbt Gold star-schema build/tests, BCBS 239 reconciliation.",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "forge-data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["lakehouse", "gold", "dbt", "bcbs239"],
)
def forge_medallion_gold_build() -> None:
    check_silver_freshness = BashOperator(
        task_id="check_silver_freshness",
        bash_command=f"dbt source freshness {DBT_FLAGS}",
    )

    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"dbt build {DBT_FLAGS} --select +marts",
    )

    reconcile_gold_to_silver = BashOperator(
        task_id="reconcile_gold_to_silver",
        bash_command=f"dbt test {DBT_FLAGS} --select assert_gold_reconciles_to_silver",
    )

    check_silver_freshness >> dbt_build_gold >> reconcile_gold_to_silver


forge_medallion_gold_build()
