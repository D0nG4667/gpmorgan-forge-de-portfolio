"""Integration tests for the deployable Lakehouse artifacts (dbt, Airflow, Spark, Trino, Debezium).

The dbt Gold models are executed for real (against SQLite, since they use portable SQL) and
compared with the in-process ``MedallionLakehousePipeline`` so the SQL and Python semantics
cannot drift apart. Airflow and Spark files are validated structurally via ``ast`` because those
runtimes are not importable in CI.
"""

from __future__ import annotations

import ast
import json
import re
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from gpmorgan_forge.distributed_privacy import MedallionLakehousePipeline

LAKEHOUSE_DIR = Path(__file__).resolve().parents[2] / "portfolio-5-distributed-cloud" / "lakehouse"
DBT_DIR = LAKEHOUSE_DIR / "dbt"

CONFIG_RE = re.compile(r"\{\{\s*config\([^}]*\)\s*\}\}")
REF_RE = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")
SOURCE_RE = re.compile(r"\{\{\s*source\('[^']+',\s*'([^']+)'\)\s*\}\}")


def render_dbt_sql(path: Path) -> str:
    """Render the tiny Jinja subset (config/ref/source) used by the Gold models."""
    sql = CONFIG_RE.sub("", path.read_text(encoding="utf-8"))
    sql = REF_RE.sub(r"\1", sql)
    return SOURCE_RE.sub(r"\1", sql).strip()


def parse_module(path: Path) -> ast.Module:
    """Parse a Python file without importing it."""
    return ast.parse(path.read_text(encoding="utf-8"))


def parse_properties(path: Path) -> dict[str, str]:
    """Parse a Trino ``.properties`` catalog file into a key/value mapping."""
    pairs: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            pairs[key] = value
    return pairs


@pytest.mark.integration
def test_dbt_gold_models_match_python_pipeline_and_pass_reconciliation() -> None:
    """Execute the dbt models and compare the Gold fact with the in-process pipeline."""
    events: list[dict[str, Any]] = [
        {"reference_code": "R1", "currency": "NGN", "amount": "6000000.0000"},
        {"reference_code": "R2", "currency": "USD", "amount": "500.0000", "is_suspicious": True},
        {"reference_code": "R3", "currency": "NGN", "amount": "250.5000"},
        {"reference_code": "R4", "currency": "NGN", "amount": "99.0000", "status": "FAILED"},
        {"reference_code": "R5", "currency": "EUR", "amount": "10.0000", "status": "PENDING"},
    ]
    lake = MedallionLakehousePipeline()
    for event in events:
        lake.publish_to_kafka(event)
    lake.execute_spark_and_warehouse_pipeline()

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "create table silver_transactions "
        "(reference_code text, currency text, amount numeric, status text, "
        "spark_fraud_flagged integer)"
    )
    conn.executemany(
        "insert into silver_transactions values (?, ?, ?, ?, ?)",
        [
            (
                r["reference_code"],
                r["currency"],
                str(r["amount"]),
                r["status"],
                int(r["spark_fraud_flagged"]),
            )
            for r in lake.silver_spark_enriched
        ],
    )
    for model in ("staging/stg_silver_transactions", "marts/dim_corridor"):
        name = model.split("/")[1]
        conn.execute(
            f"create table {name} as {render_dbt_sql(DBT_DIR / 'models' / (model + '.sql'))}"
        )
    fact_sql = render_dbt_sql(DBT_DIR / "models/marts/fact_corridor_settlement.sql")
    conn.execute(f"create table fact_corridor_settlement as {fact_sql}")

    fact = {
        key: (count, Decimal(str(total)), flagged)
        for key, count, total, flagged in conn.execute(
            "select corridor_key, transaction_count, total_settled_amount, flagged_count "
            "from fact_corridor_settlement"
        )
    }
    assert fact == {
        "NGN": (2, Decimal("6000250.5"), 1),
        "USD": (1, Decimal("500"), 1),
    }
    assert {k: v[1] for k, v in fact.items()} == {
        k: v.normalize() for k, v in lake.gold_data_warehouse.items()
    }
    assert {r[0] for r in conn.execute("select corridor_key from dim_corridor")} == set(fact)

    singular = render_dbt_sql(DBT_DIR / "tests/assert_gold_reconciles_to_silver.sql")
    assert conn.execute(singular).fetchall() == []
    conn.execute(
        "update fact_corridor_settlement set total_settled_amount = 1 where corridor_key='USD'"
    )
    assert conn.execute(singular).fetchall() == [(1,)]
    conn.close()


@pytest.mark.integration
def test_dbt_project_declares_tests_freshness_and_trino_profile() -> None:
    """dbt project wires the Trino/Iceberg profile, source freshness, and generic data tests."""
    project = (DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    assert "profile: forge_lakehouse" in project
    profile = (DBT_DIR / "profiles.yml").read_text(encoding="utf-8")
    assert "type: trino" in profile
    assert "database: iceberg" in profile
    sources = (DBT_DIR / "models/sources.yml").read_text(encoding="utf-8")
    assert "freshness:" in sources
    assert "loaded_at_field: ingested_at" in sources
    marts = (DBT_DIR / "models/marts/schema.yml").read_text(encoding="utf-8")
    for expected in ("not_null", "unique", "relationships", "ref('dim_corridor')"):
        assert expected in marts


@pytest.mark.integration
def test_airflow_dag_orchestrates_freshness_dbt_build_and_reconciliation() -> None:
    """The DAG gates dbt build on source freshness and ends with the BCBS 239 reconciliation."""
    tree = parse_module(LAKEHOUSE_DIR / "airflow/dags/forge_medallion_dag.py")
    source = (LAKEHOUSE_DIR / "airflow/dags/forge_medallion_dag.py").read_text(encoding="utf-8")

    task_ids = [
        kw.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "BashOperator"
        for kw in node.keywords
        if kw.arg == "task_id" and isinstance(kw.value, ast.Constant)
    ]
    assert task_ids == ["check_silver_freshness", "dbt_build_gold", "reconcile_gold_to_silver"]
    assert "check_silver_freshness >> dbt_build_gold >> reconcile_gold_to_silver" in source
    assert 'dag_id="forge_medallion_gold_build"' in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert '"retries": 3' in source
    assert "dbt source freshness" in source
    assert "dbt build" in source


@pytest.mark.integration
def test_spark_streaming_job_implements_dlq_merge_and_checkpointing() -> None:
    """Spark job defines the contract, DLQ routing, idempotent MERGE, and checkpointing."""
    path = LAKEHOUSE_DIR / "spark/bronze_to_silver.py"
    tree = parse_module(path)
    functions = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert {"build_session", "violates_contract", "process_batch", "main"} <= functions
    source = path.read_text(encoding="utf-8")
    assert "MERGE INTO" in source
    assert "WHEN MATCHED THEN UPDATE SET *" in source
    assert "dlq_transactions" in source
    assert "bronze_transactions" in source
    assert "checkpointLocation" in source
    assert 'type", "rest"' in source


@pytest.mark.integration
def test_trino_catalogs_and_debezium_connector_are_configured() -> None:
    """Trino federates Iceberg/PG/Mongo/Cassandra; Debezium streams ledger CDC to Kafka."""
    catalog_dir = LAKEHOUSE_DIR / "trino/catalog"
    connectors = {p.stem: parse_properties(p) for p in catalog_dir.glob("*.properties")}
    assert set(connectors) == {"iceberg", "postgresql", "mongodb", "cassandra"}
    assert connectors["iceberg"]["iceberg.catalog.type"] == "rest"
    assert "polaris-catalog" in connectors["iceberg"]["iceberg.rest-catalog.uri"]
    assert connectors["iceberg"]["iceberg.rest-catalog.vended-credentials-enabled"] == "true"
    assert "${ENV:" in connectors["postgresql"]["connection-password"]

    connector = json.loads(
        (LAKEHOUSE_DIR / "debezium/postgres-ledger-connector.json").read_text(encoding="utf-8")
    )["config"]
    assert connector["plugin.name"] == "pgoutput"
    assert connector["table.include.list"] == "public.transactions"
    assert connector["transforms.route.replacement"] == "forge.cdc.transactions"
    assert connector["database.password"] == "${env:POSTGRES_PASSWORD}"

    otel = (LAKEHOUSE_DIR / "otel/otel-collector-config.yaml").read_text(encoding="utf-8")
    assert "otlp:" in otel
    assert "memory_limiter" in otel
