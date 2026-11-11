"""End-to-end tests for evidence generation CLI, SQL/NoSQL artifacts, and repository integrity."""

from __future__ import annotations

from pathlib import Path

import pytest

from gpmorgan_forge.evidence_generator import generate_all_portfolio_evidence, main
from gpmorgan_forge.models import Base


@pytest.mark.e2e
def test_evidence_generator_and_cli() -> None:
    """Verify full portfolio evidence generation and CLI entry point."""
    evidence = generate_all_portfolio_evidence()
    assert evidence["portfolio_1_ledger_audit"]["is_balanced"] is True
    assert evidence["portfolio_1_ledger_audit"]["hash_chain_valid"] is True
    assert evidence["portfolio_2_query_optimisation"]["max_balance_drift"] == "0.0000"
    assert evidence["portfolio_3_concurrency"]["deadlock_prevented"] is True
    assert evidence["portfolio_4_nosql"]["neo4j_circular_rings_detected"] == 1
    assert evidence["portfolio_5_distributed_privacy"]["zkp_verified"] is True

    assert main([]) == 0
    assert main(["--verify"]) == 0


@pytest.mark.e2e
def test_repository_deliverables_and_test_pyramid_structure() -> None:
    """Ensure all 5 portfolio directories, 3-tier test pyramid, ADRs, and production docs exist."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    required_paths = [
        ".env.example",
        "compose.yaml",
        "Dockerfile",
        "lefthook.yml",
        "pyproject.toml",
        "README.md",
        "tests/conftest.py",
        "tests/unit/test_domain_models_and_crypto.py",
        "tests/unit/test_nosql_and_privacy_primitives.py",
        "tests/integration/test_core_banking_and_ledger_ops.py",
        "tests/integration/test_query_optimizer_and_concurrency.py",
        "tests/integration/test_polyglot_graph_and_lakehouse.py",
        "tests/e2e/test_portfolio_evidence_and_artifacts.py",
        "docs/BDR.md",
        "docs/DEVELOPER_ONBOARDING.md",
        "docs/GIT_WORKFLOW.md",
        "docs/PRODUCTION_DEPLOYMENT.md",
        "docs/FINAL_PORTFOLIO_DOCUMENTATION.md",
        "docs/adr/0001-double-entry-immutable-ledger.md",
        "docs/adr/0002-polyglot-persistence-postgres-mongo-neo4j-redis.md",
        "docs/adr/0003-concurrency-control-and-idempotency.md",
        "docs/adr/0004-declarative-partitioning-and-replication.md",
        "docs/adr/0005-lakehouse-kafka-spark-architecture.md",
        "docs/adr/0006-zero-trust-security-rbac-rls-ndpa-privacy.md",
        "portfolio-1-database-design/schema.sql",
        "portfolio-1-database-design/seed_data.sql",
        "portfolio-1-database-design/README.md",
        "portfolio-2-query-optimisation/indexes_and_views.sql",
        "portfolio-2-query-optimisation/queries.sql",
        "portfolio-2-query-optimisation/execution_plans_evidence.md",
        "portfolio-2-query-optimisation/README.md",
        "portfolio-3-transactions-concurrency/transactions_and_locks.sql",
        "portfolio-3-transactions-concurrency/concurrency_evidence.md",
        "portfolio-3-transactions-concurrency/README.md",
        "portfolio-4-nosql/mongodb_document_model.js",
        "portfolio-4-nosql/neo4j_fraud_graph.cypher",
        "portfolio-4-nosql/redis_caching_patterns.conf",
        "portfolio-4-nosql/cassandra_timeline_projection.cql",
        "portfolio-4-nosql/nosql_evidence.md",
        "portfolio-4-nosql/README.md",
        "portfolio-5-distributed-cloud/partitioning_and_sharding.sql",
        "portfolio-5-distributed-cloud/security_rbac_rls_privacy.sql",
        "portfolio-5-distributed-cloud/distributed_cloud_evidence.md",
        "portfolio-5-distributed-cloud/README.md",
        "portfolio-5-distributed-cloud/lakehouse/dbt/dbt_project.yml",
        "portfolio-5-distributed-cloud/lakehouse/dbt/profiles.yml",
        "portfolio-5-distributed-cloud/lakehouse/dbt/models/sources.yml",
        "portfolio-5-distributed-cloud/lakehouse/dbt/models/staging/stg_silver_transactions.sql",
        "portfolio-5-distributed-cloud/lakehouse/dbt/models/marts/dim_corridor.sql",
        "portfolio-5-distributed-cloud/lakehouse/dbt/models/marts/fact_corridor_settlement.sql",
        "portfolio-5-distributed-cloud/lakehouse/dbt/models/marts/schema.yml",
        "portfolio-5-distributed-cloud/lakehouse/dbt/tests/assert_gold_reconciles_to_silver.sql",
        "portfolio-5-distributed-cloud/lakehouse/airflow/dags/forge_medallion_dag.py",
        "portfolio-5-distributed-cloud/lakehouse/spark/bronze_to_silver.py",
        "portfolio-5-distributed-cloud/lakehouse/debezium/postgres-ledger-connector.json",
        "portfolio-5-distributed-cloud/lakehouse/otel/otel-collector-config.yaml",
        "portfolio-5-distributed-cloud/lakehouse/trino/catalog/iceberg.properties",
        "portfolio-5-distributed-cloud/infra/postgres/primary-init-replication.sh",
        "portfolio-5-distributed-cloud/infra/postgres/replica-entrypoint.sh",
        "portfolio-5-distributed-cloud/infra/postgres/tls-entrypoint.sh",
        "portfolio-5-distributed-cloud/infra/cassandra/cassandra-entrypoint.sh",
        "portfolio-5-distributed-cloud/infra/neo4j/neo4j-entrypoint.sh",
        "portfolio-5-distributed-cloud/infra/cockroach/cockroach-entrypoint.sh",
        "portfolio-5-distributed-cloud/infra/cockroach/init-multi-region.sql",
        "portfolio-5-distributed-cloud/infra/timescaledb/init-hypertable.sql",
        ".gitattributes",
        "scripts/generate_dev_tls.py",
        "src/gpmorgan_forge/identity.py",
        "src/gpmorgan_forge/zkp.py",
        "tests/unit/test_identity.py",
        "tests/unit/test_zkp.py",
        "tests/integration/test_lakehouse_artifacts.py",
    ]
    for rel_path in required_paths:
        assert (repo_root / rel_path).is_file(), f"Missing required deliverable: {rel_path}"


@pytest.mark.e2e
def test_sql_nosql_devops_and_markdown_artifacts_deep_verification() -> None:
    """Verify SQL DDL/DML, Cypher, MongoDB JS, Redis conf, Lefthook rumdl, and relative links."""
    repo_root = Path(__file__).resolve().parent.parent.parent

    # 1. Portfolio 1: Verify all 10 ORM tables and constraints exist in schema.sql
    schema_sql = (repo_root / "portfolio-1-database-design/schema.sql").read_text(encoding="utf-8")
    assert "Target Engine: PostgreSQL 18" in schema_sql
    orm_tables = set(Base.metadata.tables.keys())
    assert len(orm_tables) == 10
    for table_name in orm_tables:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in schema_sql
    for constraint in [
        "chk_accounts_non_negative_customer_balance",
        "chk_transactions_distinct_accounts",
        "chk_transactions_positive_amount",
        "chk_ledger_entries_balance_math",
        "chk_payment_cards_masked_pan",
        "trg_immutable_ledger_entries",
        "trg_immutable_audit_logs",
        "document_ref_hash",
    ]:
        assert constraint in schema_sql
    assert "document_Ref_hash" not in schema_sql

    seed_sql = (repo_root / "portfolio-1-database-design/seed_data.sql").read_text(encoding="utf-8")
    for table_name in orm_tables:
        assert f"INSERT INTO {table_name}" in seed_sql
    assert '"account_id":"30000000-0000-4000-8000-000000000002"' in seed_sql

    # 2. Portfolio 2: Verify indexes, materialised views, and EXPLAIN ANALYZE queries
    indexes_sql = (repo_root / "portfolio-2-query-optimisation/indexes_and_views.sql").read_text(
        encoding="utf-8"
    )
    for idx_or_view in [
        "idx_transactions_source_created_covering",
        "idx_ledger_entries_account_created_covering",
        "pidx_transactions_active_pipeline",
        "gin_transactions_metadata_jsonb",
        "mv_daily_corridor_settlement",
        "mv_customer_liquidity_360",
    ]:
        assert idx_or_view in indexes_sql

    queries_sql = (repo_root / "portfolio-2-query-optimisation/queries.sql").read_text(
        encoding="utf-8"
    )
    assert queries_sql.count("EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)") >= 5
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY" in queries_sql

    # 3. Portfolio 3: Verify ACID isolation levels, locking, savepoints, and contra-entries
    tx_sql = (
        repo_root / "portfolio-3-transactions-concurrency/transactions_and_locks.sql"
    ).read_text(encoding="utf-8")
    for pattern in [
        "ISOLATION LEVEL REPEATABLE READ",
        "ISOLATION LEVEL SERIALIZABLE",
        "ORDER BY account_id ASC",
        "FOR UPDATE",
        "SAVEPOINT sp_payroll_leg_2",
        "ROLLBACK TO SAVEPOINT sp_payroll_leg_2",
        "COMMIT;",
        "ROLLBACK;",
    ]:
        assert pattern in tx_sql

    # 4. Portfolio 4: Verify MongoDB 8.0, Neo4j 5.26 LTS, and Redis 8.0 artifacts
    mongo_js = (repo_root / "portfolio-4-nosql/mongodb_document_model.js").read_text(
        encoding="utf-8"
    )
    assert "$jsonSchema" in mongo_js
    assert "AEAD_AES_256_GCM" in mongo_js
    assert "expireAfterSeconds: 7776000" in mongo_js

    cypher = (repo_root / "portfolio-4-nosql/neo4j_fraud_graph.cypher").read_text(encoding="utf-8")
    assert "Neo4j 5.26 LTS" in cypher
    assert "CREATE CONSTRAINT uq_graph_account_number IF NOT EXISTS" in cypher
    assert "[:TRANSFERRED_TO*2..5]" in cypher
    assert "origin.account_number <= n.account_number" in cypher
    assert "collect(DISTINCT c.customer_id)" in cypher
    assert "AS linked_customers" in cypher

    redis_conf = (repo_root / "portfolio-4-nosql/redis_caching_patterns.conf").read_text(
        encoding="utf-8"
    )
    assert "Redis 8.0" in redis_conf
    assert "NX EX 86400" in redis_conf
    assert "uq_transactions_idempotency_key" in redis_conf
    assert "ZREMRANGEBYSCORE" in redis_conf

    cassandra_cql = (repo_root / "portfolio-4-nosql/cassandra_timeline_projection.cql").read_text(
        encoding="utf-8"
    )
    assert "Apache Cassandra 5.0" in cassandra_cql
    assert "PRIMARY KEY ((account_id, bucket_month), created_at, transaction_id)" in cassandra_cql
    assert "WITH CLUSTERING ORDER BY (created_at DESC, transaction_id ASC)" in cassandra_cql

    # 5. Portfolio 5: Verify partitioning, sharding, CockroachDB, TimescaleDB, RBAC, RLS, privacy
    part_sql = (
        repo_root / "portfolio-5-distributed-cloud/partitioning_and_sharding.sql"
    ).read_text(encoding="utf-8")
    assert "PARTITION BY RANGE (created_at)" in part_sql
    assert "PARTITION BY HASH (account_id)" in part_sql
    assert "MODULUS 4" in part_sql
    assert "global_corridor_settlements" in part_sql
    assert "create_hypertable" in part_sql
    assert "time_bucket" in part_sql

    sec_sql = (repo_root / "portfolio-5-distributed-cloud/security_rbac_rls_privacy.sql").read_text(
        encoding="utf-8"
    )
    for sec_object in [
        "forge_core_service",
        "forge_compliance_dpo",
        "forge_analyst",
        "forge_customer",
        "forge_admin",
        "v_masked_customers",
        "v_masked_payment_cards",
        "ENABLE ROW LEVEL SECURITY",
        "FORCE ROW LEVEL SECURITY",
        "pol_customers_customer_isolation",
        "pol_customers_core_all",
        "anonymise_customer_pii",
    ]:
        assert sec_object in sec_sql

    # 6. DevOps & Markdown Linting verification
    compose_txt = (repo_root / "compose.yaml").read_text(encoding="utf-8")
    for img in [
        "postgres:18-alpine",
        "cockroachdb/cockroach:v24.3.0",
        "mongo:8.0",
        "neo4j:5.26-community",
        "redis:8.0-alpine",
        "cassandra:5.0",
        "timescale/timescaledb:latest-pg17",
        "apache/kafka:4.0.0",
        "flink:1.20-java17",
        "apache/polaris:latest",
        "trinodb/trino:473",
        "quay.io/debezium/connect:3.0.0.Final",
        "apache/spark:4.0.0-python3",
        "apache/airflow:3.0.6",
        "wal_level=logical",
        "lakehouse/trino/catalog:/etc/trino/catalog:ro",
        "lakehouse/airflow/dags:/opt/forge/dags:ro",
        "otel/opentelemetry-collector-contrib:0.120.0",
        "POSTGRES_DSN: postgresql://${POSTGRES_USER:-forge_admin}",
        "POLARIS_CATALOG_URI: ${POLARIS_CATALOG_URI:-http://polaris-catalog:8181/api/catalog}",
        "POLARIS_BOOTSTRAP_CREDENTIALS: ${POLARIS_BOOTSTRAP_CREDENTIALS:-POLARIS,root,s3cr3t}",
        "polaris.readiness.ignore-severe-issues: ${POLARIS_IGNORE_SEVERE_ISSUES:-true}",
        'polaris.features."ALLOW_INSECURE_STORAGE_TYPES": ${POLARIS_ALLOW_INSECURE_STORAGE:-true}',
        "lakehouse_warehouse_data:/tmp/forge_lakehouse",
        "http://localhost:8182/q/health",
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"',
        "timeout: 15s",
    ]:
        assert img in compose_txt
    assert "http://localhost:8181/healthcheck" not in compose_txt

    env_example_txt = (repo_root / ".env.example").read_text(encoding="utf-8")
    for env_key in [
        "POSTGRES_DB=gpmorgan_forge",
        "COCKROACH_SQL_PORT=26257",
        "CASSANDRA_CQL_PORT=9042",
        "TIMESCALE_PORT=5434",
        "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "FLINK_REST_URI=http://flink:8081",
        "POLARIS_CATALOG_URI=",
        "POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t",
        "TRINO_COORDINATOR_URI=http://trino:8080",
    ]:
        assert env_key in env_example_txt

    dockerfile_txt = (repo_root / "Dockerfile").read_text(encoding="utf-8")
    assert "python:3.15-slim-bookworm" in dockerfile_txt
    assert "COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/" in dockerfile_txt
    assert "USER forge" in dockerfile_txt
    assert ".env.example .gitattributes compose.yaml Dockerfile lefthook.yml" in dockerfile_txt

    lefthook_txt = (repo_root / "lefthook.yml").read_text(encoding="utf-8")
    assert "markdown-lint" in lefthook_txt
    assert "uv run rumdl check" in lefthook_txt

    # 7. Verify repository audit (terminology, relative links, and file:/// checks via Git SSOT)
    from scripts.verify_commit_msg import audit_repository

    audit_ok, audit_issues = audit_repository(repo_root)
    assert audit_ok is True, f"Repository audit failed: {audit_issues}"
