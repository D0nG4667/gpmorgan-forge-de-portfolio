"""Integration tests for Neo4j graph AML ring detection and Kafka/Spark Medallion Lakehouse."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gpmorgan_forge.distributed_privacy import MedallionLakehousePipeline
from gpmorgan_forge.nosql_models import GraphFraudEngine


@pytest.mark.integration
def test_graph_fraud_engine_cypher_and_traversals() -> None:
    """Test Neo4j circular loop traversal, shared device clusters, and product recommendations."""
    graph = GraphFraudEngine()
    graph.add_customer_account(
        customer_id="C-LOW",
        customer_name="Low Risk User",
        segment="RETAIL",
        account_number="1001",
        risk_score=10.0,
    )
    graph.add_customer_account(
        customer_id="C-1",
        customer_name="Mule 1",
        segment="SME",
        account_number="9001",
        risk_score=80.0,
    )
    graph.add_customer_account(
        customer_id="C-2",
        customer_name="Mule 2",
        segment="SME",
        account_number="9002",
        risk_score=85.0,
    )
    graph.add_customer_account(
        customer_id="C-3",
        customer_name="Mule 3",
        segment="SME",
        account_number="9003",
        risk_score=90.0,
    )
    graph.add_customer_account(
        customer_id="C-4",
        customer_name="Dead End Node",
        segment="SME",
        account_number="9004",
        risk_score=75.0,
    )

    graph.link_customer_device("C-1", "DEV-SHARED", is_emulator=True)
    graph.link_customer_device("C-1", "DEV-SHARED", is_emulator=True)
    graph.link_customer_device("C-2", "DEV-SHARED", is_emulator=True)
    graph.link_customer_device("C-3", "DEV-SOLO", is_emulator=False)

    graph.record_transfer_edge(
        source_account="9001", destination_account="9002", tx_ref="T1", amount=1000
    )
    graph.record_transfer_edge(
        source_account="9002", destination_account="9003", tx_ref="T2", amount=1000
    )
    graph.record_transfer_edge(
        source_account="9003", destination_account="9001", tx_ref="T3", amount=1000
    )
    graph.record_transfer_edge(
        source_account="9002", destination_account="9004", tx_ref="T4", amount=500
    )
    graph.record_transfer_edge(
        source_account="9004", destination_account="9002", tx_ref="T5", amount=500
    )

    rings = graph.detect_circular_transfer_loops(min_hops=2, max_hops=3, min_risk_score=50.0)
    assert len(rings) == 2

    assert graph.detect_circular_transfer_loops(min_hops=1, max_hops=-1) == []

    clusters = graph.detect_shared_device_mule_clusters()
    assert len(clusters) == 1
    assert clusters[0].device_fingerprint == "DEV-SHARED"

    graph.add_product_subscription(
        customer_id="C-1", product_id="P-FX", product_name="FX Hedging", category="TREASURY"
    )
    graph.add_product_subscription(
        customer_id="C-2", product_id="P-FX", product_name="FX Hedging", category="TREASURY"
    )
    graph.add_product_subscription(
        customer_id="C-2", product_id="P-ESCROW", product_name="Escrow API", category="COMMERCIAL"
    )
    graph.add_business_partnership("C-1", "C-2")

    recs = graph.recommend_financial_products("C-1")
    assert len(recs) == 1
    assert recs[0]["product_id"] == "P-ESCROW"

    cypher = GraphFraudEngine.build_optimised_cypher_aml_query(min_hops=2, max_hops=5)
    assert "TRANSFERRED_TO*2..5" in cypher
    assert "origin.account_number <= n.account_number" in cypher

    with pytest.raises(ValueError, match="Traversal depth"):
        GraphFraudEngine.build_optimised_cypher_aml_query(min_hops=1, max_hops=7)


@pytest.mark.integration
def test_medallion_lakehouse_kafka_spark_and_warehouse_pipeline() -> None:
    """Verify Kafka 4.0 CDC, Iceberg time-travel, Trino federation, and OpenLineage governance."""
    lake = MedallionLakehousePipeline(catalog_branch="audit-2026-q4")
    lake.publish_to_kafka({"reference_code": "R1", "currency": "NGN", "amount": "6000000.0000"})
    lake.publish_to_kafka({"reference_code": "R2", "currency": "USD", "amount": "500.0000"})
    summary = lake.execute_spark_and_warehouse_pipeline(
        high_value_threshold=Decimal("5000000.0000")
    )
    assert summary.kafka_events_ingested == 2
    assert summary.silver_spark_flagged_count == 1
    assert summary.gold_warehouse_total_volume == Decimal("6000500.0000")
    assert summary.catalog_branch == "audit-2026-q4"

    time_travel_rows = lake.query_iceberg_time_travel(summary.iceberg_snapshot_id)
    assert len(time_travel_rows) == 2

    with pytest.raises(KeyError, match="Iceberg snapshot"):
        lake.query_iceberg_time_travel("snap-nonexistent")

    fed = lake.execute_trino_federated_query(
        postgres_ledger_count=10, mongo_kyc_count=3, cassandra_timeline_count=20
    )
    assert fed["zero_etl_federation_verified"] is True
    assert len(lake.openlineage_events) == 1
    event = lake.openlineage_events[0]
    assert event["run"]["facets"]["forge_lakehouse"]["catalog_engine"] == "apache_polaris"
    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "gpmorgan_forge.lakehouse"
    assert summary.gold_reconciled is True
    assert summary.dead_letter_count == 0


@pytest.mark.integration
def test_lakehouse_dead_letter_queue_idempotent_merge_and_reconciliation() -> None:
    """Verify data-contract DLQ routing, replay-safe Silver MERGE, and BCBS 239 reconciliation."""
    lake = MedallionLakehousePipeline()
    good = {"reference_code": "R1", "currency": "ngn", "amount": "100.0000"}
    lake.publish_to_kafka(good)
    lake.publish_to_kafka({"reference_code": "R2", "currency": "USD", "amount": "50.0000"})
    lake.publish_to_kafka(dict(good))  # at-least-once Kafka redelivery of R1
    lake.publish_to_kafka({"reference_code": "R1", "currency": "NGN", "amount": "300.0000"})
    lake.publish_to_kafka({"currency": "NGN", "amount": "10.0000"})  # missing reference_code
    lake.publish_to_kafka({"reference_code": "R3", "currency": "NAIRA", "amount": "10"})
    lake.publish_to_kafka({"reference_code": "R4", "currency": "NGN", "amount": "not-a-number"})
    lake.publish_to_kafka({"reference_code": "R5", "currency": "NGN", "amount": "-5.0000"})
    lake.publish_to_kafka(
        {"reference_code": "R6", "currency": "USD", "amount": "70.0000", "status": "pending"}
    )
    lake.publish_to_kafka(
        {"reference_code": "R7", "currency": "USD", "amount": "30.0000", "status": "FAILED"}
    )

    summary = lake.execute_spark_and_warehouse_pipeline()

    assert summary.kafka_events_ingested == 10  # Bronze keeps every raw event
    assert summary.dead_letter_count == 4
    assert [d["reason"] for d in lake.dead_letter_queue] == [
        "MISSING_REQUIRED_FIELDS:reference_code",
        "INVALID_CURRENCY_CODE",
        "UNPARSEABLE_AMOUNT",
        "NON_POSITIVE_AMOUNT",
    ]
    assert summary.merged_duplicate_count == 2  # R1 replayed and then updated in place
    assert len(lake.silver_spark_enriched) == 4  # R1, R2, R6 (PENDING), R7 (FAILED)
    # Only SETTLED rows reach Gold; PENDING and FAILED stay in Silver.
    assert summary.corridor_breakdown == {"NGN": "300.0000", "USD": "50.0000"}
    assert summary.gold_warehouse_total_volume == Decimal("350.0000")
    assert summary.gold_reconciled is True

    # CDC delivers the PENDING -> SETTLED transition for R6: MERGE updates Silver, Gold follows.
    lake.publish_to_kafka(
        {"reference_code": "R6", "currency": "USD", "amount": "70.0000", "status": "SETTLED"}
    )
    settled = lake.execute_spark_and_warehouse_pipeline()
    assert settled.merged_duplicate_count == 3
    assert settled.corridor_breakdown == {"NGN": "300.0000", "USD": "120.0000"}
    assert settled.gold_reconciled is True

    lake.gold_data_warehouse["NGN"] += Decimal("1.0000")  # tamper with Gold
    assert lake.reconcile_layers() is False
    lake.gold_data_warehouse["NGN"] -= Decimal("1.0000")
    lake.dead_letter_queue.pop()  # lose a dead-lettered event
    assert lake.reconcile_layers() is False


@pytest.mark.integration
def test_lakehouse_openlineage_event_follows_run_event_spec_and_is_deterministic() -> None:
    """OpenLineage RunEvent carries spec-required fields and a deterministic UUID run id."""
    when = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
    runs = []
    for _ in range(2):
        lake = MedallionLakehousePipeline()
        lake.publish_to_kafka({"reference_code": "R1", "currency": "NGN", "amount": "10.0000"})
        summary = lake.execute_spark_and_warehouse_pipeline(event_time=when)
        runs.append((summary.openlineage_run_id, lake.openlineage_events[0]))

    assert runs[0][0] == runs[1][0]
    event = runs[0][1]
    assert str(uuid.UUID(event["run"]["runId"])) == event["run"]["runId"]
    assert event["eventTime"] == when.isoformat()
    assert event["schemaURL"].endswith("#/$defs/RunEvent")
    assert event["producer"].startswith("https://")
    assert {"namespace", "name"} <= set(event["inputs"][0])
    assert {"namespace", "name"} <= set(event["outputs"][0])
