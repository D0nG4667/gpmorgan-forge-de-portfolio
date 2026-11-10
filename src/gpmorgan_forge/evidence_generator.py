"""End-to-end Portfolio Evidence Generator CLI for GPMorgan Forge (Portfolios 1 to 5)."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from gpmorgan_forge.concurrency import summarize_portfolio_3_evidence
from gpmorgan_forge.distributed_privacy import (
    AccessControlEngine,
    DatabaseTelemetryTracer,
    DataPrivacyEngine,
    DistributedClusterRouter,
    FlinkStatefulVelocityEngine,
    MedallionLakehousePipeline,
)
from gpmorgan_forge.models import (
    AccountType,
    Base,
    CurrencyCode,
    CustomerSegment,
    KYCTierLevel,
)
from gpmorgan_forge.nosql_models import (
    DocumentStoreEngine,
    GraphFraudEngine,
    KeyValueStoreEngine,
    TimeSeriesTelemetryEngine,
    WideColumnTimelineEngine,
)
from gpmorgan_forge.operations import (
    audit_account_balances,
    create_account,
    issue_payment_card,
    onboard_customer,
    process_deposit,
    process_withdrawal,
    record_consent,
    reverse_failed_transaction,
    transfer_funds,
    verify_kyc_document,
    whitelist_beneficiary,
)
from gpmorgan_forge.query_optimizer import summarize_portfolio_2_benchmarks


def generate_all_portfolio_evidence() -> dict[str, Any]:
    """Execute all five portfolio workflows against an in-memory polyglot environment."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        # ---------------------------------------------------------------------
        # Portfolio 1: Core Banking Schema, Onboarding, Deposits, Transfers, Reversals
        # ---------------------------------------------------------------------
        gl_cust = onboard_customer(
            session,
            external_ref="CUST-GL-TREASURY",
            full_name="GPMorgan Forge Treasury GL",
            email="treasury.gl@gpmorganforge.internal",
            phone_e164="+2348000000001",
            raw_bvn="BVN-GL-00000000001",
            segment=CustomerSegment.INSTITUTIONAL,
            kyc_tier=KYCTierLevel.TIER_3,
        )
        corp_cust = onboard_customer(
            session,
            external_ref="CUST-NG-00102",
            full_name="Adebayo Ogunlesi Ventures Ltd",
            email="treasury@ogunlesiventures.ng",
            phone_e164="+2348031112202",
            raw_bvn="BVN-NG-22334455662",
            segment=CustomerSegment.CORPORATE,
            kyc_tier=KYCTierLevel.TIER_2,
        )
        retail_cust = onboard_customer(
            session,
            external_ref="CUST-NG-00103",
            full_name="Chidinma Okafor",
            email="chidinma.okafor@example.ng",
            phone_e164="+2348092223303",
            raw_bvn="BVN-NG-22334455663",
            segment=CustomerSegment.RETAIL,
            kyc_tier=KYCTierLevel.TIER_3,
        )

        verify_kyc_document(
            session,
            customer_id=corp_cust.customer_id,
            document_type="CAC_CERTIFICATE",
            raw_document_ref="RC-109283",
            verified_by="compliance.officer.01",
            upgraded_tier=KYCTierLevel.TIER_3,
        )
        record_consent(
            session,
            customer_id=corp_cust.customer_id,
            regulation_code="NDPA_2023",
            purpose_code="CORE_BANKING_PROCESSING",
            consent_granted=True,
            ip_address="102.89.32.10",
        )

        gl_acc = create_account(
            session,
            customer_id=gl_cust.customer_id,
            account_number="1000000001",
            account_name="GPMorgan NGN Central Settlement GL",
            account_type=AccountType.SETTLEMENT_GL,
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("999999999999.0000"),
        )
        corp_acc = create_account(
            session,
            customer_id=corp_cust.customer_id,
            account_number="2000000002",
            account_name="Adebayo Ogunlesi Corporate Operating",
            account_type=AccountType.CHECKING,
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("100000000.0000"),
        )
        retail_acc = create_account(
            session,
            customer_id=retail_cust.customer_id,
            account_number="2000000003",
            account_name="Chidinma Okafor Primary Checking",
            account_type=AccountType.CHECKING,
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("5000000.0000"),
        )

        issue_payment_card(
            session,
            account_id=retail_acc.account_id,
            raw_pan="5061020034004821",
            card_scheme="VERVE",
            expiry_month=11,
            expiry_year=2028,
        )
        whitelist_beneficiary(
            session,
            customer_id=corp_cust.customer_id,
            bank_code="058",
            account_number=retail_acc.account_number,
            beneficiary_name=retail_cust.full_name,
        )

        process_deposit(
            session,
            idempotency_key="IDEMP-EVID-DEP-01",
            reference_code="FRG-EVID-DEP-01",
            settlement_gl_account_id=gl_acc.account_id,
            customer_account_id=corp_acc.account_id,
            amount=Decimal("15000000.0000"),
        )
        tx_transfer_1 = transfer_funds(
            session,
            idempotency_key="IDEMP-EVID-TRF-01",
            reference_code="FRG-EVID-TRF-01",
            source_account_id=corp_acc.account_id,
            destination_account_id=retail_acc.account_id,
            amount=Decimal("1250000.0000"),
            narration="Vendor consultancy milestone settlement",
        )
        tx_transfer_2 = transfer_funds(
            session,
            idempotency_key="IDEMP-EVID-TRF-02",
            reference_code="FRG-EVID-TRF-02",
            source_account_id=corp_acc.account_id,
            destination_account_id=retail_acc.account_id,
            amount=Decimal("750000.0000"),
            narration="Secondary supplier milestone transfer",
        )
        process_withdrawal(
            session,
            idempotency_key="IDEMP-EVID-WDR-01",
            reference_code="FRG-EVID-WDR-01",
            customer_account_id=retail_acc.account_id,
            settlement_gl_account_id=gl_acc.account_id,
            amount=Decimal("100000.0000"),
        )
        reverse_failed_transaction(
            session,
            original_transaction_id=tx_transfer_2.transaction_id,
            reversal_idempotency_key="IDEMP-EVID-REV-01",
            reversal_reference_code="FRG-EVID-REV-01",
            reason="Duplicate invoice billing reversal",
        )

        # ---------------------------------------------------------------------
        # Portfolio 2: Analytical Queries & Execution Plan Benchmarks
        # ---------------------------------------------------------------------
        p2_summary = summarize_portfolio_2_benchmarks(session, corp_acc.account_number)

        # ---------------------------------------------------------------------
        # Portfolio 3: Concurrency & Isolation Evidence
        # ---------------------------------------------------------------------
        p3_summary = summarize_portfolio_3_evidence(
            session,
            source_account_id=corp_acc.account_id,
            destination_account_id=retail_acc.account_id,
        )

        audit_report = audit_account_balances(session)

        # ---------------------------------------------------------------------
        # Portfolio 4: Polyglot NoSQL (MongoDB, Redis, Neo4j Graph)
        # ---------------------------------------------------------------------
        doc_store = DocumentStoreEngine()
        kyc_doc = doc_store.insert_kyc_document(
            customer_id=corp_cust.customer_id,
            external_ref=corp_cust.external_ref,
            jurisdiction="NG",
            kyc_tier="TIER_3",
            verification_status="VERIFIED",
            raw_national_id="22334455662",
            document_artifacts=[
                {"doc_type": "CAC_CERT", "sha256_checksum": "abc123", "liveness_confidence": 0.99}
            ],
        )

        kv_store = KeyValueStoreEngine()
        durable_keys: set[str] = set()
        idemp_ok, _, idemp_tier = kv_store.verify_two_tier_idempotency(
            "IDEMP-EVID-TRF-01",
            '{"status":"SETTLED"}',
            durable_ledger_keys=durable_keys,
        )
        rate_ok, req_count = kv_store.check_sliding_window_rate_limit(
            corp_cust.external_ref, "transfer", now_ms=1762603200000, request_id="req-1"
        )

        cassandra_timeline = WideColumnTimelineEngine()
        cassandra_timeline.append_cdc_timeline_event(
            account_id=corp_acc.account_id,
            bucket_month="2026-11",
            created_at="2026-11-08T12:00:00Z",
            transaction_id=tx_transfer_1.transaction_id,
            reference_code=tx_transfer_1.reference_code,
            direction="DEBIT",
            amount=tx_transfer_1.amount,
            currency="NGN",
            balance_after=corp_acc.balance,
            narration=tx_transfer_1.narration,
        )
        timeline_rows = cassandra_timeline.query_account_timeline(corp_acc.account_id, "2026-11")

        timescale = TimeSeriesTelemetryEngine()
        timescale.ingest_fx_tick(
            tick_time=datetime(2026, 11, 8, 12, 5, tzinfo=UTC),
            corridor_pair="USD/NGN",
            mid_rate="1645.2500",
            settlement_volume="250000.0000",
        )
        timescale.ingest_fx_tick(
            tick_time=datetime(2026, 11, 8, 12, 45, tzinfo=UTC),
            corridor_pair="USD/NGN",
            mid_rate="1648.7500",
            settlement_volume="500000.0000",
        )
        fx_candlesticks = timescale.compute_hourly_continuous_aggregate("USD/NGN")

        graph = GraphFraudEngine()
        graph.add_customer_account(
            customer_id="CUST-MULE-1",
            customer_name="Shell A",
            segment="SME",
            account_number="9000000011",
            risk_score=85.0,
        )
        graph.add_customer_account(
            customer_id="CUST-MULE-2",
            customer_name="Shell B",
            segment="SME",
            account_number="9000000012",
            risk_score=88.0,
        )
        graph.add_customer_account(
            customer_id="CUST-MULE-3",
            customer_name="Shell C",
            segment="SME",
            account_number="9000000013",
            risk_score=90.0,
        )
        graph.link_customer_device("CUST-MULE-1", "DEV-MULE-99", is_emulator=True)
        graph.link_customer_device("CUST-MULE-2", "DEV-MULE-99", is_emulator=True)
        graph.record_transfer_edge(
            source_account="9000000011",
            destination_account="9000000012",
            tx_ref="AML-1",
            amount=Decimal("4950000.0000"),
        )
        graph.record_transfer_edge(
            source_account="9000000012",
            destination_account="9000000013",
            tx_ref="AML-2",
            amount=Decimal("4900000.0000"),
        )
        graph.record_transfer_edge(
            source_account="9000000013",
            destination_account="9000000011",
            tx_ref="AML-3",
            amount=Decimal("4850000.0000"),
        )
        rings = graph.detect_circular_transfer_loops()
        mules = graph.detect_shared_device_mule_clusters()

        # ---------------------------------------------------------------------
        # Portfolio 5: Distributed Routing, Flink, Iceberg Lakehouse, RBAC & Privacy
        # ---------------------------------------------------------------------
        router = DistributedClusterRouter()
        route_decision = router.route_operation(
            account_key=corp_acc.account_id,
            tx_timestamp=datetime(2026, 11, 8, 12, 0, tzinfo=UTC),
            is_write=False,
            replica_lag_ms=15,
        )
        cockroach_decision = router.route_operation(
            account_key=corp_acc.account_id,
            tx_timestamp=datetime(2026, 11, 8, 12, 0, tzinfo=UTC),
            is_write=True,
            is_cross_region=True,
        )

        flink_engine = FlinkStatefulVelocityEngine()
        flink_alert = flink_engine.process_stream_event(
            account_id="9000000011",
            event_ts_ms=1762603200000,
            amount=Decimal("5250000.0000"),
        )

        lakehouse = MedallionLakehousePipeline()
        lakehouse.publish_to_kafka(
            {
                "reference_code": tx_transfer_1.reference_code,
                "currency": "NGN",
                "amount": "1250000.0000",
                "is_suspicious": False,
            }
        )
        lake_summary = lakehouse.execute_spark_and_warehouse_pipeline()
        iceberg_time_travel_rows = lakehouse.query_iceberg_time_travel(
            lake_summary.iceberg_snapshot_id
        )
        trino_federated = lakehouse.execute_trino_federated_query(
            postgres_ledger_count=audit_report.accounts_checked,
            mongo_kyc_count=1,
            cassandra_timeline_count=len(timeline_rows),
        )

        otel_tracer = DatabaseTelemetryTracer()
        otel_tracer.record_span(
            db_system="redis_valkey",
            operation_name="SET_NX_EX_IDEMPOTENCY",
            latency_ms=0.25,
            attributes={"tier": idemp_tier},
        )
        otel_tracer.record_span(
            db_system="postgresql_cockroachdb",
            operation_name="COMMIT_DOUBLE_ENTRY_LEDGER",
            latency_ms=2.40,
            attributes={"multi_region_node": cockroach_decision.target_node},
        )
        otel_tracer.record_span(
            db_system="kafka_kraft",
            operation_name="PUBLISH_CDC_EVENT",
            latency_ms=0.80,
        )
        otel_tracer.record_span(
            db_system="apache_flink",
            operation_name="KEYED_SLIDING_WINDOW_VELOCITY",
            latency_ms=1.15,
        )
        otel_tracer.record_span(
            db_system="neo4j_cypher",
            operation_name="DETECT_CIRCULAR_RINGS_2_TO_5_HOPS",
            latency_ms=1.80,
        )
        otel_tracer.record_span(
            db_system="apache_iceberg_trino",
            operation_name="COMMIT_SNAPSHOT_AND_FEDERATED_JOIN",
            latency_ms=4.10,
        )
        otel_summary = otel_tracer.summarize_telemetry()

        rbac_ok = AccessControlEngine.check_access(
            role="forge_customer",
            permission="READ_OWN_RLS",
            actor_customer_id=retail_cust.customer_id,
            target_customer_id=retail_cust.customer_id,
            actor_country="NG",
            resource_country="NG",
        )
        masked = DataPrivacyEngine.mask_customer_pii(
            full_name=retail_cust.full_name,
            email=retail_cust.email,
            phone_e164=retail_cust.phone_e164,
        )
        dp_val = DataPrivacyEngine.apply_differential_privacy_laplace(
            4125000.0, sensitivity=1000.0, epsilon=1.0
        )
        zkp = DataPrivacyEngine.generate_and_verify_zkp_tier_claim(
            customer_id=retail_cust.customer_id, actual_age=29, actual_tier=3
        )

        session.commit()

        result = {
            "portfolio_1_ledger_audit": audit_report.to_dict(),
            "portfolio_2_query_optimisation": p2_summary,
            "portfolio_3_concurrency": p3_summary,
            "portfolio_4_nosql": {
                "kyc_field_encrypted": kyc_doc.encrypted_pii_fields[
                    "bvn_or_national_id_cipher"
                ].startswith("ENCv2:"),
                "redis_idempotency_acquired": idemp_ok,
                "two_tier_idempotency_status": idemp_tier,
                "redis_rate_limit_allowed": rate_ok and req_count == 1,
                "neo4j_circular_rings_detected": len(rings),
                "neo4j_mule_clusters_detected": len(mules),
                "cassandra_timeline_events": len(timeline_rows),
                "timescaledb_fx_hourly_buckets": len(fx_candlesticks),
            },
            "portfolio_5_distributed_privacy": {
                "routed_node": route_decision.target_node,
                "multi_region_active_node": cockroach_decision.target_node,
                "flink_neo4j_trigger_activated": flink_alert.trigger_neo4j_graph_traversal,
                "lakehouse_gold_volume": str(lake_summary.gold_warehouse_total_volume),
                "iceberg_snapshot_id": lake_summary.iceberg_snapshot_id,
                "iceberg_time_travel_rows": len(iceberg_time_travel_rows),
                "openlineage_run_id": lake_summary.openlineage_run_id,
                "trino_federated_verified": trino_federated["zero_etl_federation_verified"],
                "otel_spans_recorded": otel_summary["span_count"],
                "otel_sla_under_50ms_met": otel_summary["sla_under_50ms_met"],
                "rbac_rls_verified": rbac_ok,
                "masked_pan": masked["masked_pan"],
                "differential_privacy_estimate": dp_val,
                "zkp_verified": zkp["verified"],
            },
        }
    engine.dispose()
    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for generating and verifying portfolio evidence."""
    args = argv if argv is not None else sys.argv[1:]
    evidence = generate_all_portfolio_evidence()
    if "--verify" in args:
        assert evidence["portfolio_1_ledger_audit"]["is_balanced"] is True
        assert evidence["portfolio_1_ledger_audit"]["hash_chain_valid"] is True
        assert evidence["portfolio_3_concurrency"]["deadlock_prevented"] is True
        assert evidence["portfolio_4_nosql"]["neo4j_circular_rings_detected"] == 1
        assert evidence["portfolio_5_distributed_privacy"]["zkp_verified"] is True
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
