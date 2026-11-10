"""Unit tests for NoSQL document/key-value primitives, shard routing, RBAC/RLS, and privacy."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from gpmorgan_forge.distributed_privacy import (
    AccessControlEngine,
    DatabaseTelemetryTracer,
    DataPrivacyEngine,
    DistributedClusterRouter,
    FlinkStatefulVelocityEngine,
    SecurityAccessDeniedError,
)
from gpmorgan_forge.nosql_models import (
    DocumentStoreEngine,
    DocumentValidationError,
    KeyValueStoreEngine,
    TimeSeriesTelemetryEngine,
    WideColumnTimelineEngine,
)


@pytest.mark.unit
def test_field_encryption_is_real_aes_256_gcm() -> None:
    """AES-256-GCM: round-trips, is randomised, binds AAD, and detects any tampering."""
    store = DocumentStoreEngine(kms_master_key=b"k" * 32)
    token = store.encrypt_sensitive_field("22334455667", aad=b"CUST-1")
    assert token.startswith("ENCv2:kms-ng-cbn-key-2026-01:")
    assert "22334455667" not in token
    assert store.decrypt_sensitive_field(token, aad=b"CUST-1") == "22334455667"
    # Randomised nonce: same plaintext never yields the same ciphertext.
    assert token != store.encrypt_sensitive_field("22334455667", aad=b"CUST-1")

    with pytest.raises(DocumentValidationError, match="failed authentication"):
        store.decrypt_sensitive_field(token, aad=b"CUST-2")  # ciphertext swapped to other owner

    prefix, key_id, body = token.split(":", 2)
    flipped = body[:-4] + ("A" if body[-4] != "A" else "B") + body[-3:]
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        store.decrypt_sensitive_field(f"{prefix}:{key_id}:{flipped}", aad=b"CUST-1")
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        store.decrypt_sensitive_field(f"{prefix}:other-key:{body}", aad=b"CUST-1")
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        store.decrypt_sensitive_field(f"{prefix}:{key_id}:!!!notbase64!!!", aad=b"CUST-1")
    with pytest.raises(DocumentValidationError, match="malformed"):
        store.decrypt_sensitive_field("ENCv1:key:abcd")
    with pytest.raises(DocumentValidationError, match="malformed"):
        store.decrypt_sensitive_field("garbage")

    other = DocumentStoreEngine(kms_master_key=b"z" * 32)
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        other.decrypt_sensitive_field(token, aad=b"CUST-1")  # different master key


@pytest.mark.unit
def test_kyc_national_id_is_bound_to_owner_and_revealable() -> None:
    """Stored KYC ciphertext decrypts for its owner and is useless for any other customer."""
    store = DocumentStoreEngine(kms_master_key=b"m" * 32)
    artifact = [{"doc_type": "NIN", "liveness_confidence": 0.9}]
    doc = store.insert_kyc_document(
        customer_id="CUST-A",
        external_ref="E-A",
        jurisdiction="NG",
        kyc_tier="TIER_2",
        verification_status="VERIFIED",
        raw_national_id="98765432101",
        document_artifacts=artifact,
    )
    assert doc.encrypted_pii_fields["encryption_algorithm"] == "AEAD_AES_256_GCM"
    assert "98765432101" not in doc.encrypted_pii_fields["bvn_or_national_id_cipher"]
    assert store.reveal_national_id("CUST-A") == "98765432101"
    with pytest.raises(DocumentValidationError, match="No KYC document"):
        store.reveal_national_id("CUST-MISSING")
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        store.decrypt_sensitive_field(
            doc.encrypted_pii_fields["bvn_or_national_id_cipher"], aad=b"CUST-B"
        )


@pytest.mark.unit
def test_kms_master_key_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Master key comes from env (base64) or is a random ephemeral key; short keys are refused."""
    import base64

    with pytest.raises(DocumentValidationError, match="at least 32 bytes"):
        DocumentStoreEngine(kms_master_key=b"short")

    env_key = b"e" * 32
    monkeypatch.setenv("FORGE_KMS_MASTER_KEY", base64.b64encode(env_key).decode())
    from_env = DocumentStoreEngine()
    same = DocumentStoreEngine(kms_master_key=env_key)
    token = from_env.encrypt_sensitive_field("BVN-1")
    assert same.decrypt_sensitive_field(token) == "BVN-1"

    monkeypatch.delenv("FORGE_KMS_MASTER_KEY")
    ephemeral_a, ephemeral_b = DocumentStoreEngine(), DocumentStoreEngine()
    token_a = ephemeral_a.encrypt_sensitive_field("BVN-2")
    assert ephemeral_a.decrypt_sensitive_field(token_a) == "BVN-2"
    with pytest.raises(DocumentValidationError, match="failed authentication"):
        ephemeral_b.decrypt_sensitive_field(token_a)  # no shared hard-coded key


@pytest.mark.unit
def test_document_store_engine_validation_and_telemetry() -> None:
    """Test MongoDB $jsonSchema validation, field encryption, and device telemetry aggregation."""
    store = DocumentStoreEngine()

    with pytest.raises(DocumentValidationError, match="cannot be empty"):
        store.encrypt_sensitive_field("   ")

    valid_artifact = [{"doc_type": "NIN", "liveness_confidence": 0.98}]

    with pytest.raises(DocumentValidationError, match="Unsupported jurisdiction"):
        store.insert_kyc_document(
            customer_id="C1",
            external_ref="E1",
            jurisdiction="ZZ",
            kyc_tier="TIER_1",
            verification_status="VERIFIED",
            raw_national_id="12345",
            document_artifacts=valid_artifact,
        )

    with pytest.raises(DocumentValidationError, match="Invalid kyc_tier"):
        store.insert_kyc_document(
            customer_id="C1",
            external_ref="E1",
            jurisdiction="NG",
            kyc_tier="TIER_99",
            verification_status="VERIFIED",
            raw_national_id="12345",
            document_artifacts=valid_artifact,
        )

    with pytest.raises(DocumentValidationError, match="Invalid verification_status"):
        store.insert_kyc_document(
            customer_id="C1",
            external_ref="E1",
            jurisdiction="NG",
            kyc_tier="TIER_1",
            verification_status="UNKNOWN_STATE",
            raw_national_id="12345",
            document_artifacts=valid_artifact,
        )

    with pytest.raises(DocumentValidationError, match="At least one document artifact"):
        store.insert_kyc_document(
            customer_id="C1",
            external_ref="E1",
            jurisdiction="NG",
            kyc_tier="TIER_1",
            verification_status="VERIFIED",
            raw_national_id="12345",
            document_artifacts=[],
        )

    with pytest.raises(DocumentValidationError, match="liveness_confidence"):
        store.insert_kyc_document(
            customer_id="C1",
            external_ref="E1",
            jurisdiction="NG",
            kyc_tier="TIER_1",
            verification_status="VERIFIED",
            raw_national_id="12345",
            document_artifacts=[{"doc_type": "NIN", "liveness_confidence": 1.5}],
        )

    doc = store.insert_kyc_document(
        customer_id="C1",
        external_ref="E1",
        jurisdiction="NG",
        kyc_tier="TIER_3",
        verification_status="VERIFIED",
        raw_national_id="22334455667",
        document_artifacts=valid_artifact,
    )
    assert store.get_kyc_document("C1") == doc
    assert store.get_kyc_document("MISSING") is None

    store.record_device_telemetry(
        event_id="EV-1",
        customer_id="C1",
        account_id="A1",
        device_fingerprint="FP-SHARED",
        ip_address="102.89.1.10",
        is_emulator=True,
        is_vpn_or_tor=True,
    )
    store.record_device_telemetry(
        event_id="EV-2",
        customer_id="C2",
        account_id="A2",
        device_fingerprint="FP-SHARED",
        ip_address="102.89.1.11",
        is_emulator=True,
        is_vpn_or_tor=False,
    )
    store.record_device_telemetry(
        event_id="EV-3",
        customer_id="C3",
        account_id="A3",
        device_fingerprint="FP-SOLO",
        ip_address="102.89.1.12",
        is_emulator=False,
        is_vpn_or_tor=False,
    )
    shared = store.aggregate_shared_devices()
    assert len(shared) == 1
    assert shared[0]["device_fingerprint"] == "FP-SHARED"
    assert shared[0]["vpn_hits"] == 1


@pytest.mark.unit
def test_key_value_store_engine_redis_patterns() -> None:
    """Test Redis idempotency locking, account summary caching, and ZSET rate limiting."""
    kv = KeyValueStoreEngine()

    acquired_1, payload_1 = kv.acquire_idempotency_lock("KEY-1", '{"tx":"1"}')
    assert acquired_1 is True
    assert payload_1 == '{"tx":"1"}'

    acquired_2, payload_2 = kv.acquire_idempotency_lock("KEY-1", '{"tx":"duplicate"}')
    assert acquired_2 is False
    assert payload_2 == '{"tx":"1"}'

    cache_key = kv.cache_account_summary(
        "2000000001",
        balance=Decimal("5000.0000"),
        available_balance=Decimal("5000.0000"),
        version=2,
        status="ACTIVE",
    )
    assert cache_key == "forge:account:2000000001:summary"
    cached = kv.get_cached_account_summary("2000000001")
    assert cached is not None
    assert cached["balance"] == "5000.0000"

    assert kv.invalidate_account_cache("2000000001") is True
    assert kv.invalidate_account_cache("2000000001") is False
    assert kv.get_cached_account_summary("2000000001") is None

    ok1, c1 = kv.check_sliding_window_rate_limit(
        "CUST-1", "transfer", now_ms=1000, window_ms=500, max_requests=2, request_id="r1"
    )
    ok2, c2 = kv.check_sliding_window_rate_limit(
        "CUST-1", "transfer", now_ms=1200, window_ms=500, max_requests=2, request_id="r2"
    )
    ok3, c3 = kv.check_sliding_window_rate_limit(
        "CUST-1", "transfer", now_ms=1300, window_ms=500, max_requests=2, request_id="r3"
    )
    assert (ok1, c1) == (True, 1)
    assert (ok2, c2) == (True, 2)
    assert (ok3, c3) == (False, 2)

    ok4, c4 = kv.check_sliding_window_rate_limit(
        "CUST-1", "transfer", now_ms=1600, window_ms=500, max_requests=2, request_id="r4"
    )
    assert (ok4, c4) == (True, 2)

    durable_ledger: set[str] = {"KEY-PREEXISTING-DURABLE"}
    t2_new, t2_payload, t2_stage = kv.verify_two_tier_idempotency(
        "KEY-NEW-01", '{"tx":"new"}', durable_ledger_keys=durable_ledger
    )
    assert (t2_new, t2_payload, t2_stage) == (True, '{"tx":"new"}', "NEW_ATOMIC_SETTLEMENT")
    assert "KEY-NEW-01" in durable_ledger

    t1_dup, _, t1_stage = kv.verify_two_tier_idempotency(
        "KEY-NEW-01", '{"tx":"retry"}', durable_ledger_keys=durable_ledger
    )
    assert (t1_dup, t1_stage) == (False, "TIER_1_REDIS_VALKEY_FAST_PATH")

    t2_dup, _, t2_stage_hit = kv.verify_two_tier_idempotency(
        "KEY-PREEXISTING-DURABLE",
        '{"tx":"post-ttl-retry"}',
        durable_ledger_keys=durable_ledger,
    )
    assert (t2_dup, t2_stage_hit) == (False, "TIER_2_POSTGRES_UNIQUE_CONSTRAINT")

    timeline = WideColumnTimelineEngine()
    with pytest.raises(ValueError, match="direction must be"):
        timeline.append_cdc_timeline_event(
            account_id="A1",
            bucket_month="2026-11",
            created_at="2026-11-01T10:00:00Z",
            transaction_id="TX-1",
            reference_code="REF-1",
            direction="INVALID",
            amount="100.0000",
            currency="NGN",
            balance_after="100.0000",
            narration="bad",
        )

    row1 = timeline.append_cdc_timeline_event(
        account_id="A1",
        bucket_month="2026-11",
        created_at="2026-11-01T10:00:00Z",
        transaction_id="TX-1",
        reference_code="REF-1",
        direction="CREDIT",
        amount="1000.0000",
        currency="NGN",
        balance_after="1000.0000",
        narration="deposit",
    )
    # Duplicate CDC replay is idempotent
    assert (
        timeline.append_cdc_timeline_event(
            account_id="A1",
            bucket_month="2026-11",
            created_at="2026-11-01T10:00:00Z",
            transaction_id="TX-1",
            reference_code="REF-1",
            direction="CREDIT",
            amount="1000.0000",
            currency="NGN",
            balance_after="1000.0000",
            narration="deposit",
        )
        == row1
    )
    timeline.append_cdc_timeline_event(
        account_id="A1",
        bucket_month="2026-11",
        created_at="2026-11-02T10:00:00Z",
        transaction_id="TX-2",
        reference_code="REF-2",
        direction="DEBIT",
        amount="200.0000",
        currency="NGN",
        balance_after="800.0000",
        narration="transfer",
    )
    sliced = timeline.query_account_timeline("A1", "2026-11")
    assert len(sliced) == 2
    assert sliced[0].transaction_id == "TX-2"
    with pytest.raises(PermissionError, match="append-only CDC timeline projections"):
        WideColumnTimelineEngine.forbid_mutable_balance_update()

    ts_engine = TimeSeriesTelemetryEngine()
    with pytest.raises(ValueError, match="mid_rate must be > 0"):
        ts_engine.ingest_fx_tick(
            tick_time=datetime(2026, 11, 8, 12, 0, tzinfo=UTC),
            corridor_pair="USD/NGN",
            mid_rate="0",
            settlement_volume="100",
        )
    ts_engine.ingest_fx_tick(
        tick_time=datetime(2026, 11, 8, 12, 10, tzinfo=UTC),
        corridor_pair="USD/NGN",
        mid_rate="1640.0000",
        settlement_volume="10000.0000",
    )
    ts_engine.ingest_fx_tick(
        tick_time=datetime(2026, 11, 8, 12, 50, tzinfo=UTC),
        corridor_pair="USD/NGN",
        mid_rate="1650.0000",
        settlement_volume="20000.0000",
    )
    candles = ts_engine.compute_hourly_continuous_aggregate("USD/NGN")
    assert len(candles) == 1
    assert candles[0].open_rate == Decimal("1640.0000")
    assert candles[0].close_rate == Decimal("1650.0000")
    assert candles[0].total_volume == Decimal("30000.0000")


@pytest.mark.unit
def test_distributed_router_rbac_masking_dp_and_zkp_primitives() -> None:
    """Test Portfolio 5 shard/replica routing, RBAC/ABAC/RLS, masking, DP, and ZKP primitives."""
    with pytest.raises(ValueError, match="num_shards must be >= 1"):
        DistributedClusterRouter(num_shards=0)

    router = DistributedClusterRouter(num_shards=4, max_replica_lag_ms=50)
    assert (
        router.resolve_range_partition(datetime(2026, 8, 1, tzinfo=UTC))
        == "transactions_part_2026_q3"
    )
    assert (
        router.resolve_range_partition(datetime(2026, 11, 1, tzinfo=UTC))
        == "transactions_part_2026_q4"
    )
    assert (
        router.resolve_range_partition(datetime(2027, 2, 1, tzinfo=UTC))
        == "transactions_part_2027_q1"
    )
    assert (
        router.resolve_range_partition(datetime(2028, 5, 1, tzinfo=UTC))
        == "transactions_part_default"
    )

    d_write = router.route_operation(
        account_key="ACC-1",
        tx_timestamp=datetime(2026, 11, 1, tzinfo=UTC),
        is_write=True,
    )
    assert d_write.target_node == "postgres-primary"

    d_crdb = router.route_operation(
        account_key="ACC-1",
        tx_timestamp=datetime(2026, 11, 1, tzinfo=UTC),
        is_write=True,
        is_cross_region=True,
    )
    assert d_crdb.target_node == "cockroachdb-multi-region"
    assert d_crdb.consistency_level == "RAFT_SERIALIZABLE_ZERO_RPO"

    d_strong = router.route_operation(
        account_key="ACC-1",
        tx_timestamp=datetime(2026, 11, 1, tzinfo=UTC),
        is_write=False,
        require_strong_consistency=True,
    )
    assert d_strong.consistency_level == "STRONG_READ_YOUR_WRITES"

    d_lag = router.route_operation(
        account_key="ACC-1",
        tx_timestamp=datetime(2026, 11, 1, tzinfo=UTC),
        is_write=False,
        replica_lag_ms=250,
    )
    assert d_lag.consistency_level == "FALLBACK_PRIMARY_ON_LAG"

    d_rep = router.route_operation(
        account_key="ACC-1",
        tx_timestamp=datetime(2026, 11, 1, tzinfo=UTC),
        is_write=False,
        replica_lag_ms=10,
    )
    assert d_rep.target_node == "postgres-replica"

    with pytest.raises(SecurityAccessDeniedError, match="RBAC Denied"):
        AccessControlEngine.check_access(role="forge_analyst", permission="WRITE_OLTP")

    with pytest.raises(SecurityAccessDeniedError, match="RLS Denied"):
        AccessControlEngine.check_access(
            role="forge_customer",
            permission="READ_OWN_RLS",
            actor_customer_id="C1",
            target_customer_id="C2",
        )

    with pytest.raises(SecurityAccessDeniedError, match="ABAC Data Residency Denied"):
        AccessControlEngine.check_access(
            role="forge_compliance_dpo",
            permission="READ_RAW",
            actor_country="GB",
            resource_country="NG",
        )

    assert (
        AccessControlEngine.check_access(
            role="forge_admin",
            permission="READ_RAW",
            actor_country="GB",
            resource_country="NG",
        )
        is True
    )

    masked_short = DataPrivacyEngine.mask_customer_pii(
        full_name="A",
        email="x",
        phone_e164="123",
        raw_or_masked_pan="99",
    )
    assert masked_short["masked_name"] == "A***"
    assert masked_short["masked_email"] == "***"
    assert masked_short["masked_phone"] == "******"
    assert masked_short["masked_pan"] == "XXXX-XXXX-XXXX-0000"

    with pytest.raises(ValueError, match="epsilon must be > 0"):
        DataPrivacyEngine.apply_differential_privacy_laplace(100.0, sensitivity=1.0, epsilon=0.0)

    dp_pos = DataPrivacyEngine.apply_differential_privacy_laplace(
        1000.0, sensitivity=10.0, epsilon=1.0, deterministic_seed="seed-alpha"
    )
    dp_neg = DataPrivacyEngine.apply_differential_privacy_laplace(
        1000.0, sensitivity=10.0, epsilon=1.0, deterministic_seed="seed-beta"
    )
    assert isinstance(dp_pos, float)
    assert isinstance(dp_neg, float)

    zkp_ok = DataPrivacyEngine.generate_and_verify_zkp_tier_claim(
        customer_id="C1", actual_age=25, actual_tier=3
    )
    assert zkp_ok["verified"] is True

    zkp_fail = DataPrivacyEngine.generate_and_verify_zkp_tier_claim(
        customer_id="C1", actual_age=16, actual_tier=1
    )
    assert zkp_fail["verified"] is False

    flink = FlinkStatefulVelocityEngine()
    f_normal = flink.process_stream_event(
        account_id="ACC-1", event_ts_ms=1000, amount=Decimal("1000.0000")
    )
    assert f_normal.trigger_neo4j_graph_traversal is False
    f_trigger = flink.process_stream_event(
        account_id="ACC-1", event_ts_ms=2000, amount=Decimal("6000000.0000")
    )
    assert f_trigger.trigger_neo4j_graph_traversal is True

    tracer = DatabaseTelemetryTracer()
    with pytest.raises(ValueError, match="latency_ms must be >="):
        tracer.record_span(db_system="redis_valkey", operation_name="GET", latency_ms=-1.0)
    span = tracer.record_span(
        db_system="postgresql_cockroachdb",
        operation_name="COMMIT",
        latency_ms=2.5,
        attributes={"isolation": "SERIALIZABLE"},
    )
    assert span.db_system == "postgresql_cockroachdb"
    summary = tracer.summarize_telemetry()
    assert summary["span_count"] == 1
    assert summary["sla_under_50ms_met"] is True
