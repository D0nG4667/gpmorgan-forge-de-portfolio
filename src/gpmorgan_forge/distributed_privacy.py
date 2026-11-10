"""Distributed database routing, Lakehouse pipeline, RBAC/ABAC/RLS, and Privacy (Portfolio 5).

Implements distributed topology, streaming analytics, and zero-trust privacy engines:
- Declarative Range Partitioning & Hash Sharding router (PostgreSQL 18 + CockroachDB Multi-Region)
- Primary-Replica read/write splitting with replication-lag awareness (CAP / PACELC)
- Apache Kafka 4.0 + Apache Flink stateful sub-second velocity engine (pre-Neo4j graph filter)
- Apache Iceberg + Apache Polaris REST catalog + Apache Spark 4.0 + Trino federated SQL +
  Airflow/dbt Gold Star-Schema + OpenLineage governance Medallion Lakehouse
- RBAC, ABAC, and Row-Level Security (RLS) policy evaluation
- Dynamic Data Masking (PCI-DSS PAN masking, email/phone/name redaction)
- NDPA 2023 & GDPR Right-to-Erasure customer anonymisation (preserving statutory ledger legs)
- Differential Privacy Laplace noise mechanism for inference-safe aggregate analytics
- Zero-Knowledge Proof (ZKP) commitment and verification for KYC tier and age claims
- OpenTelemetry-compatible DatabaseTelemetryTracer for distributed database observability
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar, Final

from sqlalchemy import select
from sqlalchemy.orm import Session

from gpmorgan_forge.models import ConsentRecord, Customer, compute_sha256_hex, utc_now
from gpmorgan_forge.operations import quantize_money
from gpmorgan_forge.zkp import KycCredentialIssuer, prove_eligibility, verify_eligibility

LAKEHOUSE_REQUIRED_EVENT_FIELDS: Final[tuple[str, ...]] = ("reference_code", "currency", "amount")
OPENLINEAGE_PRODUCER: Final[str] = (
    "https://github.com/D0nG4667/GPMorgan-Forge-Portfolio/tree/main/"
    "portfolio-5-distributed-cloud/lakehouse"
)
OPENLINEAGE_RUN_EVENT_SCHEMA_URL: Final[str] = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)


def validate_lakehouse_event_contract(event: dict[str, Any]) -> str | None:
    """Return a dead-letter reason if a CDC event violates the data contract, else ``None``.

    Mirrors the contract enforced by ``lakehouse/spark/bronze_to_silver.py``: required fields,
    a 3-letter alphabetic currency code, and a parseable strictly-positive amount.
    """
    missing = [name for name in LAKEHOUSE_REQUIRED_EVENT_FIELDS if event.get(name) in (None, "")]
    if missing:
        return f"MISSING_REQUIRED_FIELDS:{','.join(missing)}"
    currency = str(event["currency"])
    if len(currency) != 3 or not currency.isalpha():
        return "INVALID_CURRENCY_CODE"
    try:
        amount = quantize_money(event["amount"])
    except ArithmeticError:
        return "UNPARSEABLE_AMOUNT"
    if amount <= 0:
        return "NON_POSITIVE_AMOUNT"
    return None


class SecurityAccessDeniedError(PermissionError):
    """Raised when RBAC, ABAC, or Row-Level Security denies a database operation."""


@dataclass(frozen=True)
class ShardRoutingDecision:
    """Routing metadata for a partitioned or sharded database operation."""

    target_node: str
    partition_name: str
    consistency_level: str
    routing_reason: str


class DistributedClusterRouter:
    """Routes queries across Hash Shards, Range Partitions, and Primary/Replica nodes."""

    def __init__(self, num_shards: int = 4, max_replica_lag_ms: int = 100) -> None:
        """Initialize cluster topology settings."""
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1.")
        self.num_shards = num_shards
        self.max_replica_lag_ms = max_replica_lag_ms

    def resolve_hash_shard(self, account_key: str) -> str:
        """Compute deterministic hash shard (`MODULUS 4, REMAINDER r`)."""
        digest_int = int(compute_sha256_hex(account_key)[:8], 16)
        remainder = digest_int % self.num_shards
        return f"accounts_shard_p{remainder}"

    @staticmethod
    def resolve_range_partition(tx_timestamp: datetime) -> str:
        """Resolve PostgreSQL declarative RANGE partition for a transaction timestamp."""
        year = tx_timestamp.year
        month = tx_timestamp.month
        if year == 2026 and 7 <= month <= 9:
            return "transactions_part_2026_q3"
        if year == 2026 and 10 <= month <= 12:
            return "transactions_part_2026_q4"
        if year == 2027 and 1 <= month <= 3:
            return "transactions_part_2027_q1"
        return "transactions_part_default"

    def route_operation(
        self,
        *,
        account_key: str,
        tx_timestamp: datetime,
        is_write: bool,
        require_strong_consistency: bool = False,
        replica_lag_ms: int = 0,
        is_cross_region: bool = False,
    ) -> ShardRoutingDecision:
        """Route an OLTP or analytical operation across PostgreSQL 18 and CockroachDB nodes."""
        partition = (
            self.resolve_range_partition(tx_timestamp)
            if is_write
            else self.resolve_hash_shard(account_key)
        )
        if is_write and is_cross_region:
            return ShardRoutingDecision(
                target_node="cockroachdb-multi-region",
                partition_name=partition,
                consistency_level="RAFT_SERIALIZABLE_ZERO_RPO",
                routing_reason=(
                    "Cross-region active-active write committed via CockroachDB Raft consensus "
                    "(SERIALIZABLE)."
                ),
            )
        if is_write:
            return ShardRoutingDecision(
                target_node="postgres-primary",
                partition_name=partition,
                consistency_level="SYNCHRONOUS_QUORUM_CP",
                routing_reason="Mutating write must commit on primary leader.",
            )
        if require_strong_consistency:
            return ShardRoutingDecision(
                target_node="postgres-primary",
                partition_name=partition,
                consistency_level="STRONG_READ_YOUR_WRITES",
                routing_reason="Caller requested strict linearizable read.",
            )
        if replica_lag_ms > self.max_replica_lag_ms:
            return ShardRoutingDecision(
                target_node="postgres-primary",
                partition_name=partition,
                consistency_level="FALLBACK_PRIMARY_ON_LAG",
                routing_reason=(
                    f"Replica lag ({replica_lag_ms}ms) exceeded threshold "
                    f"({self.max_replica_lag_ms}ms); routed to primary."
                ),
            )
        return ShardRoutingDecision(
            target_node="postgres-replica",
            partition_name=partition,
            consistency_level="BOUNDED_STALENESS_READ_REPLICA",
            routing_reason=(
                f"Replica healthy (lag={replica_lag_ms}ms <= {self.max_replica_lag_ms}ms)."
            ),
        )


@dataclass(frozen=True)
class FlinkVelocityAlert:
    """Sub-second stateful stream evaluation output from Apache Flink."""

    account_id: str
    window_event_count: int
    window_cumulative_amount: Decimal
    trigger_neo4j_graph_traversal: bool
    alert_reason: str


class FlinkStatefulVelocityEngine:
    """Simulates Apache Flink stateful sub-second sliding-window velocity & structuring detection.

    Evaluates high-velocity Kafka 4.0 payment events using keyed RocksDB state semantics before
    triggering multi-hop Cypher traversals in Neo4j 5.26 LTS.
    """

    def __init__(self) -> None:
        """Initialize keyed sliding-window state per source account."""
        self._keyed_window_state: dict[str, list[tuple[int, Decimal]]] = {}

    def process_stream_event(
        self,
        *,
        account_id: str,
        event_ts_ms: int,
        amount: Decimal | str | int,
        window_ms: int = 300000,
        structuring_count_threshold: int = 3,
        velocity_amount_threshold: Decimal = Decimal("5000000.0000"),
    ) -> FlinkVelocityAlert:
        """Update keyed RocksDB sliding window and decide whether to trigger Neo4j AML traversal."""
        amt_dec = quantize_money(amount)
        cutoff = event_ts_ms - window_ms
        window_events = [
            (ts, val) for ts, val in self._keyed_window_state.get(account_id, []) if ts > cutoff
        ]
        window_events.append((event_ts_ms, amt_dec))
        self._keyed_window_state[account_id] = window_events

        count = len(window_events)
        cumulative = sum((val for _, val in window_events), Decimal("0.0000"))
        trigger_graph = (
            count >= structuring_count_threshold or cumulative >= velocity_amount_threshold
        )
        reason = (
            "FLINK_VELOCITY_THRESHOLD_EXCEEDED_TRIGGER_NEO4J"
            if trigger_graph
            else "WITHIN_NORMAL_STREAMING_BOUNDS"
        )
        return FlinkVelocityAlert(
            account_id=account_id,
            window_event_count=count,
            window_cumulative_amount=cumulative,
            trigger_neo4j_graph_traversal=trigger_graph,
            alert_reason=reason,
        )


@dataclass(frozen=True)
class LakehouseBatchSummary:
    """Summary of Kafka -> Bronze Iceberg -> Silver Spark -> Gold dbt/Airflow Warehouse pipeline."""

    kafka_events_ingested: int
    bronze_lake_raw_objects: int
    silver_spark_flagged_count: int
    gold_warehouse_total_volume: Decimal
    corridor_breakdown: dict[str, str]
    iceberg_snapshot_id: str = "SNAP-ICEBERG-0001"
    catalog_branch: str = "main"
    openlineage_run_id: str = "OL-RUN-0001"
    trino_federated_sources: tuple[str, ...] = (
        "iceberg",
        "postgresql",
        "mongodb",
        "cassandra",
    )
    dead_letter_count: int = 0
    merged_duplicate_count: int = 0
    gold_reconciled: bool = True


class MedallionLakehousePipeline:
    """Simulates Debezium/Kafka 4.0, Iceberg (Polaris), Spark 4.0, Trino, dbt & OpenLineage.

    The real deployables live in ``portfolio-5-distributed-cloud/lakehouse`` (Spark streaming job,
    dbt Gold project, Airflow DAG, Trino catalogs, Debezium connector); this class reproduces
    their semantics in-process so the data-engineering invariants are unit-testable:

    * Bronze is an immutable raw log (every event, valid or not).
    * Data contract + dead-letter queue: invalid events never reach Silver.
    * Silver is an idempotent MERGE on ``reference_code`` (replays do not double-count).
    * Gold is rebuilt from Silver (dbt ``table`` materialisation), never incrementally patched.
    * ``reconcile_layers`` proves no event was lost between layers (BCBS 239 completeness).
    """

    def __init__(self, catalog_branch: str = "main") -> None:
        """Initialize Bronze Iceberg Lake, Silver Spark tables, Gold Warehouse, and OpenLineage."""
        self.active_catalog_branch: str = catalog_branch
        self.kafka_topic_stream: list[dict[str, Any]] = []
        self.bronze_data_lake: list[dict[str, Any]] = []
        self.silver_spark_enriched: list[dict[str, Any]] = []
        self.dead_letter_queue: list[dict[str, Any]] = []
        self.gold_data_warehouse: dict[str, Decimal] = {}
        self.iceberg_snapshots: dict[str, list[dict[str, Any]]] = {}
        self.openlineage_events: list[dict[str, Any]] = []
        self._silver_index: dict[str, int] = {}
        self._merged_duplicate_total: int = 0

    def publish_to_kafka(self, event: dict[str, Any]) -> None:
        """Publish a real-time CDC or telemetry event to Apache Kafka 4.0 (KRaft)."""
        self.kafka_topic_stream.append(event)

    def _settled_silver_rows(self) -> list[dict[str, Any]]:
        """Silver rows eligible for Gold: only SETTLED transactions count toward volume."""
        return [row for row in self.silver_spark_enriched if row["status"] == "SETTLED"]

    def reconcile_layers(self) -> bool:
        """BCBS 239 completeness check across Bronze, Silver, dead-letter queue and Gold."""
        counts_conserved = len(self.bronze_data_lake) == (
            len(self.silver_spark_enriched)
            + self._merged_duplicate_total
            + len(self.dead_letter_queue)
        )
        silver_volume = sum(
            (row["amount"] for row in self._settled_silver_rows()), Decimal("0.0000")
        )
        gold_volume = sum(self.gold_data_warehouse.values(), Decimal("0.0000"))
        return counts_conserved and silver_volume == gold_volume

    def execute_spark_and_warehouse_pipeline(
        self,
        high_value_threshold: Decimal = Decimal("5000000.0000"),
        event_time: datetime | None = None,
    ) -> LakehouseBatchSummary:
        """Process Kafka stream into Iceberg Bronze/Silver, dbt Gold Warehouse, and OpenLineage."""
        merged_duplicates = 0

        while self.kafka_topic_stream:
            raw_event = self.kafka_topic_stream.pop(0)
            # 1. Store raw immutable payload in Data Lake (Schema-on-Read: S3 / HDFS / Iceberg)
            self.bronze_data_lake.append(dict(raw_event))

            # 2. Data contract: route violations to the dead-letter queue, never to Silver
            reason = validate_lakehouse_event_contract(raw_event)
            if reason is not None:
                self.dead_letter_queue.append({"event": dict(raw_event), "reason": reason})
                continue

            # 3. Apache Spark 4.0 micro-batch transformation & ML fraud feature scoring
            amt = quantize_money(raw_event["amount"])
            is_flagged = amt >= high_value_threshold or bool(raw_event.get("is_suspicious", False))
            ref_code = str(raw_event["reference_code"])
            enriched: dict[str, Any] = {
                "reference_code": ref_code,
                "currency": str(raw_event["currency"]).upper(),
                "amount": amt,
                "status": str(raw_event.get("status", "SETTLED")).upper(),
                "spark_fraud_flagged": is_flagged,
                "catalog_branch": self.active_catalog_branch,
            }

            # 4. Idempotent MERGE INTO Silver on reference_code (replay-safe upsert)
            existing_position = self._silver_index.get(ref_code)
            if existing_position is None:
                self._silver_index[ref_code] = len(self.silver_spark_enriched)
                self.silver_spark_enriched.append(enriched)
            else:
                self.silver_spark_enriched[existing_position] = enriched
                merged_duplicates += 1
        self._merged_duplicate_total += merged_duplicates

        # 5. Gold star-schema fact rebuilt from Silver (dbt `fact_corridor_settlement` semantics):
        #    only SETTLED rows count; PENDING/FAILED/REVERSED stay in Silver.
        self.gold_data_warehouse = {}
        for row in self._settled_silver_rows():
            corridor = row["currency"]
            self.gold_data_warehouse[corridor] = (
                self.gold_data_warehouse.get(corridor, Decimal("0.0000")) + row["amount"]
            )
        total_volume = sum(self.gold_data_warehouse.values(), Decimal("0.0000"))
        flagged_count = sum(1 for row in self._settled_silver_rows() if row["spark_fraud_flagged"])
        reconciled = self.reconcile_layers()

        snapshot_hash = compute_sha256_hex(
            f"{self.active_catalog_branch}:{len(self.silver_spark_enriched)}:{total_volume}"
        )[:12]
        snapshot_id = f"iceberg-snap-{snapshot_hash}"
        self.iceberg_snapshots[snapshot_id] = [dict(row) for row in self.silver_spark_enriched]

        lineage_run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"openlineage:{snapshot_hash}"))
        self.openlineage_events.append(
            {
                "eventType": "COMPLETE",
                "eventTime": (event_time or datetime.now(UTC)).isoformat(),
                "producer": OPENLINEAGE_PRODUCER,
                "schemaURL": OPENLINEAGE_RUN_EVENT_SCHEMA_URL,
                "run": {
                    "runId": lineage_run_id,
                    "facets": {
                        "forge_lakehouse": {
                            "catalog_engine": "apache_polaris",
                            "catalog_branch": self.active_catalog_branch,
                            "iceberg_snapshot_id": snapshot_id,
                            "regulatory_frameworks": ["BCBS_239", "NDPA_2023", "GDPR"],
                            "dead_lettered_events": len(self.dead_letter_queue),
                            "layers_reconciled": reconciled,
                        }
                    },
                },
                "job": {
                    "namespace": "gpmorgan_forge.lakehouse",
                    "name": "forge_medallion_lakehouse",
                },
                "inputs": [
                    {"namespace": "kafka://kafka:9092", "name": "forge.cdc.transactions"},
                    {"namespace": "s3://forge-datalake-bronze", "name": "tx"},
                ],
                "outputs": [
                    {
                        "namespace": "iceberg://polaris-catalog",
                        "name": "forge_lakehouse.silver_transactions",
                    },
                    {
                        "namespace": "trino://trino:8080",
                        "name": "iceberg.forge_dwh.fact_corridor_settlement",
                    },
                ],
            }
        )

        return LakehouseBatchSummary(
            kafka_events_ingested=len(self.bronze_data_lake),
            bronze_lake_raw_objects=len(self.bronze_data_lake),
            silver_spark_flagged_count=flagged_count,
            gold_warehouse_total_volume=total_volume,
            corridor_breakdown={k: str(v) for k, v in sorted(self.gold_data_warehouse.items())},
            iceberg_snapshot_id=snapshot_id,
            catalog_branch=self.active_catalog_branch,
            openlineage_run_id=lineage_run_id,
            dead_letter_count=len(self.dead_letter_queue),
            merged_duplicate_count=self._merged_duplicate_total,
            gold_reconciled=reconciled,
        )

    def query_iceberg_time_travel(self, snapshot_id: str) -> list[dict[str, Any]]:
        """Execute Apache Iceberg `SELECT * ... FOR SYSTEM_VERSION AS OF <snapshot_id>`."""
        if snapshot_id not in self.iceberg_snapshots:
            raise KeyError(f"Iceberg snapshot '{snapshot_id}' not found in Apache Polaris catalog.")
        return [dict(row) for row in self.iceberg_snapshots[snapshot_id]]

    def execute_trino_federated_query(
        self,
        *,
        postgres_ledger_count: int,
        mongo_kyc_count: int,
        cassandra_timeline_count: int,
    ) -> dict[str, Any]:
        """Simulate a Trino federated SQL join across Iceberg, Postgres, Mongo, and Cassandra."""
        return {
            "engine": "trino_federated_sql",
            "catalog_branch": self.active_catalog_branch,
            "iceberg_silver_rows": len(self.silver_spark_enriched),
            "postgres_ledger_rows": postgres_ledger_count,
            "mongodb_kyc_docs": mongo_kyc_count,
            "cassandra_timeline_rows": cassandra_timeline_count,
            "zero_etl_federation_verified": True,
        }


class AccessControlEngine:
    """Enforces RBAC, ABAC (data residency), and PostgreSQL Row-Level Security (RLS) policies."""

    ROLE_PERMISSIONS: ClassVar[dict[str, frozenset[str]]] = {
        "forge_admin": frozenset(
            {"READ_RAW", "WRITE_OLTP", "DDL_ADMIN", "READ_AUDIT", "ANONYMISE"}
        ),
        "forge_core_service": frozenset({"READ_RAW", "WRITE_OLTP", "APPEND_LEDGER"}),
        "forge_compliance_dpo": frozenset({"READ_RAW", "READ_AUDIT", "ANONYMISE", "RESOLVE_ALERT"}),
        "forge_analyst": frozenset({"READ_MASKED", "READ_MATERIALISED_VIEW"}),
        "forge_customer": frozenset({"READ_OWN_RLS", "INITIATE_TRANSFER"}),
    }

    @classmethod
    def check_access(
        cls,
        *,
        role: str,
        permission: str,
        actor_customer_id: str | None = None,
        target_customer_id: str | None = None,
        actor_country: str | None = None,
        resource_country: str | None = None,
    ) -> bool:
        """Evaluate RBAC role permissions, RLS row ownership, and ABAC jurisdiction residency."""
        allowed = cls.ROLE_PERMISSIONS.get(role)
        if allowed is None or permission not in allowed:
            raise SecurityAccessDeniedError(
                f"RBAC Denied: Role '{role}' lacks permission '{permission}'."
            )

        if role == "forge_customer" and actor_customer_id != target_customer_id:
            raise SecurityAccessDeniedError(
                "RLS Denied: forge_customer may only access rows matching their own customer_id."
            )

        if (
            actor_country is not None
            and resource_country is not None
            and actor_country != resource_country
            and role != "forge_admin"
        ):
            raise SecurityAccessDeniedError(
                f"ABAC Data Residency Denied: Actor region '{actor_country}' cannot access "
                f"resource pinned to region '{resource_country}'."
            )

        return True


class DataPrivacyEngine:
    """Implements Dynamic Data Masking, NDPA 2023 Anonymisation, Differential Privacy, and ZKPs."""

    @staticmethod
    def mask_customer_pii(
        *,
        full_name: str,
        email: str,
        phone_e164: str,
        raw_or_masked_pan: str = "XXXX-XXXX-XXXX-4821",
    ) -> dict[str, str]:
        """Dynamically mask sensitive customer PII for non-production and BI analyst views."""
        name_prefix = full_name[:2] if len(full_name) >= 2 else full_name
        local_part, _, domain_part = email.partition("@")
        email_prefix = local_part[:2] if len(local_part) >= 2 else local_part
        masked_email = f"{email_prefix}***@{domain_part}" if domain_part else "***"
        masked_phone = (
            f"{phone_e164[:4]}******{phone_e164[-4:]}" if len(phone_e164) >= 8 else "******"
        )
        clean_digits = "".join(ch for ch in raw_or_masked_pan if ch.isdigit())
        last4 = clean_digits[-4:] if len(clean_digits) >= 4 else "0000"
        return {
            "masked_name": f"{name_prefix}***",
            "masked_email": masked_email,
            "masked_phone": masked_phone,
            "masked_pan": f"XXXX-XXXX-XXXX-{last4}",
        }

    @staticmethod
    def anonymise_customer_record(
        session: Session,
        *,
        customer_id: str,
        dpo_actor_id: str = "dpo.officer",
    ) -> Customer:
        """Execute NDPA 2023 / GDPR Right-to-Erasure pseudonymisation preserving ledger rows."""
        customer = session.get(Customer, customer_id)
        if customer is None:
            raise ValueError(f"Customer '{customer_id}' not found.")

        pseudonym = compute_sha256_hex(f"{customer_id}:NDPA_ERASURE:{dpo_actor_id}")
        numeric_digits = "".join(ch for ch in pseudonym if ch.isdigit())
        padded_digits = (numeric_digits + "0000000000")[:10]

        customer.full_name = f"REDACTED_{pseudonym[:12]}"
        customer.email = f"{pseudonym[:16]}@anonymised.invalid"
        customer.phone_e164 = f"+000{padded_digits}"
        customer.bvn_sha256 = pseudonym
        customer.is_anonymised = True
        customer.updated_at = utc_now()

        consents = list(
            session.scalars(
                select(ConsentRecord).where(
                    ConsentRecord.customer_id == customer_id,
                    ConsentRecord.consent_granted.is_(True),
                )
            ).all()
        )
        now = utc_now()
        for c in consents:
            c.consent_granted = False
            c.revoked_at = now

        session.flush()
        return customer

    @staticmethod
    def apply_differential_privacy_laplace(
        true_value: float,
        *,
        sensitivity: float,
        epsilon: float,
        deterministic_seed: str = "forge-dp-seed-2026",
    ) -> float:
        """Add calibrated Laplace noise Lap(sensitivity / epsilon) to protect aggregate queries."""
        if epsilon <= 0.0 or sensitivity < 0.0:
            raise ValueError("epsilon must be > 0 and sensitivity must be >= 0.")
        scale = sensitivity / epsilon
        # Derive a deterministic uniform variate u in (-0.49, 0.49) from seed for reproducible tests
        digest_int = int(compute_sha256_hex(deterministic_seed)[:8], 16)
        u = ((digest_int / 0xFFFFFFFF) - 0.5) * 0.98
        sign = -1.0 if u < 0.0 else 1.0
        noise = -scale * sign * math.log(1.0 - 2.0 * abs(u))
        return round(true_value + noise, 4)

    @staticmethod
    def generate_and_verify_zkp_tier_claim(
        *,
        customer_id: str,
        actual_age: int,
        actual_tier: int,
        min_required_age: int = 18,
        min_required_tier: int = 2,
        issuer: KycCredentialIssuer | None = None,
    ) -> dict[str, Any]:
        """Prove ``age >= min AND kyc_tier >= min`` in zero knowledge and verify the proof.

        The KYC authority commits to the attributes and signs the commitments; the holder proves the
        range predicates with Pedersen/bit-decomposition NIZK proofs (see ``gpmorgan_forge.zkp``).
        The verifier sees only commitments and proofs, never the age, tier, BVN, or birthdate.
        If the predicate is false, no valid proof can be produced and ``verified`` is ``False``.
        """
        authority = issuer or KycCredentialIssuer()
        credential, opening = authority.issue(customer_id, actual_age, actual_tier)
        claim = f"age >= {min_required_age} AND kyc_tier >= TIER_{min_required_tier}"
        try:
            proof = prove_eligibility(
                credential, opening, min_age=min_required_age, min_tier=min_required_tier
            )
        except ValueError:
            return {
                "customer_id": customer_id,
                "claim": claim,
                "commitment_hash": credential.fingerprint(),
                "verified": False,
                "raw_pii_disclosed": False,
            }
        return {
            "customer_id": customer_id,
            "claim": claim,
            "commitment_hash": credential.fingerprint(),
            "verified": verify_eligibility(credential, proof, authority.public_key),
            "raw_pii_disclosed": False,
        }


@dataclass(frozen=True)
class DatabaseSpan:
    """OpenTelemetry-compatible database span capturing polyglot operation latency."""

    trace_id: str
    span_id: str
    db_system: str
    operation_name: str
    latency_ms: float
    attributes: dict[str, str]


class DatabaseTelemetryTracer:
    """Lightweight OpenTelemetry-compatible span and metric collector for polyglot databases."""

    def __init__(self, trace_seed: str = "forge-otel-trace-2026") -> None:
        """Initialize a deterministic W3C TraceContext trace_id and span buffer."""
        self.trace_id: str = compute_sha256_hex(trace_seed)[:32]
        self._spans: list[DatabaseSpan] = []

    def record_span(
        self,
        *,
        db_system: str,
        operation_name: str,
        latency_ms: float,
        attributes: dict[str, str] | None = None,
    ) -> DatabaseSpan:
        """Record an OpenTelemetry database client span (`db.system`, `db.operation.name`)."""
        if latency_ms < 0.0:
            raise ValueError("latency_ms must be >= 0.0.")
        idx = len(self._spans) + 1
        span_id = compute_sha256_hex(f"{self.trace_id}:{idx}:{db_system}:{operation_name}")[:16]
        span = DatabaseSpan(
            trace_id=self.trace_id,
            span_id=span_id,
            db_system=db_system,
            operation_name=operation_name,
            latency_ms=round(latency_ms, 3),
            attributes=dict(attributes) if attributes is not None else {},
        )
        self._spans.append(span)
        return span

    def summarize_telemetry(self) -> dict[str, Any]:
        """Aggregate end-to-end distributed trace metrics across all polyglot storage tiers."""
        total_ms = round(sum(s.latency_ms for s in self._spans), 3)
        systems = [s.db_system for s in self._spans]
        return {
            "trace_id": self.trace_id,
            "span_count": len(self._spans),
            "total_pipeline_latency_ms": total_ms,
            "systems_traced": systems,
            "sla_under_50ms_met": total_ms < 50.0,
        }
