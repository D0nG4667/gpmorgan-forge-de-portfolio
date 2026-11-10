"""Polyglot NoSQL engines for GPMorgan Forge (Portfolio 4: Document, KV, Graph, Wide-Column & TS).

Implements:
1. DocumentStoreEngine (MongoDB 8.0 LTS model): $jsonSchema validation and AEAD field-level
   encryption for polymorphic KYC verification and device telemetry documents.
2. KeyValueStoreEngine (Redis 8.0 / Valkey model): Two-tier atomic idempotency key locking
   (SET NX EX fast-path + PostgreSQL uq_transactions_idempotency_key durable fallback),
   read-through account balance caching with TTL, and sliding-window API rate limiting (ZSET).
3. GraphFraudEngine (Neo4j 5.26 LTS Cypher model): Property graph nodes/edges, bounded DFS cycle
   detection for circular money-laundering rings (*2..5 hops), shared-device mule ring clustering,
   collaborative-filtering product recommendations, and Cypher query generation.
4. WideColumnTimelineEngine (Apache Cassandra 5.0 CQL model): Strictly read-only, append-only
   customer transaction timeline projection keyed by ((account_id, bucket_month), created_at DESC,
   transaction_id ASC) fed asynchronously via CDC, never used for mutable balance state.
5. TimeSeriesTelemetryEngine (TimescaleDB model): PostgreSQL hypertable ingestion and continuous
   aggregate time_bucket OHLCV rollups for high-frequency FX liquidity tick streams.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from gpmorgan_forge.operations import quantize_money

AES_256_KEY_BYTES: Final[int] = 32
AES_GCM_NONCE_BYTES: Final[int] = 12
MIN_KMS_MASTER_KEY_BYTES: Final[int] = 32
ENC_TOKEN_PARTS: Final[int] = 3
ENCRYPTION_ALGORITHM: Final[str] = "AEAD_AES_256_GCM"


class DocumentValidationError(ValueError):
    """Raised when a NoSQL document fails $jsonSchema or field-level encryption rules."""


@dataclass(frozen=True)
class EncryptedKYCDocument:
    """Validated MongoDB KYC document with AEAD field-level encrypted PII attributes."""

    customer_id: str
    external_ref: str
    jurisdiction: str
    kyc_tier: str
    verification_status: str
    encrypted_pii_fields: dict[str, str]
    document_artifacts: list[dict[str, Any]]


class DocumentStoreEngine:
    """Simulates MongoDB $jsonSchema validation, field-level encryption, and aggregations."""

    ALLOWED_JURISDICTIONS: frozenset[str] = frozenset({"NG", "GB", "US", "EU"})
    ALLOWED_TIERS: frozenset[str] = frozenset({"TIER_1", "TIER_2", "TIER_3"})
    ALLOWED_STATUSES: frozenset[str] = frozenset({"PENDING", "VERIFIED", "REJECTED", "EXPIRED"})

    def __init__(self, kms_master_key: bytes | None = None) -> None:
        """Initialize the document store with a KMS master key for field-level encryption.

        The master key (>= 32 bytes) is taken from ``kms_master_key``, else from the base64
        ``FORGE_KMS_MASTER_KEY`` environment variable (injected by the KMS/secret manager), else a
        random ephemeral key is generated. No key material is ever hard-coded.
        """
        if kms_master_key is None:
            env_key = os.environ.get("FORGE_KMS_MASTER_KEY")
            kms_master_key = base64.b64decode(env_key) if env_key else os.urandom(32)
        if len(kms_master_key) < MIN_KMS_MASTER_KEY_BYTES:
            raise DocumentValidationError("KMS master key must be at least 32 bytes.")
        self._kms_master_key = kms_master_key
        self._kyc_collection: dict[str, EncryptedKYCDocument] = {}
        self._telemetry_collection: list[dict[str, Any]] = []

    def _derive_data_key(self, key_id: str) -> bytes:
        """Derive a per-key-id 256-bit AES data key from the KMS master key (HKDF-SHA256)."""
        return HKDF(
            algorithm=hashes.SHA256(),
            length=AES_256_KEY_BYTES,
            salt=None,
            info=f"forge-field-encryption:{key_id}".encode(),
        ).derive(self._kms_master_key)

    def encrypt_sensitive_field(
        self, plaintext: str, key_id: str = "kms-ng-cbn-key-2026-01", aad: bytes = b""
    ) -> str:
        """Encrypt a PII field with AES-256-GCM (random 96-bit nonce, authenticated AAD).

        Returns ``ENCv2:<key_id>:<base64url(nonce || ciphertext || tag)>``. The key id is always
        authenticated as additional data, together with any caller-supplied ``aad`` (for example
        the owning customer id, which prevents ciphertext from being swapped between records).
        """
        if not plaintext.strip():
            raise DocumentValidationError("Plaintext PII field cannot be empty.")
        nonce = os.urandom(AES_GCM_NONCE_BYTES)
        sealed = AESGCM(self._derive_data_key(key_id)).encrypt(
            nonce, plaintext.encode("utf-8"), key_id.encode("utf-8") + b"|" + aad
        )
        return f"ENCv2:{key_id}:{base64.urlsafe_b64encode(nonce + sealed).decode('ascii')}"

    def decrypt_sensitive_field(self, token: str, aad: bytes = b"") -> str:
        """Decrypt and authenticate an ``ENCv2`` token; any tampering raises a validation error."""
        parts = token.split(":", 2)
        if len(parts) != ENC_TOKEN_PARTS or parts[0] != "ENCv2":
            raise DocumentValidationError("Unsupported or malformed ciphertext envelope.")
        key_id = parts[1]
        try:
            blob = base64.urlsafe_b64decode(parts[2])
            plaintext = AESGCM(self._derive_data_key(key_id)).decrypt(
                blob[:AES_GCM_NONCE_BYTES],
                blob[AES_GCM_NONCE_BYTES:],
                key_id.encode("utf-8") + b"|" + aad,
            )
        except (InvalidTag, ValueError) as exc:
            raise DocumentValidationError("Ciphertext failed authentication.") from exc
        return plaintext.decode("utf-8")

    def insert_kyc_document(
        self,
        *,
        customer_id: str,
        external_ref: str,
        jurisdiction: str,
        kyc_tier: str,
        verification_status: str,
        raw_national_id: str,
        document_artifacts: list[dict[str, Any]],
        key_id: str = "kms-ng-cbn-key-2026-01",
    ) -> EncryptedKYCDocument:
        """Validate against $jsonSchema rules, encrypt PII fields, and store KYC document."""
        if jurisdiction not in self.ALLOWED_JURISDICTIONS:
            raise DocumentValidationError(f"Unsupported jurisdiction: {jurisdiction}")
        if kyc_tier not in self.ALLOWED_TIERS:
            raise DocumentValidationError(f"Invalid kyc_tier: {kyc_tier}")
        if verification_status not in self.ALLOWED_STATUSES:
            raise DocumentValidationError(f"Invalid verification_status: {verification_status}")
        if not document_artifacts:
            raise DocumentValidationError("At least one document artifact is required.")

        for artifact in document_artifacts:
            conf = float(artifact.get("liveness_confidence", -1.0))
            if not (0.0 <= conf <= 1.0):
                raise DocumentValidationError("liveness_confidence must be between 0.0 and 1.0.")

        cipher = self.encrypt_sensitive_field(
            raw_national_id, key_id=key_id, aad=customer_id.encode("utf-8")
        )
        doc = EncryptedKYCDocument(
            customer_id=customer_id,
            external_ref=external_ref,
            jurisdiction=jurisdiction,
            kyc_tier=kyc_tier,
            verification_status=verification_status,
            encrypted_pii_fields={
                "encryption_algorithm": ENCRYPTION_ALGORITHM,
                "key_id": key_id,
                "bvn_or_national_id_cipher": cipher,
            },
            document_artifacts=document_artifacts,
        )
        self._kyc_collection[customer_id] = doc
        return doc

    def get_kyc_document(self, customer_id: str) -> EncryptedKYCDocument | None:
        """Retrieve a validated KYC document by customer_id."""
        return self._kyc_collection.get(customer_id)

    def reveal_national_id(self, customer_id: str) -> str:
        """Decrypt a customer's national ID (authorised callers only; AAD binds it to the owner)."""
        doc = self._kyc_collection.get(customer_id)
        if doc is None:
            raise DocumentValidationError(f"No KYC document for customer {customer_id}.")
        return self.decrypt_sensitive_field(
            doc.encrypted_pii_fields["bvn_or_national_id_cipher"], aad=customer_id.encode("utf-8")
        )

    def record_device_telemetry(
        self,
        *,
        event_id: str,
        customer_id: str,
        account_id: str,
        device_fingerprint: str,
        ip_address: str,
        is_emulator: bool,
        is_vpn_or_tor: bool,
    ) -> dict[str, Any]:
        """Insert a device telemetry event document."""
        event = {
            "event_id": event_id,
            "customer_id": customer_id,
            "account_id": account_id,
            "device_fingerprint": device_fingerprint,
            "ip_address": ip_address,
            "risk_signals": {
                "is_emulator": is_emulator,
                "is_vpn_or_tor": is_vpn_or_tor,
                "root_or_jailbreak_detected": False,
            },
        }
        self._telemetry_collection.append(event)
        return event

    def aggregate_shared_devices(self) -> list[dict[str, Any]]:
        """Execute MongoDB-style aggregation pipeline grouping telemetry by device_fingerprint."""
        grouped: dict[str, dict[str, Any]] = {}
        for ev in self._telemetry_collection:
            fp = str(ev["device_fingerprint"])
            entry = grouped.setdefault(
                fp,
                {
                    "device_fingerprint": fp,
                    "distinct_customers": set(),
                    "distinct_accounts": set(),
                    "event_count": 0,
                    "vpn_hits": 0,
                },
            )
            entry["distinct_customers"].add(ev["customer_id"])
            entry["distinct_accounts"].add(ev["account_id"])
            entry["event_count"] += 1
            if ev["risk_signals"]["is_vpn_or_tor"]:
                entry["vpn_hits"] += 1

        results: list[dict[str, Any]] = []
        for fp, data in grouped.items():
            customers = sorted(data["distinct_customers"])
            if len(customers) > 1:
                results.append(
                    {
                        "device_fingerprint": fp,
                        "customer_count": len(customers),
                        "distinct_customers": customers,
                        "distinct_accounts": sorted(data["distinct_accounts"]),
                        "event_count": data["event_count"],
                        "vpn_hits": data["vpn_hits"],
                    }
                )
        results.sort(key=lambda r: (r["customer_count"], r["event_count"]), reverse=True)
        return results


class KeyValueStoreEngine:
    """Simulates Redis 8.0 atomic idempotency locks, read-through cache, and ZSET rate limiting."""

    def __init__(self) -> None:
        """Initialize in-memory Redis data structures."""
        self._idempotency_keys: dict[str, tuple[str, int]] = {}
        self._account_cache: dict[str, dict[str, str]] = {}
        self._rate_limit_zsets: dict[str, list[tuple[int, str]]] = {}

    def acquire_idempotency_lock(
        self, idempotency_key: str, payload_json: str, ttl_seconds: int = 86400
    ) -> tuple[bool, str]:
        """Execute Redis `SET forge:idemp:<key> <payload> NX EX <ttl>`."""
        namespaced = f"forge:idemp:{idempotency_key}"
        if namespaced in self._idempotency_keys:
            existing_payload, _ = self._idempotency_keys[namespaced]
            return False, existing_payload
        self._idempotency_keys[namespaced] = (payload_json, ttl_seconds)
        return True, payload_json

    def verify_two_tier_idempotency(
        self,
        idempotency_key: str,
        payload_json: str,
        *,
        durable_ledger_keys: set[str],
        ttl_seconds: int = 86400,
    ) -> tuple[bool, str, str]:
        """Enforce two-tier idempotency across Redis 8.0 / Valkey and PostgreSQL/CockroachDB.

        Tier 1 checks the sub-millisecond Redis/Valkey `SET ... NX EX` fast-path cache.
        Tier 2 checks the durable `uq_transactions_idempotency_key` constraint set in the
        relational ledger so cache TTL eviction or failover never permits duplicate settlement.
        """
        acquired_cache, cached_payload = self.acquire_idempotency_lock(
            idempotency_key, payload_json, ttl_seconds=ttl_seconds
        )
        if not acquired_cache:
            return False, cached_payload, "TIER_1_REDIS_VALKEY_FAST_PATH"
        if idempotency_key in durable_ledger_keys:
            return False, payload_json, "TIER_2_POSTGRES_UNIQUE_CONSTRAINT"
        durable_ledger_keys.add(idempotency_key)
        return True, payload_json, "NEW_ATOMIC_SETTLEMENT"

    def cache_account_summary(
        self,
        account_number: str,
        *,
        balance: Decimal,
        available_balance: Decimal,
        version: int,
        status: str,
        currency: str = "NGN",
    ) -> str:
        """Execute Redis `HSET forge:account:<account_number>:summary`."""
        key = f"forge:account:{account_number}:summary"
        self._account_cache[key] = {
            "account_number": account_number,
            "currency": currency,
            "balance": str(quantize_money(balance)),
            "available_balance": str(quantize_money(available_balance)),
            "version": str(version),
            "status": status,
        }
        return key

    def get_cached_account_summary(self, account_number: str) -> dict[str, str] | None:
        """Retrieve cached account summary hash or None on cache miss."""
        return self._account_cache.get(f"forge:account:{account_number}:summary")

    def invalidate_account_cache(self, account_number: str) -> bool:
        """Evict an account summary key upon balance mutation."""
        key = f"forge:account:{account_number}:summary"
        return self._account_cache.pop(key, None) is not None

    def check_sliding_window_rate_limit(
        self,
        customer_ref: str,
        action: str,
        *,
        now_ms: int,
        window_ms: int = 60000,
        max_requests: int = 10,
        request_id: str,
    ) -> tuple[bool, int]:
        """Enforce sliding-window rate limiting using Redis ZSET semantics."""
        key = f"forge:ratelimit:{customer_ref}:{action}"
        cutoff = now_ms - window_ms
        entries = [item for item in self._rate_limit_zsets.get(key, []) if item[0] > cutoff]
        if len(entries) >= max_requests:
            self._rate_limit_zsets[key] = entries
            return False, len(entries)

        entries.append((now_ms, request_id))
        self._rate_limit_zsets[key] = entries
        return True, len(entries)


@dataclass(frozen=True)
class CircularTransferRing:
    """Detected circular money-laundering transfer cycle in the property graph."""

    entry_account: str
    hop_count: int
    circular_path: list[str]
    total_laundered_volume: Decimal


@dataclass(frozen=True)
class SharedDeviceMuleCluster:
    """Cluster of distinct customer accounts controlled via the same device fingerprint."""

    device_fingerprint: str
    is_emulator: bool
    linked_customers: list[str]
    linked_accounts: list[str]


@dataclass
class GraphFraudEngine:
    """Neo4j 5 property graph model for circular AML loop detection and recommendations."""

    customers: dict[str, dict[str, Any]] = field(default_factory=dict)
    accounts: dict[str, dict[str, Any]] = field(default_factory=dict)
    devices: dict[str, dict[str, Any]] = field(default_factory=dict)
    products: dict[str, dict[str, str]] = field(default_factory=dict)
    customer_accounts: dict[str, list[str]] = field(default_factory=dict)
    customer_devices: dict[str, list[str]] = field(default_factory=dict)
    transfer_edges: list[dict[str, Any]] = field(default_factory=list)
    partner_edges: dict[str, set[str]] = field(default_factory=dict)
    subscriptions: dict[str, set[str]] = field(default_factory=dict)

    def add_customer_account(
        self,
        *,
        customer_id: str,
        customer_name: str,
        segment: str,
        account_number: str,
        risk_score: float,
        currency: str = "NGN",
    ) -> None:
        """Create `:Customer` and `:Account` nodes connected by `:OWNS_ACCOUNT`."""
        self.customers[customer_id] = {
            "customer_id": customer_id,
            "name": customer_name,
            "segment": segment,
        }
        self.accounts[account_number] = {
            "account_number": account_number,
            "currency": currency,
            "risk_score": risk_score,
            "owner_customer_id": customer_id,
        }
        self.customer_accounts.setdefault(customer_id, []).append(account_number)

    def link_customer_device(
        self, customer_id: str, device_fingerprint: str, *, is_emulator: bool = False
    ) -> None:
        """Create `(:Customer)-[:USED_DEVICE]->(:Device)` relationship."""
        self.devices[device_fingerprint] = {
            "device_fingerprint": device_fingerprint,
            "is_emulator": is_emulator,
        }
        dev_list = self.customer_devices.setdefault(customer_id, [])
        if device_fingerprint not in dev_list:
            dev_list.append(device_fingerprint)

    def record_transfer_edge(
        self,
        *,
        source_account: str,
        destination_account: str,
        tx_ref: str,
        amount: Decimal | str | int,
    ) -> None:
        """Create `(:Account)-[:TRANSFERRED_TO {tx_ref, amount}]->(:Account)` relationship."""
        self.transfer_edges.append(
            {
                "source": source_account,
                "destination": destination_account,
                "tx_ref": tx_ref,
                "amount": quantize_money(amount),
            }
        )

    def add_product_subscription(
        self,
        *,
        customer_id: str,
        product_id: str,
        product_name: str,
        category: str,
    ) -> None:
        """Create `(:Customer)-[:SUBSCRIBED_TO]->(:FinancialProduct)` relationship."""
        self.products[product_id] = {
            "product_id": product_id,
            "name": product_name,
            "category": category,
        }
        self.subscriptions.setdefault(customer_id, set()).add(product_id)

    def add_business_partnership(self, customer_a: str, customer_b: str) -> None:
        """Create bidirectional `(:Customer)-[:PARTNERS_WITH]-(:Customer)` relationship."""
        self.partner_edges.setdefault(customer_a, set()).add(customer_b)
        self.partner_edges.setdefault(customer_b, set()).add(customer_a)

    def detect_circular_transfer_loops(
        self,
        *,
        min_hops: int = 2,
        max_hops: int = 5,
        min_risk_score: float = 50.0,
    ) -> list[CircularTransferRing]:
        """Execute bounded depth traversal `(:Account)-[:TRANSFERRED_TO*min..max]->(:Account)`.

        Finds canonical elementary cycles starting at accounts whose risk_score >= min_risk_score.
        """
        adj: dict[str, list[tuple[str, Decimal]]] = {}
        for edge in self.transfer_edges:
            adj.setdefault(edge["source"], []).append((edge["destination"], edge["amount"]))

        rings: list[CircularTransferRing] = []
        seen_canonical_cycles: set[tuple[str, ...]] = set()

        for start_acct, acct_meta in sorted(self.accounts.items()):
            if float(acct_meta["risk_score"]) < min_risk_score:
                continue

            def _dfs(
                current: str,
                origin: str,
                path: list[str],
                volume: Decimal,
            ) -> None:
                hops = len(path) - 1
                if hops > max_hops:
                    return
                for nxt, edge_amt in adj.get(current, []):
                    if nxt == origin and hops + 1 >= min_hops:
                        cycle_nodes = path[:-1] if path[-1] == origin else path
                        min_node = min(cycle_nodes)
                        min_idx = cycle_nodes.index(min_node)
                        canonical = tuple(cycle_nodes[min_idx:] + cycle_nodes[:min_idx])
                        if canonical not in seen_canonical_cycles:
                            seen_canonical_cycles.add(canonical)
                            rings.append(
                                CircularTransferRing(
                                    entry_account=origin,
                                    hop_count=hops + 1,
                                    circular_path=[*path, origin],
                                    total_laundered_volume=volume + edge_amt,
                                )
                            )
                    elif nxt not in path and hops + 1 < max_hops:
                        _dfs(nxt, origin, [*path, nxt], volume + edge_amt)

            _dfs(start_acct, start_acct, [start_acct], Decimal("0.0000"))

        rings.sort(key=lambda r: r.total_laundered_volume, reverse=True)
        return rings

    def detect_shared_device_mule_clusters(self) -> list[SharedDeviceMuleCluster]:
        """Identify devices shared across multiple distinct customers and their accounts."""
        device_to_customers: dict[str, set[str]] = {}
        for cust_id, dev_list in self.customer_devices.items():
            for dev_fp in dev_list:
                device_to_customers.setdefault(dev_fp, set()).add(cust_id)

        clusters: list[SharedDeviceMuleCluster] = []
        for dev_fp, cust_set in sorted(device_to_customers.items()):
            if len(cust_set) > 1:
                sorted_custs = sorted(cust_set)
                linked_accts: list[str] = []
                for cid in sorted_custs:
                    linked_accts.extend(self.customer_accounts.get(cid, []))
                clusters.append(
                    SharedDeviceMuleCluster(
                        device_fingerprint=dev_fp,
                        is_emulator=bool(self.devices[dev_fp]["is_emulator"]),
                        linked_customers=sorted_custs,
                        linked_accounts=sorted(set(linked_accts)),
                    )
                )
        return clusters

    def recommend_financial_products(self, target_customer_id: str) -> list[dict[str, str]]:
        """Recommend financial products adopted by business partners of target_customer_id."""
        target_subs = self.subscriptions.get(target_customer_id, set())
        peers = self.partner_edges.get(target_customer_id, set())
        recommended_ids: set[str] = set()

        for peer_id in peers:
            for prod_id in self.subscriptions.get(peer_id, set()):
                if prod_id not in target_subs:
                    recommended_ids.add(prod_id)

        return [self.products[pid] for pid in sorted(recommended_ids)]

    @staticmethod
    def build_optimised_cypher_aml_query(min_hops: int = 2, max_hops: int = 5) -> str:
        """Return the optimised Neo4j 5 Cypher query for circular transfer ring detection."""
        if min_hops < 2 or max_hops < min_hops or max_hops > 6:
            raise ValueError("Traversal depth must satisfy 2 <= min_hops <= max_hops <= 6.")
        return (
            f"MATCH path = (origin:Account)-[:TRANSFERRED_TO*{min_hops}..{max_hops}]->(origin) "
            "WHERE origin.risk_score >= 50.0 "
            "AND all(n IN tail(nodes(path)) WHERE origin.account_number <= n.account_number) "
            "RETURN origin.account_number AS entry_account, "
            "length(path) AS hop_count, "
            "[n IN nodes(path) | n.account_number] AS circular_path"
        )


@dataclass(frozen=True)
class TimelineProjectionRow:
    """Immutable row in the Apache Cassandra 5.0 customer transaction timeline projection."""

    account_id: str
    bucket_month: str
    created_at: str
    transaction_id: str
    reference_code: str
    direction: str
    amount: Decimal
    currency: str
    balance_after: Decimal
    narration: str


class WideColumnTimelineEngine:
    """Simulates Apache Cassandra 5.0 append-only customer transaction timeline projections.

    Strictly restricted to read-only, append-only CDC timeline views partitioned by
    `(account_id, bucket_month)` with clustering order `(created_at DESC, transaction_id ASC)`.
    Never used for mutable balance state or double-entry locking.
    """

    def __init__(self) -> None:
        """Initialize wide-column partition storage keyed by (account_id, bucket_month)."""
        self._partitions: dict[tuple[str, str], list[TimelineProjectionRow]] = {}

    def append_cdc_timeline_event(
        self,
        *,
        account_id: str,
        bucket_month: str,
        created_at: str,
        transaction_id: str,
        reference_code: str,
        direction: str,
        amount: Decimal | str | int,
        currency: str,
        balance_after: Decimal | str | int,
        narration: str,
    ) -> TimelineProjectionRow:
        """Idempotently append a CDC ledger event into `(account_id, bucket_month)`."""
        if direction not in {"DEBIT", "CREDIT"}:
            raise ValueError("direction must be 'DEBIT' or 'CREDIT'.")
        partition_key = (account_id, bucket_month)
        existing_rows = self._partitions.setdefault(partition_key, [])
        for row in existing_rows:
            if row.created_at == created_at and row.transaction_id == transaction_id:
                return row

        projection = TimelineProjectionRow(
            account_id=account_id,
            bucket_month=bucket_month,
            created_at=created_at,
            transaction_id=transaction_id,
            reference_code=reference_code,
            direction=direction,
            amount=quantize_money(amount),
            currency=currency,
            balance_after=quantize_money(balance_after),
            narration=narration,
        )
        existing_rows.append(projection)
        existing_rows.sort(key=lambda r: r.transaction_id)
        existing_rows.sort(key=lambda r: r.created_at, reverse=True)
        return projection

    def query_account_timeline(
        self, account_id: str, bucket_month: str, *, limit: int = 50
    ) -> list[TimelineProjectionRow]:
        """Execute a single-partition `LOCAL_QUORUM` read ordered by `created_at DESC`."""
        rows = self._partitions.get((account_id, bucket_month), [])
        return list(rows[:limit])

    @staticmethod
    def forbid_mutable_balance_update() -> None:
        """Enforce the architectural invariant prohibiting mutable balance state in Cassandra."""
        raise PermissionError(
            "Apache Cassandra 5.0 is restricted strictly to append-only CDC timeline projections; "
            "mutable balance updates must execute in PostgreSQL 18 or CockroachDB."
        )


@dataclass(frozen=True)
class TimeBucketCandlestick:
    """Pre-aggregated hourly FX liquidity candlestick from a TimescaleDB continuous aggregate."""

    bucket_hour: str
    corridor_pair: str
    open_rate: Decimal
    high_rate: Decimal
    low_rate: Decimal
    close_rate: Decimal
    total_volume: Decimal
    tick_count: int


class TimeSeriesTelemetryEngine:
    """Simulates TimescaleDB hypertables and `time_bucket` continuous aggregates for FX ticks."""

    def __init__(self) -> None:
        """Initialize hypertable time-chunk storage."""
        self._hypertable_ticks: list[dict[str, Any]] = []

    def ingest_fx_tick(
        self,
        *,
        tick_time: datetime,
        corridor_pair: str,
        mid_rate: Decimal | str | int,
        settlement_volume: Decimal | str | int,
    ) -> dict[str, Any]:
        """Insert a high-frequency FX liquidity tick into the range-partitioned hypertable."""
        rate_dec = quantize_money(mid_rate)
        vol_dec = quantize_money(settlement_volume)
        if rate_dec <= Decimal("0.0000") or vol_dec < Decimal("0.0000"):
            raise ValueError("mid_rate must be > 0 and settlement_volume must be >= 0.")
        tick = {
            "tick_time": tick_time,
            "bucket_hour": tick_time.strftime("%Y-%m-%dT%H:00:00Z"),
            "corridor_pair": corridor_pair,
            "mid_rate": rate_dec,
            "settlement_volume": vol_dec,
        }
        self._hypertable_ticks.append(tick)
        return tick

    def compute_hourly_continuous_aggregate(
        self, corridor_pair: str
    ) -> list[TimeBucketCandlestick]:
        """Execute TimescaleDB `WITH (timescaledb.continuous)` hourly `time_bucket` rollup."""
        filtered = [t for t in self._hypertable_ticks if t["corridor_pair"] == corridor_pair]
        filtered.sort(key=lambda t: t["tick_time"])

        buckets: dict[str, list[dict[str, Any]]] = {}
        for tick in filtered:
            buckets.setdefault(str(tick["bucket_hour"]), []).append(tick)

        candlesticks: list[TimeBucketCandlestick] = []
        for bucket_hour, ticks in sorted(buckets.items(), reverse=True):
            rates = [t["mid_rate"] for t in ticks]
            volumes = [t["settlement_volume"] for t in ticks]
            candlesticks.append(
                TimeBucketCandlestick(
                    bucket_hour=bucket_hour,
                    corridor_pair=corridor_pair,
                    open_rate=rates[0],
                    high_rate=max(rates),
                    low_rate=min(rates),
                    close_rate=rates[-1],
                    total_volume=sum(volumes, Decimal("0.0000")),
                    tick_count=len(ticks),
                )
            )
        return candlesticks
