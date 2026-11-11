"""Spark 4.0 Structured Streaming job: Kafka CDC -> Bronze (raw) -> Silver (validated, merged).

Design patterns:
  * Self-bootstrapping catalog & DDL: auto-provisions the Polaris warehouse, namespaces, and
    Bronze/DLQ/Silver Iceberg v2 tables on startup before consuming Kafka offsets.
  * Medallion layering: Bronze keeps every raw event immutably; Silver holds only valid rows.
  * Data contract + dead-letter queue: events violating the contract go to ``dlq_transactions``.
  * Idempotent upsert: ``MERGE INTO`` on ``reference_code`` makes replays/at-least-once safe.
  * Checkpointing: offsets are stored in ``CHECKPOINT_DIR`` for restartable, exactly-once effects.
"""

from __future__ import annotations

import contextlib
import functools
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

EVENT_SCHEMA = StructType(
    [
        StructField("reference_code", StringType()),
        StructField("currency", StringType()),
        StructField("amount", DecimalType(28, 4)),
        StructField("status", StringType()),
        StructField("is_suspicious", StringType()),
    ]
)


@dataclass(frozen=True, slots=True)
class LakehouseStreamConfig:
    """Immutable 12-Factor runtime configuration resolved at call time from environment vars."""

    catalog_uri: str
    oauth_credential: str
    catalog: str = "forge"
    warehouse: str = "forge_lakehouse"
    warehouse_location: str = "file:///tmp/forge_lakehouse/"
    storage_type: str = "FILE"
    management_uri: str = ""
    lakehouse_namespace: str = "forge_lakehouse"
    dwh_namespace: str = "forge_dwh"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "forge.cdc.transactions"
    checkpoint_dir: str = "/tmp/forge-checkpoints/transactions"
    high_value_threshold: str = "5000000.0000"

    def __post_init__(self) -> None:
        for field_name, identifier in (
            ("catalog", self.catalog),
            ("lakehouse_namespace", self.lakehouse_namespace),
            ("dwh_namespace", self.dwh_namespace),
        ):
            if not _SQL_IDENTIFIER_RE.match(identifier):
                raise ValueError(f"Invalid SQL identifier for {field_name}: {identifier!r}")
        try:
            threshold = Decimal(self.high_value_threshold)
        except InvalidOperation as exc:
            raise ValueError(
                f"Invalid decimal HIGH_VALUE_THRESHOLD: {self.high_value_threshold!r}"
            ) from exc
        if not threshold.is_finite() or threshold <= 0:
            raise ValueError("HIGH_VALUE_THRESHOLD must be a finite positive decimal.")

    @property
    def resolved_management_uri(self) -> str:
        """Return the explicit or derived Apache Polaris Management v1 API base URL."""
        if self.management_uri:
            return self.management_uri.rstrip("/")
        clean_uri = self.catalog_uri.rstrip("/")
        if clean_uri.endswith("/api/catalog"):
            return f"{clean_uri.removesuffix('/api/catalog')}/api/management/v1"
        return f"{clean_uri}/management/v1"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LakehouseStreamConfig:
        """Resolve all streaming, catalog, and contract settings from ``env`` or ``os.environ``."""
        source = os.environ if env is None else env
        return cls(
            catalog_uri=source["POLARIS_CATALOG_URI"].rstrip("/"),
            oauth_credential=source["POLARIS_OAUTH_CREDENTIAL"],
            catalog=source.get("SPARK_CATALOG", "forge"),
            warehouse=source.get("POLARIS_WAREHOUSE", "forge_lakehouse"),
            warehouse_location=source.get(
                "POLARIS_WAREHOUSE_LOCATION", "file:///tmp/forge_lakehouse/"
            ),
            storage_type=source.get("POLARIS_STORAGE_TYPE", "FILE"),
            management_uri=source.get("POLARIS_MANAGEMENT_URI", ""),
            lakehouse_namespace=source.get("LAKEHOUSE_NAMESPACE", "forge_lakehouse"),
            dwh_namespace=source.get("DWH_NAMESPACE", "forge_dwh"),
            kafka_bootstrap_servers=source.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=source.get("CDC_TOPIC", "forge.cdc.transactions"),
            checkpoint_dir=source.get("CHECKPOINT_DIR", "/tmp/forge-checkpoints/transactions"),
            high_value_threshold=source.get("HIGH_VALUE_THRESHOLD", "5000000.0000"),
        )


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    payload: bytes | None = None,
    allowed_statuses: frozenset[int] = frozenset({200, 201, 204}),
) -> tuple[int, dict[str, Any]]:
    """Execute an HTTP request against the REST catalog and return status + parsed JSON."""
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw_body = response.read().decode("utf-8")
            body: dict[str, Any] = json.loads(raw_body) if raw_body.strip() else {}
            return response.status, body
    except urllib.error.HTTPError as exc:
        if exc.code in allowed_statuses:
            raw_err = exc.read().decode("utf-8", errors="replace")
            try:
                parsed_err: dict[str, Any] = json.loads(raw_err) if raw_err.strip() else {}
            except json.JSONDecodeError:
                parsed_err = {}
            return exc.code, parsed_err
        raise


def ensure_polaris_catalog(config: LakehouseStreamConfig | None = None) -> None:
    """Idempotently provision the Polaris catalog warehouse and RBAC grants on startup."""
    cfg = config or LakehouseStreamConfig.from_env()
    if ":" not in cfg.oauth_credential:
        return
    client_id, client_secret = cfg.oauth_credential.split(":", 1)

    if cfg.warehouse_location.startswith("file://"):
        warehouse_path = Path(cfg.warehouse_location.removeprefix("file://"))
        warehouse_path.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            warehouse_path.chmod(0o777)

    management_uri = cfg.resolved_management_uri
    token_payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    ).encode("utf-8")

    try:
        _, token_body = _request_json(
            "POST",
            f"{cfg.catalog_uri}/v1/oauth/tokens",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            payload=token_payload,
        )
        access_token = str(token_body.get("access_token", ""))
        if not access_token:
            return

        auth_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        status, _ = _request_json(
            "GET",
            f"{management_uri}/catalogs/{cfg.warehouse}",
            headers=auth_headers,
            allowed_statuses=frozenset({200, 404}),
        )
        if status == 404:
            create_status, _ = _request_json(
                "POST",
                f"{management_uri}/catalogs",
                headers=auth_headers,
                payload=json.dumps(
                    {
                        "catalog": {
                            "name": cfg.warehouse,
                            "type": "INTERNAL",
                            "properties": {"default-base-location": cfg.warehouse_location},
                            "storageConfigInfo": {
                                "storageType": cfg.storage_type,
                                "allowedLocations": [cfg.warehouse_location],
                            },
                        }
                    }
                ).encode("utf-8"),
                allowed_statuses=frozenset({200, 201, 404, 409}),
            )
            if create_status == 404:
                # Non-Polaris generic Iceberg REST catalog without /api/management/v1.
                return

        _request_json(
            "PUT",
            f"{management_uri}/catalogs/{cfg.warehouse}/catalog-roles/catalog_admin/grants",
            headers=auth_headers,
            payload=json.dumps(
                {
                    "grant": {
                        "type": "catalog",
                        "privilege": "CATALOG_MANAGE_CONTENT",
                        "catalogName": cfg.warehouse,
                    }
                }
            ).encode("utf-8"),
            allowed_statuses=frozenset({200, 201, 204, 409}),
        )
        _request_json(
            "PUT",
            f"{management_uri}/principal-roles/service_admin/catalog-roles/{cfg.warehouse}",
            headers=auth_headers,
            payload=json.dumps({"catalogRole": {"name": "catalog_admin"}}).encode("utf-8"),
            allowed_statuses=frozenset({200, 201, 204, 409}),
        )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        logger.error("Polaris catalog auto-bootstrap failed (HTTP %d): %s", exc.code, detail)
        raise
    except urllib.error.URLError as exc:
        logger.warning("Skipping Polaris management catalog auto-bootstrap: %s", exc)


def build_session(config: LakehouseStreamConfig | None = None) -> SparkSession:
    """Create a Spark session wired to the Apache Polaris Iceberg REST catalog."""
    cfg = config or LakehouseStreamConfig.from_env()
    return (
        SparkSession.builder.appName("forge-bronze-to-silver")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config(f"spark.sql.catalog.{cfg.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cfg.catalog}.type", "rest")
        .config(f"spark.sql.catalog.{cfg.catalog}.uri", cfg.catalog_uri)
        .config(f"spark.sql.catalog.{cfg.catalog}.warehouse", cfg.warehouse)
        .config(f"spark.sql.catalog.{cfg.catalog}.credential", cfg.oauth_credential)
        .config(f"spark.sql.catalog.{cfg.catalog}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{cfg.catalog}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .getOrCreate()
    )


def bootstrap_lakehouse_tables(
    spark: SparkSession, config: LakehouseStreamConfig | None = None
) -> None:
    """Idempotently create Medallion namespaces and Bronze/DLQ/Silver Iceberg v2 tables."""
    cfg = config or LakehouseStreamConfig.from_env()
    target_ns = f"{cfg.catalog}.{cfg.lakehouse_namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_ns}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{cfg.dwh_namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_ns}.bronze_transactions (
            raw STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_ns}.dlq_transactions (
            raw STRING,
            reason STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_ns}.silver_transactions (
            reference_code STRING,
            currency STRING,
            amount DECIMAL(28, 4),
            status STRING,
            spark_fraud_flagged BOOLEAN,
            ingested_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )


def violates_contract() -> F.Column:
    """Data contract: required fields, positive amount, ISO-4217-shaped currency."""
    return (
        F.col("reference_code").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") <= 0)
        | ~F.col("currency").rlike("^[A-Za-z]{3}$")
    )


def process_batch(
    batch: DataFrame, batch_id: int, config: LakehouseStreamConfig | None = None
) -> None:
    """Route one micro-batch: Bronze append, DLQ for bad rows, idempotent MERGE into Silver."""
    if batch.isEmpty():
        return
    cfg = config or LakehouseStreamConfig.from_env()
    target_ns = f"{cfg.catalog}.{cfg.lakehouse_namespace}"
    spark = batch.sparkSession
    view_name = f"batch_{batch_id}"
    parsed = (
        batch.select(
            F.col("value").cast("string").alias("raw"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("raw", "event.*")
        .persist()
    )
    try:
        parsed.select("raw", F.current_timestamp().alias("ingested_at")).writeTo(
            f"{target_ns}.bronze_transactions"
        ).append()

        bad = parsed.filter(violates_contract())
        bad.select("raw", F.lit("CONTRACT_VIOLATION").alias("reason")).writeTo(
            f"{target_ns}.dlq_transactions"
        ).append()

        good = (
            parsed.filter(~violates_contract())
            .dropDuplicates(["reference_code"])
            .select(
                "reference_code",
                F.upper("currency").alias("currency"),
                "amount",
                F.upper(F.coalesce(F.col("status"), F.lit("SETTLED"))).alias("status"),
                (
                    (F.col("amount") >= F.lit(cfg.high_value_threshold).cast(DecimalType(28, 4)))
                    | (F.col("is_suspicious") == "true")
                ).alias("spark_fraud_flagged"),
                F.current_timestamp().alias("ingested_at"),
            )
        )
        good.createOrReplaceTempView(view_name)
        spark.sql(
            f"""
            MERGE INTO {target_ns}.silver_transactions AS t
            USING {view_name} AS s
            ON t.reference_code = s.reference_code
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view_name)
        parsed.unpersist()


def main() -> None:
    """Bootstrap Lakehouse tables and start the continuous Kafka -> Bronze/Silver stream."""
    cfg = LakehouseStreamConfig.from_env()
    ensure_polaris_catalog(cfg)
    spark = build_session(cfg)
    bootstrap_lakehouse_tables(spark, cfg)
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    (
        stream.writeStream.foreachBatch(functools.partial(process_batch, config=cfg))
        .option("checkpointLocation", cfg.checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
