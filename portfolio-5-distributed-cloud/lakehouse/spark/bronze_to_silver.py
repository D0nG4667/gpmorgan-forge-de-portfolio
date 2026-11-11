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
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

CATALOG = "forge"
DEFAULT_WAREHOUSE = "forge_lakehouse"
DEFAULT_WAREHOUSE_LOCATION = "file:///tmp/forge_lakehouse/"
DEFAULT_STORAGE_TYPE = "FILE"
DEFAULT_KAFKA_TOPIC = "forge.cdc.transactions"
DEFAULT_CHECKPOINT_DIR = "/tmp/forge-checkpoints/transactions"
HIGH_VALUE_THRESHOLD = "5000000.0000"

EVENT_SCHEMA = StructType(
    [
        StructField("reference_code", StringType()),
        StructField("currency", StringType()),
        StructField("amount", DecimalType(28, 4)),
        StructField("status", StringType()),
        StructField("is_suspicious", StringType()),
    ]
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


def ensure_polaris_catalog() -> None:
    """Idempotently provision the Polaris catalog warehouse and RBAC grants on startup."""
    catalog_uri = os.environ["POLARIS_CATALOG_URI"].rstrip("/")
    credential = os.environ["POLARIS_OAUTH_CREDENTIAL"]
    if ":" not in credential:
        return
    client_id, client_secret = credential.split(":", 1)
    warehouse = os.environ.get("POLARIS_WAREHOUSE", DEFAULT_WAREHOUSE)
    warehouse_location = os.environ.get("POLARIS_WAREHOUSE_LOCATION", DEFAULT_WAREHOUSE_LOCATION)
    storage_type = os.environ.get("POLARIS_STORAGE_TYPE", DEFAULT_STORAGE_TYPE)

    if warehouse_location.startswith("file://"):
        warehouse_path = Path(warehouse_location.removeprefix("file://"))
        warehouse_path.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            warehouse_path.chmod(0o777)

    management_uri = os.environ.get("POLARIS_MANAGEMENT_URI") or (
        f"{catalog_uri.removesuffix('/api/catalog')}/api/management/v1"
        if catalog_uri.endswith("/api/catalog")
        else f"{catalog_uri}/management/v1"
    )
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
            f"{catalog_uri}/v1/oauth/tokens",
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
            f"{management_uri}/catalogs/{warehouse}",
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
                            "name": warehouse,
                            "type": "INTERNAL",
                            "properties": {"default-base-location": warehouse_location},
                            "storageConfigInfo": {
                                "storageType": storage_type,
                                "allowedLocations": [warehouse_location],
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
            f"{management_uri}/catalogs/{warehouse}/catalog-roles/catalog_admin/grants",
            headers=auth_headers,
            payload=json.dumps(
                {
                    "grant": {
                        "type": "catalog",
                        "privilege": "CATALOG_MANAGE_CONTENT",
                        "catalogName": warehouse,
                    }
                }
            ).encode("utf-8"),
            allowed_statuses=frozenset({200, 201, 204, 409}),
        )
        _request_json(
            "PUT",
            f"{management_uri}/principal-roles/service_admin/catalog-roles/{warehouse}",
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


def build_session() -> SparkSession:
    """Create a Spark session wired to the Apache Polaris Iceberg REST catalog."""
    warehouse = os.environ.get("POLARIS_WAREHOUSE", DEFAULT_WAREHOUSE)
    return (
        SparkSession.builder.appName("forge-bronze-to-silver")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.environ["POLARIS_CATALOG_URI"])
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.credential", os.environ["POLARIS_OAUTH_CREDENTIAL"])
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation", "vended-credentials"
        )
        .getOrCreate()
    )


def bootstrap_lakehouse_tables(spark: SparkSession) -> None:
    """Idempotently create Medallion namespaces and Bronze/DLQ/Silver Iceberg v2 tables."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.forge_lakehouse")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.forge_dwh")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.forge_lakehouse.bronze_transactions (
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
        CREATE TABLE IF NOT EXISTS {CATALOG}.forge_lakehouse.dlq_transactions (
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
        CREATE TABLE IF NOT EXISTS {CATALOG}.forge_lakehouse.silver_transactions (
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


def process_batch(batch: DataFrame, batch_id: int) -> None:
    """Route one micro-batch: Bronze append, DLQ for bad rows, idempotent MERGE into Silver."""
    if batch.isEmpty():
        return
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
            f"{CATALOG}.forge_lakehouse.bronze_transactions"
        ).append()

        bad = parsed.filter(violates_contract())
        bad.select("raw", F.lit("CONTRACT_VIOLATION").alias("reason")).writeTo(
            f"{CATALOG}.forge_lakehouse.dlq_transactions"
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
                    (F.col("amount") >= F.lit(HIGH_VALUE_THRESHOLD).cast(DecimalType(28, 4)))
                    | (F.col("is_suspicious") == "true")
                ).alias("spark_fraud_flagged"),
                F.current_timestamp().alias("ingested_at"),
            )
        )
        good.createOrReplaceTempView(view_name)
        spark.sql(
            f"""
            MERGE INTO {CATALOG}.forge_lakehouse.silver_transactions AS t
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
    kafka_topic = os.environ.get("CDC_TOPIC", DEFAULT_KAFKA_TOPIC)
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR)
    ensure_polaris_catalog()
    spark = build_session()
    bootstrap_lakehouse_tables(spark)
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
