"""Spark 4.0 Structured Streaming job: Kafka CDC -> Bronze (raw) -> Silver (validated, merged).

Design patterns:
  * Medallion layering: Bronze keeps every raw event immutably; Silver holds only valid rows.
  * Data contract + dead-letter queue: events violating the contract go to ``dlq_transactions``.
  * Idempotent upsert: ``MERGE INTO`` on ``reference_code`` makes replays/at-least-once safe.
  * Checkpointing: offsets are stored in ``CHECKPOINT_DIR`` for restartable, exactly-once effects.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

CATALOG = "forge"
KAFKA_TOPIC = os.environ.get("CDC_TOPIC", "forge.cdc.transactions")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/forge-checkpoints/transactions")
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


def build_session() -> SparkSession:
    """Create a Spark session wired to the Apache Polaris Iceberg REST catalog."""
    return (
        SparkSession.builder.appName("forge-bronze-to-silver")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", os.environ["POLARIS_CATALOG_URI"])
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", "forge_lakehouse")
        .config(f"spark.sql.catalog.{CATALOG}.credential", os.environ["POLARIS_OAUTH_CREDENTIAL"])
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation", "vended-credentials"
        )
        .getOrCreate()
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
    spark = batch.sparkSession
    parsed = batch.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    ).select("raw", "event.*")

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
    good.createOrReplaceTempView(f"batch_{batch_id}")
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.forge_lakehouse.silver_transactions AS t
        USING batch_{batch_id} AS s
        ON t.reference_code = s.reference_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    """Start the continuous Kafka -> Bronze/Silver streaming query."""
    spark = build_session()
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
