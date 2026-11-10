-- Iceberg tables for the Medallion Lakehouse (run through Trino against the Polaris catalog).
-- Matches the columns written by spark/bronze_to_silver.py and read by dbt/models/sources.yml.

CREATE SCHEMA IF NOT EXISTS iceberg.forge_lakehouse;
CREATE SCHEMA IF NOT EXISTS iceberg.forge_dwh;

-- Bronze: immutable raw CDC payloads (schema-on-read).
CREATE TABLE IF NOT EXISTS iceberg.forge_lakehouse.bronze_transactions (
    raw VARCHAR,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', format_version = 2);

-- Dead-letter queue: events that violate the data contract.
CREATE TABLE IF NOT EXISTS iceberg.forge_lakehouse.dlq_transactions (
    raw VARCHAR,
    reason VARCHAR
)
WITH (format = 'PARQUET', format_version = 2);

-- Silver: validated rows, upserted by MERGE on reference_code.
CREATE TABLE IF NOT EXISTS iceberg.forge_lakehouse.silver_transactions (
    reference_code VARCHAR,
    currency VARCHAR,
    amount DECIMAL(28, 4),
    status VARCHAR,
    spark_fraud_flagged BOOLEAN,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', format_version = 2);
