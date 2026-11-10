{{ config(materialized='view') }}

select
    reference_code,
    upper(currency) as currency_code,
    cast(amount as decimal(28, 4)) as amount,
    upper(status) as status,
    spark_fraud_flagged as is_fraud_flagged
from {{ source('forge_lakehouse', 'silver_transactions') }}
where reference_code is not null
