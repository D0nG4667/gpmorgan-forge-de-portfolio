{{ config(materialized='table') }}

select distinct
    currency_code as corridor_key,
    currency_code
from {{ ref('stg_silver_transactions') }}
where status = 'SETTLED'
