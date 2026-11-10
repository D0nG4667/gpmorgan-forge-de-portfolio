{{ config(materialized='table') }}

-- Only SETTLED transactions count toward settled volume; PENDING/FAILED/REVERSED stay in Silver.
select
    currency_code as corridor_key,
    count(*) as transaction_count,
    sum(amount) as total_settled_amount,
    sum(case when is_fraud_flagged then 1 else 0 end) as flagged_count
from {{ ref('stg_silver_transactions') }}
where status = 'SETTLED'
group by currency_code
