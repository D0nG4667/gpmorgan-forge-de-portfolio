-- BCBS 239 reconciliation: Gold settled volume must equal Silver SETTLED volume exactly.
-- A dbt singular test fails when this query returns any row.
select 1
from (select sum(total_settled_amount) as gold_total from {{ ref('fact_corridor_settlement') }}) as gold
cross join (
    select sum(amount) as silver_total
    from {{ ref('stg_silver_transactions') }}
    where status = 'SETTLED'
) as silver
where coalesce(gold.gold_total, 0) <> coalesce(silver.silver_total, 0)
