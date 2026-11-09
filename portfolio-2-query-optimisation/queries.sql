-- =============================================================================
-- GPMorgan Forge: Operational & Analytical SQL Query Suite
-- Portfolio 2: Query Processing and Optimisation
-- Target Engine: PostgreSQL 18
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: Chronological Account Statement & Running Balance Verification
-- Uses Window Functions (SUM() OVER, LAG() OVER) to reconstruct the running
-- balance from immutable ledger legs and detect any mathematical drift.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
WITH signed_ledger AS (
    SELECT
        le.entry_id,
        le.account_id,
        a.account_number,
        a.account_name,
        le.transaction_id,
        t.reference_code,
        t.transaction_type,
        le.direction,
        le.amount,
        CASE
            WHEN le.direction = 'CREDIT' THEN le.amount
            ELSE -le.amount
        END AS signed_amount,
        le.balance_before,
        le.balance_after,
        le.created_at,
        le.entry_sequence
    FROM ledger_entries le
    INNER JOIN accounts a ON a.account_id = le.account_id
    INNER JOIN transactions t ON t.transaction_id = le.transaction_id
    WHERE a.account_number = '2000000002'
)
SELECT
    account_number,
    reference_code,
    transaction_type,
    direction,
    amount,
    balance_before,
    balance_after,
    SUM(signed_amount) OVER (
        PARTITION BY account_id
        ORDER BY created_at ASC, entry_sequence ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS window_computed_balance,
    balance_after - SUM(signed_amount) OVER (
        PARTITION BY account_id
        ORDER BY created_at ASC, entry_sequence ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS balance_drift,
    created_at
FROM signed_ledger
ORDER BY created_at ASC, entry_sequence ASC;

-- -----------------------------------------------------------------------------
-- Query 2: High-Velocity Structuring / Smurfing AML Detection (Rolling Window)
-- Identifies source accounts executing multiple outbound transfers within a
-- rolling 24-hour window or approaching CBN reporting thresholds.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
WITH outbound_velocity AS (
    SELECT
        t.transaction_id,
        t.reference_code,
        t.source_account_id,
        a.account_number,
        c.full_name,
        c.segment,
        t.amount,
        t.created_at,
        COUNT(t.transaction_id) OVER (
            PARTITION BY t.source_account_id
            ORDER BY t.created_at
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS tx_count_24h,
        SUM(t.amount) OVER (
            PARTITION BY t.source_account_id
            ORDER BY t.created_at
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS rolling_volume_24h,
        a.daily_transfer_limit
    FROM transactions t
    INNER JOIN accounts a ON a.account_id = t.source_account_id
    INNER JOIN customers c ON c.customer_id = a.customer_id
    WHERE t.status = 'SETTLED'
      AND t.transaction_type IN ('TRANSFER', 'WITHDRAWAL')
)
SELECT
    account_number,
    full_name,
    segment,
    reference_code,
    amount,
    tx_count_24h,
    rolling_volume_24h,
    ROUND((rolling_volume_24h / daily_transfer_limit) * 100.0, 2) AS limit_utilisation_pct,
    created_at
FROM outbound_velocity
WHERE tx_count_24h >= 2 OR rolling_volume_24h >= 1000000.0000
ORDER BY rolling_volume_24h DESC, created_at DESC;

-- -----------------------------------------------------------------------------
-- Query 3: Global Double-Entry Trial Balance & Account Reconciliation Audit
-- Verifies that every account's live `balance` matches the net sum of its
-- historical `CREDIT` minus `DEBIT` ledger entries.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT
    a.account_number,
    a.account_name,
    a.account_type,
    a.currency,
    a.balance AS live_account_balance,
    COALESCE(SUM(CASE WHEN le.direction = 'CREDIT' THEN le.amount ELSE 0.0000 END), 0.0000) AS total_credits,
    COALESCE(SUM(CASE WHEN le.direction = 'DEBIT'  THEN le.amount ELSE 0.0000 END), 0.0000) AS total_debits,
    COALESCE(
        SUM(CASE WHEN le.direction = 'CREDIT' THEN le.amount ELSE -le.amount END),
        0.0000
    ) AS ledger_net_balance,
    a.balance - COALESCE(
        SUM(CASE WHEN le.direction = 'CREDIT' THEN le.amount ELSE -le.amount END),
        0.0000
    ) AS reconciliation_discrepancy
FROM accounts a
LEFT JOIN ledger_entries le ON le.account_id = a.account_id
GROUP BY a.account_id, a.account_number, a.account_name, a.account_type, a.currency, a.balance
ORDER BY a.account_number ASC;

-- -----------------------------------------------------------------------------
-- Query 4: Customer 360 Liquidity, RFM Scoring & Segment Ranking
-- Ranks customers within each segment by total settled volume and liquidity.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
WITH customer_metrics AS (
    SELECT
        c.customer_id,
        c.external_ref,
        c.full_name,
        c.segment,
        c.kyc_tier,
        c.risk_rating,
        COALESCE(SUM(DISTINCT a.balance), 0.0000) AS total_balance,
        COUNT(t.transaction_id) AS settled_tx_count,
        COALESCE(SUM(t.amount), 0.0000) AS total_outbound_volume,
        MAX(t.created_at) AS last_tx_timestamp
    FROM customers c
    LEFT JOIN accounts a ON a.customer_id = c.customer_id
    LEFT JOIN transactions t
        ON t.source_account_id = a.account_id
        AND t.status = 'SETTLED'
    WHERE a.account_type IS NULL OR a.account_type <> 'SETTLEMENT_GL'
    GROUP BY c.customer_id, c.external_ref, c.full_name, c.segment, c.kyc_tier, c.risk_rating
)
SELECT
    external_ref,
    full_name,
    segment,
    kyc_tier,
    total_balance,
    settled_tx_count,
    total_outbound_volume,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY total_balance DESC) AS segment_liquidity_rank,
    last_tx_timestamp
FROM customer_metrics
WHERE external_ref <> 'CUST-GL-TREASURY'
ORDER BY total_balance DESC;

-- -----------------------------------------------------------------------------
-- Query 5: Reversed & Failed Transaction Lineage Analysis (Self-Join)
-- Traces compensating contra-entry reversals back to their parent transaction.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT
    orig.reference_code         AS original_reference,
    orig.transaction_type       AS original_type,
    orig.amount                 AS original_amount,
    orig.status                 AS original_status,
    orig.failure_reason         AS reversal_reason,
    rev.reference_code          AS reversal_reference,
    rev.settled_at              AS reversed_at,
    EXTRACT(EPOCH FROM (rev.settled_at - orig.created_at)) AS resolution_seconds
FROM transactions rev
INNER JOIN transactions orig ON orig.transaction_id = rev.parent_transaction_id
WHERE rev.transaction_type = 'REVERSAL'
ORDER BY rev.settled_at DESC;

-- -----------------------------------------------------------------------------
-- Query 6: Materialised View Refresh & Instant Corridor Settlement Reporting
-- Demonstrates sub-millisecond analytical query execution against pre-computed
-- Materialised Views refreshed concurrently.
-- -----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_corridor_settlement;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_liquidity_360;

EXPLAIN (ANALYZE, BUFFERS, COSTS, TIMING)
SELECT
    settlement_day,
    currency,
    transaction_type,
    channel,
    settled_tx_count,
    reversed_tx_count,
    gross_settled_volume,
    total_fee_revenue,
    avg_ticket_size
FROM mv_daily_corridor_settlement
ORDER BY settlement_day DESC, gross_settled_volume DESC;
