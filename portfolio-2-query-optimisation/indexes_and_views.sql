-- =============================================================================
-- GPMorgan Forge: Strategic Indexing & Materialised Views
-- Portfolio 2: Query Processing and Optimisation
-- Target Engine: PostgreSQL 18
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Composite & Covering B-Tree Indexes (Index-Only Scan Optimisation)
-- -----------------------------------------------------------------------------

-- Optimises chronological account statement & velocity window queries:
-- Supports Index-Only Scan by including amount, fee_amount, status, and reference_code
CREATE INDEX IF NOT EXISTS idx_transactions_source_created_covering
    ON transactions (source_account_id, created_at DESC)
    INCLUDE (amount, fee_amount, status, transaction_type, reference_code);

CREATE INDEX IF NOT EXISTS idx_transactions_dest_created_covering
    ON transactions (destination_account_id, created_at DESC)
    INCLUDE (amount, status, transaction_type, reference_code);

-- Optimises double-entry ledger reconciliation and running balance reconstruction:
CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_created_covering
    ON ledger_entries (account_id, created_at ASC, entry_sequence ASC)
    INCLUDE (direction, amount, balance_before, balance_after, transaction_id);

-- Optimises customer portfolio lookups by segment and KYC tier:
CREATE INDEX IF NOT EXISTS idx_customers_segment_tier_risk
    ON customers (segment, kyc_tier, risk_rating DESC)
    INCLUDE (full_name, country_code);

-- Optimises multi-currency account discovery per customer:
CREATE INDEX IF NOT EXISTS idx_accounts_customer_currency_status
    ON accounts (customer_id, currency, status)
    INCLUDE (account_number, balance, available_balance);

-- -----------------------------------------------------------------------------
-- 2. Partial Indexes (High-Selectivity Hot-Subset Optimisation)
-- -----------------------------------------------------------------------------

-- Index only PENDING or FLAGGED transactions requiring settlement worker or AML review
-- (Reduces index storage footprint by >95% compared to indexing all historical settled rows)
CREATE INDEX IF NOT EXISTS pidx_transactions_active_pipeline
    ON transactions (created_at ASC, source_account_id)
    WHERE status IN ('PENDING', 'FLAGGED');

-- Index only unresolved HIGH and CRITICAL fraud alerts for compliance officers:
CREATE INDEX IF NOT EXISTS pidx_fraud_alerts_unresolved_critical
    ON fraud_alerts (severity, risk_score DESC, created_at DESC)
    WHERE is_resolved = FALSE;

-- Index only active accounts for fast payment routing validation:
CREATE INDEX IF NOT EXISTS pidx_accounts_active_routing
    ON accounts (account_number, currency)
    INCLUDE (account_id, balance, available_balance, daily_transfer_limit, version)
    WHERE status = 'ACTIVE';

-- -----------------------------------------------------------------------------
-- 3. GIN Index for Semi-Structured JSONB Metadata Queries
-- -----------------------------------------------------------------------------

-- Accelerates containment (@>) and key-existence (?) predicates on transaction metadata
CREATE INDEX IF NOT EXISTS gin_transactions_metadata_jsonb
    ON transactions USING GIN (metadata_json jsonb_path_ops);

-- -----------------------------------------------------------------------------
-- 4. Materialised Views for Analytical & BI Offloading
-- -----------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS mv_daily_corridor_settlement;
CREATE MATERIALIZED VIEW mv_daily_corridor_settlement AS
SELECT
    date_trunc('day', t.created_at)                     AS settlement_day,
    t.currency                                          AS currency,
    t.transaction_type                                  AS transaction_type,
    t.channel                                           AS channel,
    COUNT(t.transaction_id)                             AS total_tx_count,
    COUNT(t.transaction_id) FILTER (WHERE t.status = 'SETTLED')  AS settled_tx_count,
    COUNT(t.transaction_id) FILTER (WHERE t.status = 'REVERSED') AS reversed_tx_count,
    COUNT(t.transaction_id) FILTER (WHERE t.status = 'FAILED')   AS failed_tx_count,
    COALESCE(SUM(t.amount) FILTER (WHERE t.status = 'SETTLED'), 0.0000)     AS gross_settled_volume,
    COALESCE(SUM(t.fee_amount) FILTER (WHERE t.status = 'SETTLED'), 0.0000) AS total_fee_revenue,
    ROUND(AVG(t.amount) FILTER (WHERE t.status = 'SETTLED'), 4)             AS avg_ticket_size
FROM transactions t
GROUP BY
    date_trunc('day', t.created_at),
    t.currency,
    t.transaction_type,
    t.channel
WITH DATA;

-- Unique index required for non-blocking REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS uidx_mv_daily_corridor_settlement
    ON mv_daily_corridor_settlement (settlement_day, currency, transaction_type, channel);


DROP MATERIALIZED VIEW IF EXISTS mv_customer_liquidity_360;
CREATE MATERIALIZED VIEW mv_customer_liquidity_360 AS
SELECT
    c.customer_id                                       AS customer_id,
    c.external_ref                                      AS external_ref,
    c.full_name                                         AS full_name,
    c.segment                                           AS segment,
    c.kyc_tier                                          AS kyc_tier,
    c.risk_rating                                       AS risk_rating,
    COUNT(DISTINCT a.account_id)                        AS total_accounts,
    COALESCE(SUM(a.balance) FILTER (WHERE a.currency = 'NGN'), 0.0000) AS total_ngn_balance,
    COUNT(l.entry_id)                                   AS total_ledger_movements,
    MAX(l.created_at)                                   AS last_movement_at
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.customer_id
LEFT JOIN ledger_entries l ON l.account_id = a.account_id
WHERE c.is_anonymised = FALSE
GROUP BY
    c.customer_id,
    c.external_ref,
    c.full_name,
    c.segment,
    c.kyc_tier,
    c.risk_rating
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_mv_customer_liquidity_360
    ON mv_customer_liquidity_360 (customer_id);

COMMIT;
