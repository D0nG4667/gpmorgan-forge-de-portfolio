-- =============================================================================
-- GPMorgan Forge: CockroachDB v24.3 Multi-Region Corridor Settlement Schema
-- Portfolio 5: Active-Active Distributed SQL (Raft SERIALIZABLE Consensus)
-- =============================================================================

CREATE TABLE IF NOT EXISTS global_corridor_settlements (
    settlement_id           UUID NOT NULL DEFAULT gen_random_uuid(),
    idempotency_key         VARCHAR(128) NOT NULL,
    corridor_region         CHAR(2) NOT NULL DEFAULT 'NG',
    source_account_id       UUID NOT NULL,
    destination_account_id  UUID NOT NULL,
    currency                CHAR(3) NOT NULL DEFAULT 'USD',
    amount                  DECIMAL(19, 4) NOT NULL,
    raft_consensus_state    VARCHAR(24) NOT NULL DEFAULT 'SERIALIZABLE_COMMITTED',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT current_timestamp(),
    PRIMARY KEY (corridor_region, settlement_id),
    CONSTRAINT uq_global_settlements_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_global_settlement_positive CHECK (amount > 0.0000)
);

INSERT INTO global_corridor_settlements (
    settlement_id, idempotency_key, corridor_region,
    source_account_id, destination_account_id, currency, amount, raft_consensus_state
) VALUES (
    '99000000-0000-4000-8000-000000000001',
    'IDEMP-CRDB-NG-GB-2026-001',
    'NG',
    '30000000-0000-4000-8000-000000000002',
    '30000000-0000-4000-8000-000000000004',
    'USD',
    250000.0000,
    'SERIALIZABLE_COMMITTED'
) ON CONFLICT (idempotency_key) DO NOTHING;
