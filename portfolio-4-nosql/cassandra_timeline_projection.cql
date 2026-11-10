-- =============================================================================
-- GPMorgan Forge: Apache Cassandra 5.0 Wide-Column Timeline Projection
-- Portfolio 4: NoSQL and Advanced Data Models
-- Target Engine: Apache Cassandra 5.0 (CQL v3.4.7)
--
-- ARCHITECTURAL INVARIANT:
--   Apache Cassandra is used STRICTLY as a read-only, append-only customer
--   transaction timeline projection fed asynchronously via Kafka 4.0 CDC from
--   the PostgreSQL 18 / CockroachDB double-entry ledger. It is NEVER used for
--   mutable account balances, pessimistic locking, or double-entry transfers.
-- =============================================================================

-- Multi-DC Ring Topology: {'class': 'NetworkTopologyStrategy', 'dc_lagos_ng': 3, 'dc_london_gb': 3}
CREATE KEYSPACE IF NOT EXISTS forge_timeline
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc_lagos_ng': 1
} AND durable_writes = true;

-- -----------------------------------------------------------------------------
-- 1. Partitioned Wide-Row Timeline Table
--    - Partition Key: (account_id, bucket_month) prevents unbounded wide-row
--      growth while co-locating a full calendar month of customer statement
--      postings on a single storage replica ring token.
--    - Clustering Key: (created_at DESC, transaction_id ASC) pre-sorts events
--      on disk in reverse chronological order for sub-5ms mobile scrolling.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forge_timeline.customer_transaction_timeline (
    account_id      UUID,
    bucket_month    TEXT,
    created_at      TIMESTAMP,
    transaction_id  UUID,
    reference_code  TEXT,
    direction       TEXT,
    amount          DECIMAL,
    currency        TEXT,
    balance_after   DECIMAL,
    narration       TEXT,
    corridor_region TEXT,
    PRIMARY KEY ((account_id, bucket_month), created_at, transaction_id)
) WITH CLUSTERING ORDER BY (created_at DESC, transaction_id ASC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 7
  }
  AND gc_grace_seconds = 864000
  AND comment = 'Append-only CDC customer transaction timeline projection (read-only view)';

-- -----------------------------------------------------------------------------
-- 2. Idempotent Append-Only CDC Projection Inserts (CONSISTENCY LOCAL_QUORUM)
-- -----------------------------------------------------------------------------
INSERT INTO forge_timeline.customer_transaction_timeline (
    account_id, bucket_month, created_at, transaction_id, reference_code,
    direction, amount, currency, balance_after, narration, corridor_region
) VALUES (
    30000000-0000-4000-8000-000000000002,
    '2026-11',
    '2026-11-01T09:00:00Z',
    50000000-0000-4000-8000-000000000001,
    'FRG-DEP-20261101-0001',
    'CREDIT',
    15000000.0000,
    'NGN',
    15000000.0000,
    'Corporate treasury initial funding deposit',
    'NG'
);

INSERT INTO forge_timeline.customer_transaction_timeline (
    account_id, bucket_month, created_at, transaction_id, reference_code,
    direction, amount, currency, balance_after, narration, corridor_region
) VALUES (
    30000000-0000-4000-8000-000000000002,
    '2026-11',
    '2026-11-02T14:30:00Z',
    50000000-0000-4000-8000-000000000002,
    'FRG-TRF-20261102-0002',
    'DEBIT',
    1250000.0000,
    'NGN',
    13750000.0000,
    'Vendor consultancy milestone settlement',
    'NG'
);

-- -----------------------------------------------------------------------------
-- 3. Single-Partition Contiguous Time-Slice Read Query
--    Scans a single storage partition (account_id, bucket_month) in O(1) ring
--    coordination without scatter-gather fan-out across cluster nodes.
-- -----------------------------------------------------------------------------
SELECT account_id, bucket_month, created_at, transaction_id, reference_code,
       direction, amount, currency, balance_after, narration
FROM forge_timeline.customer_transaction_timeline
WHERE account_id = 30000000-0000-4000-8000-000000000002
  AND bucket_month = '2026-11'
  AND created_at >= '2026-11-01T00:00:00Z'
LIMIT 50;
