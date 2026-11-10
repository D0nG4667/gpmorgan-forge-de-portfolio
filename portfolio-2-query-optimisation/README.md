# Portfolio 2: Query Processing and Optimisation | GPMorgan Forge

> [!IMPORTANT]
> **Platform Domain**: High-Throughput Financial Analytics, Indexing & Execution Plan Tuning
> **Storage Engine**: PostgreSQL 18 Cost-Based Optimiser (CBO)

---

## 1. Query Processing Pipeline Architecture

When GPMorgan Forge submits an operational or analytical SQL query to PostgreSQL 18, the statement traverses four internal stages before returning results:

```mermaid
flowchart LR
    SQL["1. SQL Statement (Parameterised Query)"] --> Parser["2. Parser & Rewriter (Syntax Tree, Views & RLS Rules)"]
    Parser --> Planner["3. Cost-Based Optimiser (CBO: pg_statistic Histograms, Selectivity & Join Paths)"]
    Planner --> Plan["4. Optimal Execution Plan (Index Only Scan / Nested Loop / WindowAgg)"]
    Plan --> Executor["5. Plan Executor & Shared Buffers (MVCC Visibility Map & Heap/Index Pages)"]
    Executor --> Result["6. Query Result Set (EXPLAIN ANALYZE Telemetry)"]
```

---

## 2. Indexing & Optimisation Strategy

### 2.1 Index Taxonomy Applied to GPMorgan Forge

[`indexes_and_views.sql`](indexes_and_views.sql) defines **nine base-table indexes** (five composite covering B-Trees, three partial B-Trees, and one `jsonb_path_ops` GIN index) alongside **two uniquely indexed materialised views**:

1. **Composite & Covering B-Tree Indexes (`INCLUDE`)**:
   - `idx_ledger_entries_account_created_covering ON ledger_entries (account_id, created_at ASC, entry_sequence ASC) INCLUDE (direction, amount, balance_before, balance_after, transaction_id)` (paired with `idx_transactions_source_created_covering`, `idx_transactions_dest_created_covering`, `idx_customers_segment_tier_risk`, and `idx_accounts_customer_currency_status`)
   - **Why it works**: Satisfies both the `WHERE account_id = $1` equality predicate and the `ORDER BY created_at ASC, entry_sequence ASC` window ordering directly from the B-Tree leaf pages. The `INCLUDE` payload columns allow PostgreSQL to execute an **Index-Only Scan** (`Heap Fetches: 0`) without touching table heap pages or performing an explicit `Sort`.
2. **Partial Indexes (`WHERE` filter on hot operational subsets)**:
   - `pidx_transactions_active_pipeline ON transactions (created_at ASC, source_account_id) WHERE status IN ('PENDING', 'FLAGGED')` (paired with `pidx_fraud_alerts_unresolved_critical` and `pidx_accounts_active_routing`)
   - **Why it works**: In a production payment ledger, $>99.9\%$ of historical transactions are in the terminal `SETTLED` state. Indexing only `PENDING` and `FLAGGED` rows shrinks the B-Tree size by over $95\%$, keeps the entire active index pinned in RAM (`shared_buffers`), and eliminates write amplification when settled rows are archived.
3. **GIN (Generalised Inverted Index) for Semi-Structured JSONB**:
   - `gin_transactions_metadata_jsonb ON transactions USING GIN (metadata_json jsonb_path_ops)`
   - **Why it works**: Hashes every path-value pair inside `metadata_json` (such as device fingerprint, ISO 8583 response code, or NIBSS session ID), turning full-table JSON parsing scans into sub-millisecond `Bitmap Index Scan` lookups.

### 2.2 Materialised Views & Cloud Caching Strategies

To prevent analytical BI dashboards from competing with core OLTP payment transactions for CPU and I/O buffers, GPMorgan Forge implements a two-tier caching and pre-computation layer:

1. **Database-Layer Materialised Views (`mv_daily_corridor_settlement`, `mv_customer_liquidity_360`)**:
   - Pre-computes multi-million-row daily settlement volume, fee revenue, and customer liquidity aggregations.
   - Equipped with `uidx_mv_daily_corridor_settlement` and `uidx_mv_customer_liquidity_360` so `REFRESH MATERIALIZED VIEW CONCURRENTLY` updates each view in the background without acquiring an `ACCESS EXCLUSIVE` lock on concurrent readers.
2. **Application-Layer Redis Caching Strategies (`KeyValueStoreEngine` in [`nosql_models.py`](../src/gpmorgan_forge/nosql_models.py))**:
   - **Read-Through Cache with Explicit Invalidation**: Account balance and status summaries are cached in Redis (`forge:account:<account_number>:summary`, TTL `60s` via `cache_account_summary`) and explicitly invalidated (`invalidate_account_cache` / `DEL`) by the service tier after `transfer_funds` or `reverse_failed_transaction` commits.
   - **Write-Around Cache for Historical Statements**: Monthly PDF/CSV statement exports bypass Redis to prevent cache pollution (evicting hot active-user keys), reading directly from PostgreSQL Read Replicas and Materialised Views.

---

## 3. Performance Benchmark Analysis

```mermaid
xychart-beta
    title "Query Execution Latency Before vs. After Indexing (Milliseconds - Lower is Better)"
    x-axis ["P1 (Q1): Statement", "P2 (Q2): 24h Velocity", "P3: Pending Queue", "P4: JSONB Metadata", "P5 (Q6): Corridor MV"]
    y-axis "Execution Time (ms)" 0 --> 120
    bar [68.71, 54.18, 34.92, 41.60, 112.40]
    bar [0.44, 0.82, 0.12, 0.65, 0.19]
```

### Key Findings from `EXPLAIN (ANALYZE, BUFFERS)`

- **Elimination of External Sort Operations (`P1` / `queries.sql` `Q1`)**: On Query 1 (Chronological Running Balance), providing a composite index matching the `PARTITION BY account_id ORDER BY created_at ASC, entry_sequence ASC` window specification eliminated the `Sort` operator entirely, reducing execution latency from **`68.71 ms` to `0.44 ms` ($156.2\times$ speedup)** and dropping buffer page reads from `6,434` blocks to `14` shared-buffer hits.
- **Partial Index Selectivity (`P3` Pipeline Micro-Benchmark)**: Polling the active settlement queue with `pidx_transactions_active_pipeline` instead of a full-table `Seq Scan` (which discarded `249,955` settled rows) reduced latency from **`34.92 ms` to `0.12 ms` ($291.0\times$ speedup)**. *(Note: `queries.sql` `Q3`–`Q5` cover the Global Trial Balance, `mv_customer_liquidity_360` ranking, and Reversal Lineage self-join.)*
- **Materialised View Pre-Aggregation (`P5` / `queries.sql` `Q6`)**: Pre-computing daily corridor metrics in `mv_daily_corridor_settlement` reduced executive dashboard query latency from **`112.40 ms` to `0.19 ms` ($591.6\times$ speedup)**.

---

## 4. Deliverable Files in This Directory

- [`queries.sql`](queries.sql): 6 production SQL queries (`Q1`–`Q6`: CTEs, Window Functions, Trial Balance Audit, Customer 360 Materialised View, Reversal Lineage Self-Join, and Corridor Rollup).
- [`indexes_and_views.sql`](indexes_and_views.sql): 9 base-table indexes (5 covering B-Trees, 3 partial B-Trees, 1 GIN JSONB index) and 2 uniquely indexed concurrent Materialised Views.
- [`execution_plans_evidence.md`](execution_plans_evidence.md): Captured query results, PostgreSQL 18 `EXPLAIN (ANALYZE, BUFFERS)` execution plans, and quantitative performance tables.
- [`src/gpmorgan_forge/query_optimizer.py`](../src/gpmorgan_forge/query_optimizer.py): Python SQLAlchemy 2.1 analytical query builder, running-balance verifier, velocity detector, and deterministic execution plan inspector (`EXPLAIN` on PostgreSQL / `EXPLAIN QUERY PLAN` in the automated SQLite test harness).
