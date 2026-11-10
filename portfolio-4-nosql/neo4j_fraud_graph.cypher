// =============================================================================
// GPMorgan Forge: Neo4j 5.26 LTS Cypher Fraud Ring & Recommendation Graph
// Portfolio 4: NoSQL and Advanced Data Models
// Target Engine: Neo4j 5.26 LTS (Cypher)
// =============================================================================

// -----------------------------------------------------------------------------
// 1. Graph Schema Constraints & Property Indexes
//    Note: Uses modern Neo4j 5+ `CREATE CONSTRAINT/INDEX ... IF NOT EXISTS FOR`
//    (Replaces legacy Neo4j 3.x/4.x `CREATE INDEX ON :Account(account_number)`)
// -----------------------------------------------------------------------------
CREATE CONSTRAINT uq_graph_customer_id IF NOT EXISTS
FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;

CREATE CONSTRAINT uq_graph_account_number IF NOT EXISTS
FOR (a:Account) REQUIRE a.account_number IS UNIQUE;

CREATE CONSTRAINT uq_graph_device_id IF NOT EXISTS
FOR (d:Device) REQUIRE d.device_fingerprint IS UNIQUE;

CREATE INDEX idx_graph_account_risk IF NOT EXISTS
FOR (a:Account) ON (a.risk_score);

CREATE INDEX idx_graph_customer_segment IF NOT EXISTS
FOR (c:Customer) ON (c.segment);

// -----------------------------------------------------------------------------
// 2. Seed Graph Nodes & Relationships (Customers, Accounts, Devices, Products)
// -----------------------------------------------------------------------------
MERGE (c1:Customer {customer_id: "CUST-NG-00102", name: "Adebayo Ogunlesi Ventures", segment: "CORPORATE"})
MERGE (c2:Customer {customer_id: "CUST-NG-00103", name: "Chidinma Okafor", segment: "RETAIL"})
MERGE (c3:Customer {customer_id: "CUST-NG-00104", name: "Ibrahim Danjuma", segment: "SME"})
MERGE (c4:Customer {customer_id: "CUST-NG-00991", name: "Syndicate Shell Alpha", segment: "SME"})
MERGE (c5:Customer {customer_id: "CUST-NG-00992", name: "Syndicate Shell Beta", segment: "SME"})
MERGE (c6:Customer {customer_id: "CUST-NG-00993", name: "Syndicate Shell Gamma", segment: "SME"})

MERGE (a1:Account {account_number: "2000000002", currency: "NGN", risk_score: 12.5})
MERGE (a2:Account {account_number: "2000000003", currency: "NGN", risk_score: 8.0})
MERGE (a3:Account {account_number: "2000000004", currency: "NGN", risk_score: 18.75})
MERGE (a4:Account {account_number: "9000000011", currency: "NGN", risk_score: 82.0})
MERGE (a5:Account {account_number: "9000000012", currency: "NGN", risk_score: 85.0})
MERGE (a6:Account {account_number: "9000000013", currency: "NGN", risk_score: 88.0})

MERGE (d_legit:Device {device_fingerprint: "DEV-IOS-CHIDINMA-01", os: "iOS 18", is_emulator: false})
MERGE (d_mule:Device  {device_fingerprint: "DEV-ANDROID-MULE-FARM-99", os: "Android Emulator", is_emulator: true})

MERGE (p_fx:FinancialProduct {product_id: "PROD-FX-FORWARD", name: "FX Forward Hedging Line", category: "TREASURY"})
MERGE (p_inv:FinancialProduct {product_id: "PROD-TBILLS-90D", name: "90-Day Sovereign T-Bill Vault", category: "INVESTMENT"})
MERGE (p_wc:FinancialProduct {product_id: "PROD-SME-WC-LOAN", name: "Supply Chain Working Capital", category: "CREDIT"})

MERGE (c1)-[:OWNS_ACCOUNT]->(a1)
MERGE (c2)-[:OWNS_ACCOUNT]->(a2)
MERGE (c3)-[:OWNS_ACCOUNT]->(a3)
MERGE (c4)-[:OWNS_ACCOUNT]->(a4)
MERGE (c5)-[:OWNS_ACCOUNT]->(a5)
MERGE (c6)-[:OWNS_ACCOUNT]->(a6)

// Shared-device mule farm connection (c4, c5, c6 all log in from DEV-ANDROID-MULE-FARM-99)
MERGE (c2)-[:USED_DEVICE {last_seen: datetime("2026-11-04T10:00:00Z")}]->(d_legit)
MERGE (c4)-[:USED_DEVICE {last_seen: datetime("2026-11-04T11:01:00Z")}]->(d_mule)
MERGE (c5)-[:USED_DEVICE {last_seen: datetime("2026-11-04T11:03:00Z")}]->(d_mule)
MERGE (c6)-[:USED_DEVICE {last_seen: datetime("2026-11-04T11:05:00Z")}]->(d_mule)

// Legitimate commercial transfers
MERGE (a1)-[:TRANSFERRED_TO {tx_ref: "FRG-TRF-20261102-0003", amount: 1250000.00, timestamp: datetime("2026-11-02T14:30:00Z")}]->(a2)
MERGE (a1)-[:TRANSFERRED_TO {tx_ref: "FRG-TRF-20261103-0004", amount: 1500000.00, timestamp: datetime("2026-11-03T11:20:00Z")}]->(a3)

// Circular Money Laundering Ring (Round-Tripping Loop: a4 -> a5 -> a6 -> a4)
MERGE (a4)-[:TRANSFERRED_TO {tx_ref: "FRG-AML-LOOP-01", amount: 4950000.00, timestamp: datetime("2026-11-04T11:10:00Z")}]->(a5)
MERGE (a5)-[:TRANSFERRED_TO {tx_ref: "FRG-AML-LOOP-02", amount: 4900000.00, timestamp: datetime("2026-11-04T11:12:00Z")}]->(a6)
MERGE (a6)-[:TRANSFERRED_TO {tx_ref: "FRG-AML-LOOP-03", amount: 4850000.00, timestamp: datetime("2026-11-04T11:15:00Z")}]->(a4)

// Financial Product Subscriptions & Peer Network for Recommendations
MERGE (c1)-[:PARTNERS_WITH {since: 2024}]->(c3)
MERGE (c1)-[:SUBSCRIBED_TO]->(p_fx)
MERGE (c1)-[:SUBSCRIBED_TO]->(p_inv)
MERGE (c1)-[:SUBSCRIBED_TO]->(p_wc)
MERGE (c3)-[:SUBSCRIBED_TO]->(p_wc);

// -----------------------------------------------------------------------------
// 3. Cypher Query 1: Circular Money-Laundering Loop Detection (2 to 5 Hops)
//    Optimised with bounded depth `*2..5`, specific relationship `:TRANSFERRED_TO`,
//    and indexed property lookup on `risk_score`.
// -----------------------------------------------------------------------------
MATCH path = (origin:Account)-[:TRANSFERRED_TO*2..5]->(origin)
WHERE origin.risk_score >= 50.0
  AND all(n IN tail(nodes(path)) WHERE origin.account_number <= n.account_number)
WITH origin, path,
     [n IN nodes(path) | n.account_number] AS ring_accounts,
     reduce(total = 0.0, r IN relationships(path) | total + r.amount) AS total_laundered_volume
RETURN
    origin.account_number AS entry_account,
    length(path)          AS hop_count,
    ring_accounts         AS circular_path,
    total_laundered_volume
ORDER BY total_laundered_volume DESC;

// -----------------------------------------------------------------------------
// 4. Cypher Query 2: Shared-Device Mule Ring Detection
//    Identifies distinct customers/accounts controlled by the same physical device.
// -----------------------------------------------------------------------------
MATCH (c:Customer)-[:USED_DEVICE]->(d:Device)
MATCH (c)-[:OWNS_ACCOUNT]->(a:Account)
WITH d,
     collect(DISTINCT c.customer_id)    AS linked_customers,
     collect(DISTINCT a.account_number) AS linked_accounts
WHERE size(linked_customers) > 1
RETURN
    d.device_fingerprint AS shared_device,
    d.is_emulator        AS is_emulator,
    linked_customers,
    linked_accounts;

// -----------------------------------------------------------------------------
// 5. Cypher Query 3: Collaborative Filtering Product Recommendation Engine
//    Recommends financial products subscribed to by business partners that the
//    target customer has not yet adopted.
// -----------------------------------------------------------------------------
MATCH (target:Customer {customer_id: "CUST-NG-00104"})-[:PARTNERS_WITH]-(peer:Customer)-[:SUBSCRIBED_TO]->(rec:FinancialProduct)
WHERE NOT (target)-[:SUBSCRIBED_TO]->(rec)
RETURN
    target.name      AS customer_name,
    rec.product_id   AS recommended_product_id,
    rec.name         AS recommended_product_name,
    rec.category     AS product_category;
