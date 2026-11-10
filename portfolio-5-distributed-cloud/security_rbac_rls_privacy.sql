-- =============================================================================
-- GPMorgan Forge: Database Security, RBAC, ABAC, RLS & Privacy Compliance
-- Portfolio 5: Distributed and Cloud Database Exercise
-- Target Engine: PostgreSQL 18
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Role-Based Access Control (RBAC): Principle of Least Privilege
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forge_admin') THEN
        CREATE ROLE forge_admin NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forge_core_service') THEN
        CREATE ROLE forge_core_service NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forge_compliance_dpo') THEN
        CREATE ROLE forge_compliance_dpo NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forge_analyst') THEN
        CREATE ROLE forge_analyst NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forge_customer') THEN
        CREATE ROLE forge_customer NOLOGIN;
    END IF;
END
$$;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO forge_admin;

-- Core payment service can read/insert/update operational tables, but only
-- INSERT (append) into immutable ledger_entries and transaction_audit_logs:
GRANT SELECT, INSERT, UPDATE ON customers, accounts, transactions, kyc_records, consent_records, fraud_alerts TO forge_core_service;
GRANT SELECT, INSERT ON ledger_entries, transaction_audit_logs TO forge_core_service;

-- Compliance Officer & Data Protection Officer (DPO) role:
GRANT SELECT ON ALL TABLES IN SCHEMA public TO forge_compliance_dpo;
GRANT UPDATE (is_resolved, resolved_by, resolved_at) ON fraud_alerts TO forge_compliance_dpo;

-- Customer role: strictly governed by Row-Level Security (RLS)
GRANT SELECT ON customers, accounts, transactions, ledger_entries, payment_cards, beneficiaries TO forge_customer;

-- -----------------------------------------------------------------------------
-- 2. Dynamic Data Masking Views for BI Analysts (`forge_analyst`)
--    Prevents PII exposure via dynamic masking and inference prevention
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_masked_customers AS
SELECT
    customer_id,
    external_ref,
    -- Mask full name to initials + "***"
    LEFT(full_name, 2) || '***'                         AS masked_name,
    -- Mask email local part: ch***@example.ng
    LEFT(email, 2) || '***@' || SPLIT_PART(email, '@', 2) AS masked_email,
    -- Mask phone number: +234******3303
    LEFT(phone_e164, 4) || '******' || RIGHT(phone_e164, 4) AS masked_phone,
    segment,
    kyc_tier,
    country_code,
    risk_rating,
    is_anonymised,
    created_at
FROM customers;

CREATE OR REPLACE VIEW v_masked_payment_cards AS
SELECT
    card_id,
    account_id,
    masked_pan, -- Strictly formatted as XXXX-XXXX-XXXX-1234
    card_scheme,
    expiry_year,
    status
FROM payment_cards;

-- Revoke raw PII table access from analysts; grant only dynamically masked views
REVOKE ALL ON customers, kyc_records, payment_cards FROM forge_analyst;
GRANT SELECT ON v_masked_customers, v_masked_payment_cards TO forge_analyst;

-- -----------------------------------------------------------------------------
-- 3. Row-Level Security (RLS) & Attribute-Based Access Control (ABAC)
--    Restricts `forge_customer` to rows matching session context attribute
--    `app.current_customer_id`, and restricts `forge_compliance_dpo` to their
--    assigned jurisdiction `app.actor_country` (Data Residency).
-- -----------------------------------------------------------------------------
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE customers FORCE ROW LEVEL SECURITY;
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE accounts FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pol_customers_customer_isolation ON customers;
CREATE POLICY pol_customers_customer_isolation ON customers
    FOR SELECT
    TO forge_customer
    USING (customer_id = NULLIF(current_setting('app.current_customer_id', TRUE), '')::uuid);

DROP POLICY IF EXISTS pol_customers_dpo_jurisdiction ON customers;
CREATE POLICY pol_customers_dpo_jurisdiction ON customers
    FOR SELECT
    TO forge_compliance_dpo
    USING (country_code = NULLIF(current_setting('app.actor_country', TRUE), ''));

DROP POLICY IF EXISTS pol_customers_core_all ON customers;
CREATE POLICY pol_customers_core_all ON customers
    FOR ALL
    TO forge_core_service
    USING (TRUE)
    WITH CHECK (TRUE);

DROP POLICY IF EXISTS pol_accounts_customer_isolation ON accounts;
CREATE POLICY pol_accounts_customer_isolation ON accounts
    FOR SELECT
    TO forge_customer
    USING (customer_id = NULLIF(current_setting('app.current_customer_id', TRUE), '')::uuid);

DROP POLICY IF EXISTS pol_accounts_core_and_dpo_all ON accounts;
CREATE POLICY pol_accounts_core_and_dpo_all ON accounts
    FOR ALL
    TO forge_core_service, forge_compliance_dpo
    USING (TRUE)
    WITH CHECK (TRUE);

-- -----------------------------------------------------------------------------
-- 4. NDPA 2023 & GDPR Right-to-Erasure / Pseudonymisation Stored Procedure
--    Anonymises customer PII while preserving statutory non-PII double-entry
--    financial ledger records required by Central Bank (CBN) regulations.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION anonymise_customer_pii(
    p_customer_id UUID,
    p_dpo_actor_id VARCHAR(80)
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_pseudonym VARCHAR(64);
BEGIN
    v_pseudonym := encode(digest(p_customer_id::text || ':NDPA_ERASURE', 'sha256'), 'hex');

    UPDATE customers
    SET full_name     = 'REDACTED_' || LEFT(v_pseudonym, 12),
        email         = LEFT(v_pseudonym, 16) || '@anonymised.invalid',
        phone_e164    = '+000' || LEFT(REGEXP_REPLACE(v_pseudonym, '[^0-9]', '', 'g') || '0000000000', 10),
        bvn_sha256    = v_pseudonym,
        is_anonymised = TRUE,
        updated_at    = CURRENT_TIMESTAMP
    WHERE customer_id = p_customer_id;

    UPDATE consent_records
    SET consent_granted = FALSE,
        revoked_at      = CURRENT_TIMESTAMP
    WHERE customer_id = p_customer_id
      AND consent_granted = TRUE;
END;
$$;

COMMIT;
