-- =============================================================================
-- GPMorgan Forge: Core Banking & Double-Entry Financial Ledger Schema (3NF/BCNF)
-- Portfolio 1: Database Design and Modelling
-- Target Engine: PostgreSQL 18
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------------------------------------
-- 1. Domain Enumerated Types
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'customer_segment') THEN
        CREATE TYPE customer_segment AS ENUM ('RETAIL', 'SME', 'CORPORATE', 'INSTITUTIONAL');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'kyc_tier_level') THEN
        CREATE TYPE kyc_tier_level AS ENUM ('TIER_1', 'TIER_2', 'TIER_3');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_type') THEN
        CREATE TYPE account_type AS ENUM ('CHECKING', 'SAVINGS', 'ESCROW', 'SETTLEMENT_GL', 'TREASURY');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'account_status') THEN
        CREATE TYPE account_status AS ENUM ('ACTIVE', 'FROZEN', 'DORMANT', 'CLOSED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'currency_code') THEN
        CREATE TYPE currency_code AS ENUM ('NGN', 'USD', 'GBP', 'EUR');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_type') THEN
        CREATE TYPE transaction_type AS ENUM ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'REVERSAL', 'FEE_SETTLEMENT');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_status') THEN
        CREATE TYPE transaction_status AS ENUM ('PENDING', 'SETTLED', 'FAILED', 'REVERSED', 'FLAGGED');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'entry_direction') THEN
        CREATE TYPE entry_direction AS ENUM ('DEBIT', 'CREDIT');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'risk_severity') THEN
        CREATE TYPE risk_severity AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 2. Customers Table (3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_ref        VARCHAR(32) NOT NULL,
    full_name           VARCHAR(160) NOT NULL,
    email               VARCHAR(255) NOT NULL,
    phone_e164          VARCHAR(24) NOT NULL,
    segment             customer_segment NOT NULL DEFAULT 'RETAIL',
    kyc_tier            kyc_tier_level NOT NULL DEFAULT 'TIER_1',
    bvn_sha256          CHAR(64) NOT NULL,
    country_code        CHAR(2) NOT NULL DEFAULT 'NG',
    risk_rating         NUMERIC(5, 2) NOT NULL DEFAULT 10.00,
    is_anonymised       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_customers_external_ref UNIQUE (external_ref),
    CONSTRAINT uq_customers_email UNIQUE (email),
    CONSTRAINT uq_customers_phone UNIQUE (phone_e164),
    CONSTRAINT uq_customers_bvn_sha256 UNIQUE (bvn_sha256),
    CONSTRAINT chk_customers_risk_rating CHECK (risk_rating >= 0.00 AND risk_rating <= 100.00),
    CONSTRAINT chk_customers_country_code CHECK (country_code ~ '^[A-Z]{2}$')
);

-- -----------------------------------------------------------------------------
-- 3. KYC Verification Records Table (3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kyc_records (
    kyc_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id         UUID NOT NULL,
    document_type       VARCHAR(40) NOT NULL,
    document_ref_hash   CHAR(64) NOT NULL,
    issuing_country     CHAR(2) NOT NULL DEFAULT 'NG',
    verification_status VARCHAR(24) NOT NULL DEFAULT 'VERIFIED',
    verified_by         VARCHAR(80) NOT NULL,
    verified_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at          TIMESTAMPTZ,

    CONSTRAINT fk_kyc_records_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON DELETE RESTRICT,
    CONSTRAINT uq_kyc_records_doc_hash UNIQUE (document_ref_hash),
    CONSTRAINT chk_kyc_verification_status
        CHECK (verification_status IN ('PENDING', 'VERIFIED', 'REJECTED', 'EXPIRED'))
);

-- -----------------------------------------------------------------------------
-- 4. Regulatory Consent Records Table (NDPA 2023 & GDPR Compliance, BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consent_records (
    consent_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id         UUID NOT NULL,
    regulation_code     VARCHAR(24) NOT NULL,
    purpose_code        VARCHAR(64) NOT NULL,
    consent_granted     BOOLEAN NOT NULL,
    ip_address          VARCHAR(45) NOT NULL,
    granted_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at          TIMESTAMPTZ,

    CONSTRAINT fk_consent_records_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON DELETE RESTRICT,
    CONSTRAINT chk_consent_regulation
        CHECK (regulation_code IN ('NDPA_2023', 'GDPR', 'CBN_AML', 'PCI_DSS'))
);

-- -----------------------------------------------------------------------------
-- 5. Accounts Table (Multi-Currency Wallets & Settlement GLs, 3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts (
    account_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id             UUID NOT NULL,
    account_number          CHAR(10) NOT NULL,
    account_name            VARCHAR(160) NOT NULL,
    account_type            account_type NOT NULL DEFAULT 'CHECKING',
    currency                currency_code NOT NULL DEFAULT 'NGN',
    balance                 NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    available_balance       NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    daily_transfer_limit    NUMERIC(19, 4) NOT NULL DEFAULT 5000000.0000,
    status                  account_status NOT NULL DEFAULT 'ACTIVE',
    version                 INTEGER NOT NULL DEFAULT 1,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_accounts_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON DELETE RESTRICT,
    CONSTRAINT uq_accounts_account_number UNIQUE (account_number),
    CONSTRAINT chk_accounts_number_digits CHECK (account_number ~ '^[0-9]{10}$'),
    CONSTRAINT chk_accounts_non_negative_customer_balance
        CHECK (account_type = 'SETTLEMENT_GL' OR (balance >= 0.0000 AND available_balance >= 0.0000)),
    CONSTRAINT chk_accounts_available_le_balance
        CHECK (account_type = 'SETTLEMENT_GL' OR available_balance <= balance),
    CONSTRAINT chk_accounts_daily_limit_positive CHECK (daily_transfer_limit > 0.0000),
    CONSTRAINT chk_accounts_version_positive CHECK (version >= 1)
);

-- -----------------------------------------------------------------------------
-- 6. Saved Beneficiaries Table (3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS beneficiaries (
    beneficiary_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id             UUID NOT NULL,
    bank_code               VARCHAR(16) NOT NULL,
    account_number          CHAR(10) NOT NULL,
    beneficiary_name        VARCHAR(160) NOT NULL,
    currency                currency_code NOT NULL DEFAULT 'NGN',
    is_whitelisted          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_beneficiaries_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON DELETE CASCADE,
    CONSTRAINT uq_beneficiaries_customer_bank_acct
        UNIQUE (customer_id, bank_code, account_number)
);

-- -----------------------------------------------------------------------------
-- 7. Payment Cards Table (PCI-DSS v4.0 Masked & Tokenised, 3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payment_cards (
    card_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id              UUID NOT NULL,
    card_token              CHAR(64) NOT NULL,
    masked_pan              VARCHAR(19) NOT NULL,
    card_scheme             VARCHAR(20) NOT NULL,
    expiry_month            SMALLINT NOT NULL,
    expiry_year             SMALLINT NOT NULL,
    spending_limit          NUMERIC(19, 4) NOT NULL DEFAULT 1000000.0000,
    status                  VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_payment_cards_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT uq_payment_cards_token UNIQUE (card_token),
    CONSTRAINT chk_payment_cards_masked_pan
        CHECK (masked_pan ~ '^XXXX-XXXX-XXXX-[0-9]{4}$'),
    CONSTRAINT chk_payment_cards_expiry_month
        CHECK (expiry_month BETWEEN 1 AND 12),
    CONSTRAINT chk_payment_cards_status
        CHECK (status IN ('ACTIVE', 'BLOCKED', 'EXPIRED'))
);

-- -----------------------------------------------------------------------------
-- 8. Transactions Table (Transfers, Deposits, Withdrawals, Reversals, 3NF/BCNF)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key         VARCHAR(128) NOT NULL,
    reference_code          VARCHAR(40) NOT NULL,
    source_account_id       UUID NOT NULL,
    destination_account_id  UUID NOT NULL,
    transaction_type        transaction_type NOT NULL,
    status                  transaction_status NOT NULL DEFAULT 'PENDING',
    amount                  NUMERIC(19, 4) NOT NULL,
    fee_amount              NUMERIC(19, 4) NOT NULL DEFAULT 0.0000,
    currency                currency_code NOT NULL DEFAULT 'NGN',
    exchange_rate           NUMERIC(19, 6) NOT NULL DEFAULT 1.000000,
    parent_transaction_id   UUID,
    channel                 VARCHAR(32) NOT NULL DEFAULT 'API',
    narration               VARCHAR(255) NOT NULL,
    failure_reason          VARCHAR(255),
    metadata_json           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    settled_at              TIMESTAMPTZ,

    CONSTRAINT fk_transactions_source_account
        FOREIGN KEY (source_account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT fk_transactions_destination_account
        FOREIGN KEY (destination_account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT fk_transactions_parent_transaction
        FOREIGN KEY (parent_transaction_id) REFERENCES transactions (transaction_id) ON DELETE RESTRICT,
    CONSTRAINT uq_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_transactions_reference_code UNIQUE (reference_code),
    CONSTRAINT chk_transactions_distinct_accounts
        CHECK (source_account_id <> destination_account_id),
    CONSTRAINT chk_transactions_positive_amount
        CHECK (amount > 0.0000 AND fee_amount >= 0.0000 AND exchange_rate > 0.000000)
);

-- -----------------------------------------------------------------------------
-- 9. Double-Entry Ledger Entries Table (Append-Only Zero-Sum Invariant)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id          UUID NOT NULL,
    account_id              UUID NOT NULL,
    direction               entry_direction NOT NULL,
    amount                  NUMERIC(19, 4) NOT NULL,
    currency                currency_code NOT NULL,
    balance_before          NUMERIC(19, 4) NOT NULL,
    balance_after           NUMERIC(19, 4) NOT NULL,
    entry_sequence          SMALLINT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_ledger_entries_transaction
        FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id) ON DELETE RESTRICT,
    CONSTRAINT fk_ledger_entries_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT uq_ledger_entries_tx_seq UNIQUE (transaction_id, entry_sequence),
    CONSTRAINT chk_ledger_entries_positive_amount CHECK (amount > 0.0000),
    CONSTRAINT chk_ledger_entries_balance_math CHECK (
        (direction = 'DEBIT' AND balance_after = balance_before - amount)
        OR (direction = 'CREDIT' AND balance_after = balance_before + amount)
    )
);

-- -----------------------------------------------------------------------------
-- 10. Cryptographic Hash-Chained Transaction Audit Logs Table (Append-Only)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_audit_logs (
    log_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id          UUID,
    account_id              UUID,
    actor_id                VARCHAR(80) NOT NULL,
    actor_role              VARCHAR(40) NOT NULL,
    event_type              VARCHAR(64) NOT NULL,
    ip_address              VARCHAR(45) NOT NULL DEFAULT '127.0.0.1',
    previous_state          JSONB,
    new_state               JSONB NOT NULL,
    prev_hash               CHAR(64) NOT NULL,
    curr_hash               CHAR(64) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_audit_logs_transaction
        FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id) ON DELETE RESTRICT,
    CONSTRAINT fk_audit_logs_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT uq_audit_logs_curr_hash UNIQUE (curr_hash)
);

-- -----------------------------------------------------------------------------
-- 11. Real-Time Fraud & AML Alerts Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id          UUID NOT NULL,
    account_id              UUID NOT NULL,
    rule_code               VARCHAR(64) NOT NULL,
    severity                risk_severity NOT NULL,
    risk_score              NUMERIC(5, 2) NOT NULL,
    details                 VARCHAR(512) NOT NULL,
    is_resolved             BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_by             VARCHAR(80),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at             TIMESTAMPTZ,

    CONSTRAINT fk_fraud_alerts_transaction
        FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fraud_alerts_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT chk_fraud_alerts_score CHECK (risk_score >= 0.00 AND risk_score <= 100.00)
);

-- -----------------------------------------------------------------------------
-- 12. Immutability & Double-Entry Integrity Triggers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_prevent_immutable_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Immutable financial table (%) does not permit % operations. Use compensating contra-entries.',
        TG_TABLE_NAME, TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS trg_immutable_ledger_entries ON ledger_entries;
CREATE TRIGGER trg_immutable_ledger_entries
BEFORE UPDATE OR DELETE ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION fn_prevent_immutable_mutation();

DROP TRIGGER IF EXISTS trg_immutable_audit_logs ON transaction_audit_logs;
CREATE TRIGGER trg_immutable_audit_logs
BEFORE UPDATE OR DELETE ON transaction_audit_logs
FOR EACH ROW EXECUTE FUNCTION fn_prevent_immutable_mutation();

COMMIT;
