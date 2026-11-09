-- =============================================================================
-- GPMorgan Forge: Seed Dataset for Core Banking & Ledger Verification
-- Portfolio 1: Database Design and Modelling
-- Target Engine: PostgreSQL 18
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Seed Customers (Institutional GL Owner, Corporate & Retail Customers)
-- -----------------------------------------------------------------------------
INSERT INTO customers (
    customer_id, external_ref, full_name, email, phone_e164,
    segment, kyc_tier, bvn_sha256, country_code, risk_rating
) VALUES
    (
        '10000000-0000-4000-8000-000000000001',
        'CUST-GL-TREASURY',
        'GPMorgan Forge Treasury & Settlement GL',
        'treasury.gl@gpmorganforge.internal',
        '+2348000000001',
        'INSTITUTIONAL',
        'TIER_3',
        encode(digest('BVN-GL-00000000001', 'sha256'), 'hex'),
        'NG',
        1.00
    ),
    (
        '10000000-0000-4000-8000-000000000002',
        'CUST-NG-00102',
        'Adebayo Ogunlesi Ventures Ltd',
        'treasury@ogunlesiventures.ng',
        '+2348031112202',
        'CORPORATE',
        'TIER_3',
        encode(digest('BVN-NG-22334455662', 'sha256'), 'hex'),
        'NG',
        12.50
    ),
    (
        '10000000-0000-4000-8000-000000000003',
        'CUST-NG-00103',
        'Chidinma Okafor',
        'chidinma.okafor@example.ng',
        '+2348092223303',
        'RETAIL',
        'TIER_3',
        encode(digest('BVN-NG-22334455663', 'sha256'), 'hex'),
        'NG',
        8.00
    ),
    (
        '10000000-0000-4000-8000-000000000004',
        'CUST-NG-00104',
        'Ibrahim Danjuma',
        'ibrahim.danjuma@example.ng',
        '+2348053334404',
        'SME',
        'TIER_2',
        encode(digest('BVN-NG-22334455664', 'sha256'), 'hex'),
        'NG',
        18.75
    ),
    (
        '10000000-0000-4000-8000-000000000005',
        'CUST-GB-00105',
        'Tunde Bakare Global Remit',
        'settlements@bakareremit.co.uk',
        '+447700900105',
        'CORPORATE',
        'TIER_3',
        encode(digest('BVN-GB-22334455665', 'sha256'), 'hex'),
        'GB',
        22.00
    )
ON CONFLICT (customer_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 2. Seed KYC Verification Records
-- -----------------------------------------------------------------------------
INSERT INTO kyc_records (
    kyc_id, customer_id, document_type, document_ref_hash,
    issuing_country, verification_status, verified_by, expires_at
) VALUES
    (
        '20000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000002',
        'CAC_CERTIFICATE_AND_TIN',
        encode(digest('DOC-CAC-RC109283', 'sha256'), 'hex'),
        'NG',
        'VERIFIED',
        'compliance.officer.01',
        '2030-12-31 23:59:59+00'
    ),
    (
        '20000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000003',
        'NIN_PASSPORT_BIOMETRIC',
        encode(digest('DOC-NIN-7788990011', 'sha256'), 'hex'),
        'NG',
        'VERIFIED',
        'compliance.officer.01',
        '2029-06-30 23:59:59+00'
    ),
    (
        '20000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000004',
        'NIN_SLIP_TIER2',
        encode(digest('DOC-NIN-4455667788', 'sha256'), 'hex'),
        'NG',
        'VERIFIED',
        'compliance.officer.02',
        '2028-12-31 23:59:59+00'
    )
ON CONFLICT (kyc_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 3. Seed Regulatory Consent Records (NDPA 2023 & GDPR)
-- -----------------------------------------------------------------------------
INSERT INTO consent_records (
    consent_id, customer_id, regulation_code, purpose_code, consent_granted, ip_address
) VALUES
    (
        '25000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000002',
        'NDPA_2023',
        'CORE_BANKING_AND_AML_PROCESSING',
        TRUE,
        '102.89.32.10'
    ),
    (
        '25000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000003',
        'NDPA_2023',
        'CORE_BANKING_AND_AML_PROCESSING',
        TRUE,
        '102.89.34.19'
    ),
    (
        '25000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000005',
        'GDPR',
        'CROSS_BORDER_SETTLEMENT_PROCESSING',
        TRUE,
        '51.140.22.88'
    )
ON CONFLICT (consent_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 4. Seed Accounts (Settlement GL + Customer Checking/Savings Accounts)
-- -----------------------------------------------------------------------------
INSERT INTO accounts (
    account_id, customer_id, account_number, account_name,
    account_type, currency, balance, available_balance, daily_transfer_limit, status, version
) VALUES
    (
        '30000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        '1000000001',
        'GPMorgan NGN Central Settlement GL',
        'SETTLEMENT_GL',
        'NGN',
        -16500000.0000,
        -16500000.0000,
        999999999999.0000,
        'ACTIVE',
        4
    ),
    (
        '30000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000002',
        '2000000002',
        'Adebayo Ogunlesi Ventures Corporate Operating',
        'CHECKING',
        'NGN',
        12250000.0000,
        12250000.0000,
        100000000.0000,
        'ACTIVE',
        3
    ),
    (
        '30000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000003',
        '2000000003',
        'Chidinma Okafor Primary Checking',
        'CHECKING',
        'NGN',
        2750000.0000,
        2750000.0000,
        5000000.0000,
        'ACTIVE',
        3
    ),
    (
        '30000000-0000-4000-8000-000000000004',
        '10000000-0000-4000-8000-000000000004',
        '2000000004',
        'Ibrahim Danjuma SME Savings',
        'SAVINGS',
        'NGN',
        1500000.0000,
        1500000.0000,
        2000000.0000,
        'ACTIVE',
        2
    )
ON CONFLICT (account_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 5. Seed Payment Cards (PCI-DSS Masked PAN)
-- -----------------------------------------------------------------------------
INSERT INTO payment_cards (
    card_id, account_id, card_token, masked_pan, card_scheme,
    expiry_month, expiry_year, spending_limit, status
) VALUES
    (
        '40000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000003',
        encode(digest('TOKEN-CARD-CHIDINMA-01', 'sha256'), 'hex'),
        'XXXX-XXXX-XXXX-4821',
        'VERVE',
        11,
        2028,
        1500000.0000,
        'ACTIVE'
    ),
    (
        '40000000-0000-4000-8000-000000000002',
        '30000000-0000-4000-8000-000000000002',
        encode(digest('TOKEN-CARD-OGUNLESI-01', 'sha256'), 'hex'),
        'XXXX-XXXX-XXXX-9014',
        'MASTERCARD',
        8,
        2029,
        10000000.0000,
        'ACTIVE'
    )
ON CONFLICT (card_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 6. Seed Transactions & Balanced Double-Entry Ledger Entries
--    Note: Total GL Credits/Debits strictly balance to 0.0000 across all legs!
-- -----------------------------------------------------------------------------
INSERT INTO transactions (
    transaction_id, idempotency_key, reference_code,
    source_account_id, destination_account_id, transaction_type, status,
    amount, fee_amount, currency, channel, narration, settled_at
) VALUES
    -- Tx 1: Corporate Deposit of 15,000,000 NGN into Account 2000000002 from Settlement GL
    (
        '50000000-0000-4000-8000-000000000001',
        'IDEMP-DEP-2026-0001',
        'FRG-DEP-20261101-0001',
        '30000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000002',
        'DEPOSIT',
        'SETTLED',
        15000000.0000,
        0.0000,
        'NGN',
        'NIP_INWARD',
        'Initial corporate capital injection via NIP',
        '2026-11-01 09:00:00+00'
    ),
    -- Tx 2: Retail Deposit of 1,500,000 NGN into Account 2000000003 from Settlement GL
    (
        '50000000-0000-4000-8000-000000000002',
        'IDEMP-DEP-2026-0002',
        'FRG-DEP-20261101-0002',
        '30000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000003',
        'DEPOSIT',
        'SETTLED',
        1500000.0000,
        0.0000,
        'NGN',
        'CARD_FUNDING',
        'Retail account funding via Verve card',
        '2026-11-01 10:15:00+00'
    ),
    -- Tx 3: Corporate Transfer of 1,250,000 NGN from Account 2000000002 to Chidinma (2000000003)
    (
        '50000000-0000-4000-8000-000000000003',
        'IDEMP-TRF-2026-0003',
        'FRG-TRF-20261102-0003',
        '30000000-0000-4000-8000-000000000002',
        '30000000-0000-4000-8000-000000000003',
        'TRANSFER',
        'SETTLED',
        1250000.0000,
        0.0000,
        'NGN',
        'API',
        'Vendor consultancy milestone settlement',
        '2026-11-02 14:30:00+00'
    ),
    -- Tx 4: Corporate Transfer of 1,500,000 NGN from Account 2000000002 to Ibrahim (2000000004)
    (
        '50000000-0000-4000-8000-000000000004',
        'IDEMP-TRF-2026-0004',
        'FRG-TRF-20261103-0004',
        '30000000-0000-4000-8000-000000000002',
        '30000000-0000-4000-8000-000000000004',
        'TRANSFER',
        'SETTLED',
        1500000.0000,
        0.0000,
        'NGN',
        'MOBILE_APP',
        'SME logistics supply chain advance',
        '2026-11-03 11:20:00+00'
    )
ON CONFLICT (transaction_id) DO NOTHING;

INSERT INTO ledger_entries (
    entry_id, transaction_id, account_id, direction, amount, currency,
    balance_before, balance_after, entry_sequence, created_at
) VALUES
    -- Legs for Tx 1 (15,000,000 NGN Deposit)
    (
        '60000000-0000-4000-8000-000000000001',
        '50000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000001',
        'DEBIT',
        15000000.0000,
        'NGN',
        0.0000,
        -15000000.0000,
        1,
        '2026-11-01 09:00:00+00'
    ),
    (
        '60000000-0000-4000-8000-000000000002',
        '50000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000002',
        'CREDIT',
        15000000.0000,
        'NGN',
        0.0000,
        15000000.0000,
        2,
        '2026-11-01 09:00:00+00'
    ),
    -- Legs for Tx 2 (1,500,000 NGN Deposit)
    (
        '60000000-0000-4000-8000-000000000003',
        '50000000-0000-4000-8000-000000000002',
        '30000000-0000-4000-8000-000000000001',
        'DEBIT',
        1500000.0000,
        'NGN',
        -15000000.0000,
        -16500000.0000,
        1,
        '2026-11-01 10:15:00+00'
    ),
    (
        '60000000-0000-4000-8000-000000000004',
        '50000000-0000-4000-8000-000000000002',
        '30000000-0000-4000-8000-000000000003',
        'CREDIT',
        1500000.0000,
        'NGN',
        0.0000,
        1500000.0000,
        2,
        '2026-11-01 10:15:00+00'
    ),
    -- Legs for Tx 3 (1,250,000 NGN Transfer: 2000000002 -> 2000000003)
    (
        '60000000-0000-4000-8000-000000000005',
        '50000000-0000-4000-8000-000000000003',
        '30000000-0000-4000-8000-000000000002',
        'DEBIT',
        1250000.0000,
        'NGN',
        15000000.0000,
        13750000.0000,
        1,
        '2026-11-02 14:30:00+00'
    ),
    (
        '60000000-0000-4000-8000-000000000006',
        '50000000-0000-4000-8000-000000000003',
        '30000000-0000-4000-8000-000000000003',
        'CREDIT',
        1250000.0000,
        'NGN',
        1500000.0000,
        2750000.0000,
        2,
        '2026-11-02 14:30:00+00'
    ),
    -- Legs for Tx 4 (1,500,000 NGN Transfer: 2000000002 -> 2000000004)
    (
        '60000000-0000-4000-8000-000000000007',
        '50000000-0000-4000-8000-000000000004',
        '30000000-0000-4000-8000-000000000002',
        'DEBIT',
        1500000.0000,
        'NGN',
        13750000.0000,
        12250000.0000,
        1,
        '2026-11-03 11:20:00+00'
    ),
    (
        '60000000-0000-4000-8000-000000000008',
        '50000000-0000-4000-8000-000000000004',
        '30000000-0000-4000-8000-000000000004',
        'CREDIT',
        1500000.0000,
        'NGN',
        0.0000,
        1500000.0000,
        2,
        '2026-11-03 11:20:00+00'
    )
ON CONFLICT (entry_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7. Seed Whitelisted Beneficiaries
-- -----------------------------------------------------------------------------
INSERT INTO beneficiaries (
    beneficiary_id, customer_id, bank_code, account_number,
    beneficiary_name, currency, is_whitelisted
) VALUES
    (
        '70000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000002',
        '058',
        '2000000003',
        'Chidinma Okafor',
        'NGN',
        TRUE
    ),
    (
        '70000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000002',
        '044',
        '2000000004',
        'Ibrahim Danjuma SME Logistics',
        'NGN',
        TRUE
    )
ON CONFLICT (beneficiary_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 8. Seed Cryptographic Hash-Chained Transaction Audit Logs
--    Uses canonical sorted-key JSON payload matching build_audit_hash() in models.py
-- -----------------------------------------------------------------------------
INSERT INTO transaction_audit_logs (
    transaction_id, account_id, actor_id, actor_role, event_type,
    previous_state, new_state, prev_hash, curr_hash
) VALUES
    (
        '50000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000002',
        'forge_core_service',
        'SYSTEM',
        'TRANSACTION_DEPOSIT_SETTLED',
        '{"balance":"0.0000"}'::jsonb,
        '{"balance":"15000000.0000"}'::jsonb,
        repeat('0', 64),
        encode(
            digest(
                '{"account_id":"30000000-0000-4000-8000-000000000002","actor_id":"forge_core_service","event_type":"TRANSACTION_DEPOSIT_SETTLED","new_state":"{\"balance\":\"15000000.0000\"}","prev_hash":"'
                || repeat('0', 64)
                || '","transaction_id":"50000000-0000-4000-8000-000000000001"}',
                'sha256'
            ),
            'hex'
        )
    ),
    (
        '50000000-0000-4000-8000-000000000003',
        '30000000-0000-4000-8000-000000000002',
        'forge_core_service',
        'SYSTEM',
        'TRANSACTION_TRANSFER_SETTLED',
        '{"balance":"15000000.0000"}'::jsonb,
        '{"balance":"13750000.0000"}'::jsonb,
        encode(
            digest(
                '{"account_id":"30000000-0000-4000-8000-000000000002","actor_id":"forge_core_service","event_type":"TRANSACTION_DEPOSIT_SETTLED","new_state":"{\"balance\":\"15000000.0000\"}","prev_hash":"'
                || repeat('0', 64)
                || '","transaction_id":"50000000-0000-4000-8000-000000000001"}',
                'sha256'
            ),
            'hex'
        ),
        encode(
            digest(
                '{"account_id":"30000000-0000-4000-8000-000000000002","actor_id":"forge_core_service","event_type":"TRANSACTION_TRANSFER_SETTLED","new_state":"{\"balance\":\"13750000.0000\"}","prev_hash":"'
                || encode(
                    digest(
                        '{"account_id":"30000000-0000-4000-8000-000000000002","actor_id":"forge_core_service","event_type":"TRANSACTION_DEPOSIT_SETTLED","new_state":"{\"balance\":\"15000000.0000\"}","prev_hash":"'
                        || repeat('0', 64)
                        || '","transaction_id":"50000000-0000-4000-8000-000000000001"}',
                        'sha256'
                    ),
                    'hex'
                )
                || '","transaction_id":"50000000-0000-4000-8000-000000000003"}',
                'sha256'
            ),
            'hex'
        )
    )
ON CONFLICT (curr_hash) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 9. Seed Real-Time AML Fraud Alerts
-- -----------------------------------------------------------------------------
INSERT INTO fraud_alerts (
    alert_id, transaction_id, account_id, rule_code,
    severity, risk_score, details, is_resolved
) VALUES
    (
        '80000000-0000-4000-8000-000000000001',
        '50000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000002',
        'CBN_LARGE_VALUE_THRESHOLD',
        'HIGH',
        82.50,
        'Single inward NIP deposit exceeds 10,000,000.0000 NGN statutory reporting threshold.',
        FALSE
    )
ON CONFLICT (alert_id) DO NOTHING;

COMMIT;
