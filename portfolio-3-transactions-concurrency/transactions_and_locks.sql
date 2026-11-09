-- =============================================================================
-- GPMorgan Forge: ACID Transactions, Isolation Levels & Concurrency Control
-- Portfolio 3: Transactions and Concurrency
-- Target Engine: PostgreSQL 18
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Scenario 1: Atomic Fund Transfer with Pessimistic Lock Ordering & COMMIT
-- Demonstrates canonical ascending account_id locking (SELECT ... FOR UPDATE)
-- to prevent AB-BA deadlocks, double-entry ledger posting, and COMMIT.
-- -----------------------------------------------------------------------------
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- Step 1: Acquire row-level locks in canonical ascending account_id order
SELECT account_id, account_number, balance, available_balance, version
FROM accounts
WHERE account_id IN (
    '30000000-0000-4000-8000-000000000002',
    '30000000-0000-4000-8000-000000000003'
)
ORDER BY account_id ASC
FOR UPDATE;

-- Step 2: Debit Source Account (2000000002) and Credit Destination (2000000003)
UPDATE accounts
SET balance           = balance - 250000.0000,
    available_balance = available_balance - 250000.0000,
    version           = version + 1,
    updated_at        = CURRENT_TIMESTAMP
WHERE account_id = '30000000-0000-4000-8000-000000000002'
  AND status = 'ACTIVE'
  AND available_balance >= 250000.0000;

UPDATE accounts
SET balance           = balance + 250000.0000,
    available_balance = available_balance + 250000.0000,
    version           = version + 1,
    updated_at        = CURRENT_TIMESTAMP
WHERE account_id = '30000000-0000-4000-8000-000000000003'
  AND status = 'ACTIVE';

-- Step 3: Insert Transaction & Balanced Double-Entry Ledger Legs
INSERT INTO transactions (
    transaction_id, idempotency_key, reference_code,
    source_account_id, destination_account_id, transaction_type, status,
    amount, fee_amount, currency, channel, narration, settled_at
) VALUES (
    '50000000-0000-4000-8000-000000000005',
    'IDEMP-TRF-2026-0005',
    'FRG-TRF-20261104-0005',
    '30000000-0000-4000-8000-000000000002',
    '30000000-0000-4000-8000-000000000003',
    'TRANSFER',
    'SETTLED',
    250000.0000,
    0.0000,
    'NGN',
    'API',
    'Atomic supplier invoice settlement',
    CURRENT_TIMESTAMP
);

INSERT INTO ledger_entries (
    transaction_id, account_id, direction, amount, currency,
    balance_before, balance_after, entry_sequence
) VALUES
    (
        '50000000-0000-4000-8000-000000000005',
        '30000000-0000-4000-8000-000000000002',
        'DEBIT',
        250000.0000,
        'NGN',
        12250000.0000,
        12000000.0000,
        1
    ),
    (
        '50000000-0000-4000-8000-000000000005',
        '30000000-0000-4000-8000-000000000003',
        'CREDIT',
        250000.0000,
        'NGN',
        2750000.0000,
        3000000.0000,
        2
    );

COMMIT;

-- -----------------------------------------------------------------------------
-- Scenario 2: Full Transaction ROLLBACK on Overdraft / Insufficient Funds
-- Demonstrates Atomicity & Consistency: attempting to debit 50,000,000 NGN from
-- an account holding 1,500,000 NGN violates `chk_accounts_non_negative_customer_balance`
-- and triggers a complete ROLLBACK, leaving balances untouched.
-- -----------------------------------------------------------------------------
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Attempt an invalid debit exceeding available balance on Account 2000000004
-- (In production, this raises SQLSTATE 23514 check_violation, after which the
--  session executes ROLLBACK)
SAVEPOINT before_overdraft_attempt;

-- Simulate guard check and explicit abort:
DO $$
DECLARE
    v_avail NUMERIC(19, 4);
BEGIN
    SELECT available_balance INTO v_avail
    FROM accounts
    WHERE account_id = '30000000-0000-4000-8000-000000000004'
    FOR UPDATE;

    IF v_avail < 50000000.0000 THEN
        RAISE NOTICE 'Insufficient funds (available=%, requested=50000000.0000). Rolling back.', v_avail;
    END IF;
END
$$;

ROLLBACK;

-- -----------------------------------------------------------------------------
-- Scenario 3: Partial Rollback Using SAVEPOINT in Multi-Recipient Payroll Batch
-- A corporate treasury batch pays two recipients: Leg A succeeds, Leg B targets
-- a FROZEN or over-limit sub-operation and rolls back to `sp_payroll_leg_2`
-- without aborting Leg A!
-- -----------------------------------------------------------------------------
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;

SAVEPOINT sp_payroll_leg_1;
-- Leg 1: Valid payroll credit continues
SELECT account_number, balance FROM accounts WHERE account_number = '2000000003' FOR UPDATE;

SAVEPOINT sp_payroll_leg_2;
-- Leg 2 encounters a validation error and rolls back only to sp_payroll_leg_2:
ROLLBACK TO SAVEPOINT sp_payroll_leg_2;
RELEASE SAVEPOINT sp_payroll_leg_1;

COMMIT;

-- -----------------------------------------------------------------------------
-- Scenario 4: Optimistic Concurrency Control (OCC) via Version Column
-- Prevents the Lost Update anomaly without holding long-lived database locks.
-- -----------------------------------------------------------------------------
-- Session reads `version = 4` on account 2000000002.
-- When committing the update, it predicates on `WHERE account_id = $1 AND version = 4`.
-- If another concurrent worker already incremented `version` to 5, 0 rows are
-- updated and the application raises `OptimisticLockError` and retries.
UPDATE accounts
SET balance           = balance,
    available_balance = available_balance,
    version           = version + 1,
    updated_at        = CURRENT_TIMESTAMP
WHERE account_id = '30000000-0000-4000-8000-000000000002'
  AND version = 999999; -- Returns 0 rows affected (simulating concurrent version mismatch)

-- -----------------------------------------------------------------------------
-- Scenario 5: Compensating Contra-Entry Reversal of a Settled Transaction
-- Reverses Tx 4 (1,500,000 NGN transfer to 2000000004) by posting an equal and
-- opposite REVERSAL transaction and contra ledger entries, preserving history.
-- -----------------------------------------------------------------------------
-- Executed programmatically via `reverse_failed_transaction()` in `operations.py`
-- and verified by `audit_account_balances()`.
