"""Integration tests for core banking onboarding, double-entry transfers, reversals, and audits."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from gpmorgan_forge.distributed_privacy import DataPrivacyEngine
from gpmorgan_forge.models import (
    AccountStatus,
    AccountType,
    CurrencyCode,
    CustomerSegment,
    FraudAlert,
    KYCTierLevel,
    Transaction,
    TransactionAuditLog,
    TransactionStatus,
)
from gpmorgan_forge.operations import (
    AccountStateError,
    BankingOperationError,
    DailyLimitExceededError,
    InsufficientFundsError,
    OptimisticLockError,
    ReversalError,
    _lock_accounts_canonically,
    audit_account_balances,
    create_account,
    issue_payment_card,
    onboard_customer,
    process_deposit,
    process_withdrawal,
    record_consent,
    reverse_failed_transaction,
    set_account_status,
    transfer_funds,
    verify_kyc_document,
    whitelist_beneficiary,
)


@pytest.mark.integration
def test_onboarding_kyc_consent_cards_beneficiaries_and_erasure(db_session: Session) -> None:
    """Test customer onboarding, KYC verification, consent tracking, cards, and NDPA erasure."""
    cust = onboard_customer(
        db_session,
        external_ref="CUST-TEST-001",
        full_name="Adaobi Nwosu",
        email="ADAOBI@EXAMPLE.NG ",
        phone_e164=" +2348030001111 ",
        raw_bvn="22110099887",
        segment=CustomerSegment.RETAIL,
        kyc_tier=KYCTierLevel.TIER_1,
    )
    assert cust.email == "adaobi@example.ng"
    assert cust.kyc_tier == KYCTierLevel.TIER_1.value

    kyc_rec = verify_kyc_document(
        db_session,
        customer_id=cust.customer_id,
        document_type="NIN_SLIP",
        raw_document_ref="NIN-99887766",
        verified_by="kyc.verifier",
        upgraded_tier=KYCTierLevel.TIER_3,
    )
    assert kyc_rec.verification_status == "VERIFIED"
    assert cust.kyc_tier == KYCTierLevel.TIER_3.value

    with pytest.raises(BankingOperationError, match="not found"):
        verify_kyc_document(
            db_session,
            customer_id="missing-customer-id",
            document_type="PASSPORT",
            raw_document_ref="A12345",
            verified_by="kyc.verifier",
        )

    c_granted = record_consent(
        db_session,
        customer_id=cust.customer_id,
        regulation_code="NDPA_2023",
        purpose_code="MARKETING",
        consent_granted=True,
        ip_address="102.89.1.1",
    )
    assert c_granted.revoked_at is None

    c_revoked = record_consent(
        db_session,
        customer_id=cust.customer_id,
        regulation_code="GDPR",
        purpose_code="THIRD_PARTY_ANALYTICS",
        consent_granted=False,
        ip_address="102.89.1.2",
    )
    assert c_revoked.revoked_at is not None

    with pytest.raises(BankingOperationError, match="10-digit NUBAN"):
        create_account(
            db_session,
            customer_id=cust.customer_id,
            account_number="12345",
            account_name="Invalid Short Account",
        )

    acc = create_account(
        db_session,
        customer_id=cust.customer_id,
        account_number="2000001234",
        account_name="Adaobi Primary Checking",
    )

    with pytest.raises(BankingOperationError, match="Invalid card PAN"):
        issue_payment_card(
            db_session,
            account_id=acc.account_id,
            raw_pan="1234-ABCD",
            card_scheme="VISA",
            expiry_month=12,
            expiry_year=2028,
        )

    card = issue_payment_card(
        db_session,
        account_id=acc.account_id,
        raw_pan="4111-2222-3333-4444",
        card_scheme="visa",
        expiry_month=12,
        expiry_year=2028,
    )
    assert card.masked_pan == "XXXX-XXXX-XXXX-4444"
    assert card.card_scheme == "VISA"

    ben = whitelist_beneficiary(
        db_session,
        customer_id=cust.customer_id,
        bank_code="058",
        account_number="0123456789",
        beneficiary_name="Counterparty Ltd",
    )
    assert ben.is_whitelisted is True

    updated_acc = set_account_status(
        db_session,
        account_id=acc.account_id,
        new_status=AccountStatus.FROZEN,
    )
    assert updated_acc.status == AccountStatus.FROZEN.value

    with pytest.raises(BankingOperationError, match="not found"):
        set_account_status(
            db_session,
            account_id="non-existent-account",
            new_status=AccountStatus.ACTIVE,
        )

    anon_cust = DataPrivacyEngine.anonymise_customer_record(
        db_session,
        customer_id=cust.customer_id,
    )
    assert anon_cust.is_anonymised is True
    assert anon_cust.full_name.startswith("REDACTED_")

    with pytest.raises(ValueError, match="not found"):
        DataPrivacyEngine.anonymise_customer_record(db_session, customer_id="missing-id")


@pytest.mark.integration
def test_transfers_deposits_withdrawals_reversals_and_audits(db_session: Session) -> None:
    """Verify atomic transfers, guardrails, high-value alerts, reversals, and audit integrity."""
    cust = onboard_customer(
        db_session,
        external_ref="CUST-CORE-01",
        full_name="Core Tester",
        email="core@example.ng",
        phone_e164="+2348055550001",
        raw_bvn="22110099001",
    )
    gl_acc = create_account(
        db_session,
        customer_id=cust.customer_id,
        account_number="1000000099",
        account_name="Settlement GL",
        account_type=AccountType.SETTLEMENT_GL,
        currency=CurrencyCode.NGN,
        daily_transfer_limit=Decimal("999999999.0000"),
    )
    acc_a = create_account(
        db_session,
        customer_id=cust.customer_id,
        account_number="2000000101",
        account_name="Checking A",
        currency=CurrencyCode.NGN,
        daily_transfer_limit=Decimal("20000000.0000"),
    )
    acc_b = create_account(
        db_session,
        customer_id=cust.customer_id,
        account_number="2000000102",
        account_name="Checking B",
        currency=CurrencyCode.NGN,
        daily_transfer_limit=Decimal("500000.0000"),
    )
    acc_usd = create_account(
        db_session,
        customer_id=cust.customer_id,
        account_number="3000000103",
        account_name="USD Treasury",
        currency=CurrencyCode.USD,
    )

    with pytest.raises(BankingOperationError, match="must be distinct"):
        _lock_accounts_canonically(db_session, acc_a.account_id, acc_a.account_id)

    with pytest.raises(BankingOperationError, match="does not exist"):
        _lock_accounts_canonically(db_session, acc_a.account_id, "missing-acc-id")

    dep_tx = process_deposit(
        db_session,
        idempotency_key="IDEMP-DEP-LARGE",
        reference_code="REF-DEP-LARGE",
        settlement_gl_account_id=gl_acc.account_id,
        customer_account_id=acc_a.account_id,
        amount=Decimal("12000000.0000"),
    )
    assert dep_tx.status == TransactionStatus.SETTLED.value
    alerts = list(db_session.scalars(select(FraudAlert)).all())
    assert len(alerts) == 1
    assert alerts[0].rule_code == "CBN_LARGE_VALUE_THRESHOLD"

    replay_tx = process_deposit(
        db_session,
        idempotency_key="IDEMP-DEP-LARGE",
        reference_code="REF-DEP-LARGE-DUP",
        settlement_gl_account_id=gl_acc.account_id,
        customer_account_id=acc_a.account_id,
        amount=Decimal("12000000.0000"),
    )
    assert replay_tx.transaction_id == dep_tx.transaction_id

    with pytest.raises(BankingOperationError, match="must be positive"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-ZERO",
            reference_code="REF-ZERO",
            source_account_id=acc_a.account_id,
            destination_account_id=acc_b.account_id,
            amount=Decimal("0.0000"),
            narration="Zero transfer",
        )

    with pytest.raises(OptimisticLockError, match="Optimistic concurrency conflict"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-OCC-FAIL",
            reference_code="REF-OCC-FAIL",
            source_account_id=acc_a.account_id,
            destination_account_id=acc_b.account_id,
            amount=Decimal("1000.0000"),
            narration="Stale version",
            expected_source_version=999,
        )

    with pytest.raises(AccountStateError, match="Currency mismatch"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-CURR-MISMATCH",
            reference_code="REF-CURR-MISMATCH",
            source_account_id=acc_a.account_id,
            destination_account_id=acc_usd.account_id,
            amount=Decimal("100.0000"),
            narration="Cross currency without FX",
        )

    set_account_status(db_session, account_id=acc_b.account_id, new_status=AccountStatus.FROZEN)
    with pytest.raises(AccountStateError, match="Destination account"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-DST-FROZEN",
            reference_code="REF-DST-FROZEN",
            source_account_id=acc_a.account_id,
            destination_account_id=acc_b.account_id,
            amount=Decimal("100.0000"),
            narration="Dest frozen",
        )
    set_account_status(db_session, account_id=acc_b.account_id, new_status=AccountStatus.ACTIVE)

    set_account_status(db_session, account_id=acc_a.account_id, new_status=AccountStatus.FROZEN)
    with pytest.raises(AccountStateError, match="Source account"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-SRC-FROZEN",
            reference_code="REF-SRC-FROZEN",
            source_account_id=acc_a.account_id,
            destination_account_id=acc_b.account_id,
            amount=Decimal("100.0000"),
            narration="Source frozen",
        )
    set_account_status(db_session, account_id=acc_a.account_id, new_status=AccountStatus.ACTIVE)

    tx_ab = transfer_funds(
        db_session,
        idempotency_key="IDEMP-AB-1",
        reference_code="REF-AB-1",
        source_account_id=acc_a.account_id,
        destination_account_id=acc_b.account_id,
        amount=Decimal("400000.0000"),
        narration="Valid transfer",
    )

    with pytest.raises(DailyLimitExceededError, match="exceeds daily limit"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-LIMIT-B",
            reference_code="REF-LIMIT-B",
            source_account_id=acc_b.account_id,
            destination_account_id=acc_a.account_id,
            amount=Decimal("600000.0000"),
            narration="Over daily limit",
        )

    with pytest.raises(InsufficientFundsError, match="insufficient funds"):
        transfer_funds(
            db_session,
            idempotency_key="IDEMP-INSUFF-B",
            reference_code="REF-INSUFF-B",
            source_account_id=acc_b.account_id,
            destination_account_id=acc_a.account_id,
            amount=Decimal("450000.0000"),
            narration="Over available balance",
        )

    wdr_tx = process_withdrawal(
        db_session,
        idempotency_key="IDEMP-WDR-B",
        reference_code="REF-WDR-B",
        customer_account_id=acc_b.account_id,
        settlement_gl_account_id=gl_acc.account_id,
        amount=Decimal("350000.0000"),
    )
    assert wdr_tx.status == TransactionStatus.SETTLED.value

    with pytest.raises(InsufficientFundsError, match="insufficient funds for clawback"):
        reverse_failed_transaction(
            db_session,
            original_transaction_id=tx_ab.transaction_id,
            reversal_idempotency_key="IDEMP-REV-FAIL-CLAWBACK",
            reversal_reference_code="REF-REV-FAIL-CLAWBACK",
            reason="Clawback test",
        )

    rev_wdr = reverse_failed_transaction(
        db_session,
        original_transaction_id=wdr_tx.transaction_id,
        reversal_idempotency_key="IDEMP-REV-WDR",
        reversal_reference_code="REF-REV-WDR",
        reason="ATM dispense failure",
    )
    assert rev_wdr.status == TransactionStatus.SETTLED.value

    rev_wdr_dup = reverse_failed_transaction(
        db_session,
        original_transaction_id=wdr_tx.transaction_id,
        reversal_idempotency_key="IDEMP-REV-WDR",
        reversal_reference_code="REF-REV-WDR-DUP",
        reason="ATM dispense failure",
    )
    assert rev_wdr_dup.transaction_id == rev_wdr.transaction_id

    with pytest.raises(ReversalError, match="already been reversed"):
        reverse_failed_transaction(
            db_session,
            original_transaction_id=wdr_tx.transaction_id,
            reversal_idempotency_key="IDEMP-REV-WDR-SECOND",
            reversal_reference_code="REF-REV-WDR-SECOND",
            reason="Double reversal attempt",
        )

    with pytest.raises(ReversalError, match="not found"):
        reverse_failed_transaction(
            db_session,
            original_transaction_id="missing-tx-id",
            reversal_idempotency_key="IDEMP-REV-MISSING",
            reversal_reference_code="REF-REV-MISSING",
            reason="Missing tx",
        )

    pending_tx = Transaction(
        idempotency_key="IDEMP-PENDING-RAW",
        reference_code="REF-PENDING-RAW",
        source_account_id=acc_a.account_id,
        destination_account_id=acc_b.account_id,
        transaction_type="TRANSFER",
        status=TransactionStatus.PENDING.value,
        amount=Decimal("500.0000"),
        currency="NGN",
        narration="Pending tx",
    )
    db_session.add(pending_tx)
    db_session.flush()

    with pytest.raises(ReversalError, match="Only SETTLED transactions"):
        reverse_failed_transaction(
            db_session,
            original_transaction_id=pending_tx.transaction_id,
            reversal_idempotency_key="IDEMP-REV-PENDING",
            reversal_reference_code="REF-REV-PENDING",
            reason="Cannot reverse pending",
        )

    report_ok = audit_account_balances(db_session)
    assert report_ok.is_balanced is True
    assert report_ok.hash_chain_valid is True
    assert report_ok.to_dict()["is_balanced"] is True

    acc_a.balance = Decimal("999999999.0000")
    first_log = db_session.scalar(
        select(TransactionAuditLog).order_by(TransactionAuditLog.log_id.asc()).limit(1)
    )
    assert first_log is not None
    first_log.new_state = '{"tampered": true}'
    db_session.flush()

    report_bad = audit_account_balances(db_session)
    assert report_bad.is_balanced is False
    assert acc_a.account_number in report_bad.mismatched_accounts
    assert report_bad.hash_chain_valid is False
    assert first_log.log_id in report_bad.tampered_log_ids
