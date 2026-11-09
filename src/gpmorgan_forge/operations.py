"""Core banking database operations for GPMorgan Forge (Portfolios 1 & 3).

Implements all essential fintech/banking operations:
- Customer onboarding, KYC verification, and regulatory consent recording
- Multi-currency account provisioning, freezing, and card/beneficiary management
- Deposit processing and withdrawal processing
- Atomic fund transfers with canonical lock ordering, optimistic version checks,
  idempotency key deduplication, and zero-sum double-entry ledger posting
- Failed or disputed transaction reversals via compensating contra-entries
- Comprehensive account balance auditing and SHA-256 audit log chain verification
"""

from __future__ import annotations

import json
from decimal import ROUND_HALF_EVEN, Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from gpmorgan_forge.models import (
    Account,
    AccountStatus,
    AccountType,
    AuditReconciliationReport,
    Beneficiary,
    ConsentRecord,
    CurrencyCode,
    Customer,
    CustomerSegment,
    EntryDirection,
    FraudAlert,
    KYCRecord,
    KYCTierLevel,
    LedgerEntry,
    PaymentCard,
    RiskSeverity,
    Transaction,
    TransactionAuditLog,
    TransactionStatus,
    TransactionType,
    build_audit_hash,
    compute_sha256_hex,
    utc_now,
)

GENESIS_HASH: str = "0" * 64
FOUR_DP: Decimal = Decimal("0.0001")


class BankingOperationError(Exception):
    """Base exception for GPMorgan Forge banking operation errors."""


class InsufficientFundsError(BankingOperationError):
    """Raised when an account lacks sufficient available balance for a debit."""


class AccountStateError(BankingOperationError):
    """Raised when an account is frozen, dormant, closed, or violates currency rules."""


class DailyLimitExceededError(BankingOperationError):
    """Raised when a transfer or withdrawal exceeds the account daily limit."""


class OptimisticLockError(BankingOperationError):
    """Raised when concurrent modification causes a version mismatch."""


class ReversalError(BankingOperationError):
    """Raised when a transaction reversal request violates domain invariants."""


def quantize_money(value: Decimal | int | str) -> Decimal:
    """Quantize a monetary amount to 4 decimal places using banker's rounding."""
    return Decimal(str(value)).quantize(FOUR_DP, rounding=ROUND_HALF_EVEN)


def _append_audit_log(
    session: Session,
    *,
    transaction_id: str | None,
    account_id: str | None,
    actor_id: str,
    actor_role: str,
    event_type: str,
    previous_state: dict[str, object] | None,
    new_state: dict[str, object],
    ip_address: str = "127.0.0.1",
) -> TransactionAuditLog:
    """Append a cryptographically hash-chained record to transaction_audit_logs."""
    last_log = session.scalar(
        select(TransactionAuditLog).order_by(TransactionAuditLog.log_id.desc()).limit(1)
    )
    prev_hash = last_log.curr_hash if last_log is not None else GENESIS_HASH
    prev_json = json.dumps(previous_state, sort_keys=True) if previous_state is not None else None
    new_json = json.dumps(new_state, sort_keys=True)
    curr_hash = build_audit_hash(
        prev_hash=prev_hash,
        transaction_id=transaction_id,
        account_id=account_id,
        actor_id=actor_id,
        event_type=event_type,
        new_state_json=new_json,
    )
    entry = TransactionAuditLog(
        transaction_id=transaction_id,
        account_id=account_id,
        actor_id=actor_id,
        actor_role=actor_role,
        event_type=event_type,
        ip_address=ip_address,
        previous_state=prev_json,
        new_state=new_json,
        prev_hash=prev_hash,
        curr_hash=curr_hash,
    )
    session.add(entry)
    session.flush()
    return entry


def onboard_customer(
    session: Session,
    *,
    external_ref: str,
    full_name: str,
    email: str,
    phone_e164: str,
    raw_bvn: str,
    segment: CustomerSegment = CustomerSegment.RETAIL,
    kyc_tier: KYCTierLevel = KYCTierLevel.TIER_1,
    country_code: str = "NG",
    risk_rating: Decimal = Decimal("10.00"),
) -> Customer:
    """Register a new customer with hashed BVN identifier and audit trail."""
    customer = Customer(
        external_ref=external_ref,
        full_name=full_name,
        email=email.lower().strip(),
        phone_e164=phone_e164.strip(),
        segment=segment.value,
        kyc_tier=kyc_tier.value,
        bvn_sha256=compute_sha256_hex(raw_bvn),
        country_code=country_code.upper(),
        risk_rating=Decimal(str(risk_rating)).quantize(Decimal("0.01")),
    )
    session.add(customer)
    session.flush()
    _append_audit_log(
        session,
        transaction_id=None,
        account_id=None,
        actor_id="onboarding.service",
        actor_role="forge_core_service",
        event_type="CUSTOMER_ONBOARDED",
        previous_state=None,
        new_state={
            "customer_id": customer.customer_id,
            "external_ref": customer.external_ref,
            "segment": customer.segment,
            "kyc_tier": customer.kyc_tier,
        },
    )
    return customer


def verify_kyc_document(
    session: Session,
    *,
    customer_id: str,
    document_type: str,
    raw_document_ref: str,
    verified_by: str,
    upgraded_tier: KYCTierLevel = KYCTierLevel.TIER_3,
    issuing_country: str = "NG",
) -> KYCRecord:
    """Record a verified KYC document and upgrade the customer's KYC tier."""
    customer = session.get(Customer, customer_id)
    if customer is None:
        raise BankingOperationError(f"Customer '{customer_id}' not found.")

    record = KYCRecord(
        customer_id=customer_id,
        document_type=document_type,
        document_ref_hash=compute_sha256_hex(raw_document_ref),
        issuing_country=issuing_country.upper(),
        verification_status="VERIFIED",
        verified_by=verified_by,
    )
    customer.kyc_tier = upgraded_tier.value
    customer.updated_at = utc_now()
    session.add(record)
    session.flush()
    return record


def record_consent(
    session: Session,
    *,
    customer_id: str,
    regulation_code: str,
    purpose_code: str,
    consent_granted: bool,
    ip_address: str,
) -> ConsentRecord:
    """Record a customer privacy consent decision under NDPA 2023 or GDPR."""
    consent = ConsentRecord(
        customer_id=customer_id,
        regulation_code=regulation_code,
        purpose_code=purpose_code,
        consent_granted=consent_granted,
        ip_address=ip_address,
        revoked_at=None if consent_granted else utc_now(),
    )
    session.add(consent)
    session.flush()
    return consent


def create_account(
    session: Session,
    *,
    customer_id: str,
    account_number: str,
    account_name: str,
    account_type: AccountType = AccountType.CHECKING,
    currency: CurrencyCode = CurrencyCode.NGN,
    daily_transfer_limit: Decimal = Decimal("5000000.0000"),
) -> Account:
    """Provision a new multi-currency customer or general ledger account."""
    if len(account_number) != 10 or not account_number.isdigit():
        raise BankingOperationError("Account number must be a 10-digit NUBAN string.")

    account = Account(
        customer_id=customer_id,
        account_number=account_number,
        account_name=account_name,
        account_type=account_type.value,
        currency=currency.value,
        balance=Decimal("0.0000"),
        available_balance=Decimal("0.0000"),
        daily_transfer_limit=quantize_money(daily_transfer_limit),
        status=AccountStatus.ACTIVE.value,
        version=1,
    )
    session.add(account)
    session.flush()
    _append_audit_log(
        session,
        transaction_id=None,
        account_id=account.account_id,
        actor_id="account.service",
        actor_role="forge_core_service",
        event_type="ACCOUNT_CREATED",
        previous_state=None,
        new_state={
            "account_number": account.account_number,
            "account_type": account.account_type,
            "currency": account.currency,
        },
    )
    return account


def issue_payment_card(
    session: Session,
    *,
    account_id: str,
    raw_pan: str,
    card_scheme: str,
    expiry_month: int,
    expiry_year: int,
    spending_limit: Decimal = Decimal("1000000.0000"),
) -> PaymentCard:
    """Issue a tokenised payment card storing only PCI-DSS masked PAN."""
    clean_pan = raw_pan.replace("-", "").replace(" ", "")
    if len(clean_pan) < 12 or not clean_pan.isdigit():
        raise BankingOperationError("Invalid card PAN provided for tokenisation.")

    masked_pan = f"XXXX-XXXX-XXXX-{clean_pan[-4:]}"
    card = PaymentCard(
        account_id=account_id,
        card_token=compute_sha256_hex(f"CARD-TOKEN:{clean_pan}:{expiry_month}:{expiry_year}"),
        masked_pan=masked_pan,
        card_scheme=card_scheme.upper(),
        expiry_month=expiry_month,
        expiry_year=expiry_year,
        spending_limit=quantize_money(spending_limit),
        status="ACTIVE",
    )
    session.add(card)
    session.flush()
    return card


def whitelist_beneficiary(
    session: Session,
    *,
    customer_id: str,
    bank_code: str,
    account_number: str,
    beneficiary_name: str,
    currency: CurrencyCode = CurrencyCode.NGN,
) -> Beneficiary:
    """Register a whitelisted transfer counterparty for a customer."""
    beneficiary = Beneficiary(
        customer_id=customer_id,
        bank_code=bank_code,
        account_number=account_number,
        beneficiary_name=beneficiary_name,
        currency=currency.value,
        is_whitelisted=True,
    )
    session.add(beneficiary)
    session.flush()
    return beneficiary


def set_account_status(
    session: Session,
    *,
    account_id: str,
    new_status: AccountStatus,
    actor_id: str = "risk.controller",
) -> Account:
    """Transition an account status (e.g., ACTIVE -> FROZEN or FROZEN -> ACTIVE)."""
    account = session.get(Account, account_id)
    if account is None:
        raise BankingOperationError(f"Account '{account_id}' not found.")

    old_status = account.status
    account.status = new_status.value
    account.version += 1
    account.updated_at = utc_now()
    session.flush()
    _append_audit_log(
        session,
        transaction_id=None,
        account_id=account.account_id,
        actor_id=actor_id,
        actor_role="forge_compliance_dpo",
        event_type="ACCOUNT_STATUS_CHANGED",
        previous_state={"status": old_status},
        new_state={"status": account.status, "version": account.version},
    )
    return account


def _lock_accounts_canonically(
    session: Session, account_id_a: str, account_id_b: str
) -> tuple[Account, Account]:
    """Acquire row locks in canonical ascending ID order to eliminate deadlocks."""
    if account_id_a == account_id_b:
        raise BankingOperationError("Source and destination accounts must be distinct.")

    ordered_ids = sorted([account_id_a, account_id_b])
    locked_map: dict[str, Account] = {}
    for acc_id in ordered_ids:
        stmt = select(Account).where(Account.account_id == acc_id).with_for_update()
        acc = session.scalar(stmt)
        if acc is None:
            raise BankingOperationError(f"Account '{acc_id}' does not exist.")
        locked_map[acc_id] = acc

    return locked_map[account_id_a], locked_map[account_id_b]


def transfer_funds(
    session: Session,
    *,
    idempotency_key: str,
    reference_code: str,
    source_account_id: str,
    destination_account_id: str,
    amount: Decimal | str | int,
    narration: str,
    fee_amount: Decimal | str | int = Decimal("0.0000"),
    transaction_type: TransactionType = TransactionType.TRANSFER,
    channel: str = "API",
    expected_source_version: int | None = None,
    actor_id: str = "payment.router",
) -> Transaction:
    """Execute an atomic double-entry fund movement between two accounts.

    Supports idempotency deduplication, canonical deadlock-free account locking,
    optimistic concurrency version checking, daily limit validation, and SHA-256
    hash-chained audit logging.
    """
    existing_tx = session.scalar(
        select(Transaction).where(Transaction.idempotency_key == idempotency_key)
    )
    if existing_tx is not None:
        return existing_tx

    quantized_amount = quantize_money(amount)
    quantized_fee = quantize_money(fee_amount)
    if quantized_amount <= Decimal("0.0000") or quantized_fee < Decimal("0.0000"):
        raise BankingOperationError("Transfer amount must be positive and fee non-negative.")

    source_acc, dest_acc = _lock_accounts_canonically(
        session, source_account_id, destination_account_id
    )

    if expected_source_version is not None and source_acc.version != expected_source_version:
        raise OptimisticLockError(
            f"Optimistic concurrency conflict on account {source_acc.account_number}: "
            f"expected version {expected_source_version}, actual {source_acc.version}."
        )

    if source_acc.status != AccountStatus.ACTIVE.value:
        raise AccountStateError(
            f"Source account {source_acc.account_number} is {source_acc.status}."
        )
    if dest_acc.status != AccountStatus.ACTIVE.value:
        raise AccountStateError(
            f"Destination account {dest_acc.account_number} is {dest_acc.status}."
        )
    if source_acc.currency != dest_acc.currency:
        raise AccountStateError(f"Currency mismatch: {source_acc.currency} vs {dest_acc.currency}.")

    total_debit = quantized_amount + quantized_fee
    is_gl_source = source_acc.account_type == AccountType.SETTLEMENT_GL.value

    if not is_gl_source:
        if total_debit > quantize_money(source_acc.daily_transfer_limit):
            raise DailyLimitExceededError(
                f"Debit {total_debit} exceeds daily limit {source_acc.daily_transfer_limit}."
            )
        if quantize_money(source_acc.available_balance) < total_debit:
            raise InsufficientFundsError(
                f"Account {source_acc.account_number} has insufficient funds: "
                f"available={source_acc.available_balance}, required={total_debit}."
            )

    src_before = quantize_money(source_acc.balance)
    dst_before = quantize_money(dest_acc.balance)
    src_after = src_before - total_debit
    dst_after = dst_before + total_debit

    source_acc.balance = src_after
    source_acc.available_balance = quantize_money(source_acc.available_balance) - total_debit
    source_acc.version += 1
    source_acc.updated_at = utc_now()

    dest_acc.balance = dst_after
    dest_acc.available_balance = quantize_money(dest_acc.available_balance) + total_debit
    dest_acc.version += 1
    dest_acc.updated_at = utc_now()

    now = utc_now()
    tx = Transaction(
        idempotency_key=idempotency_key,
        reference_code=reference_code,
        source_account_id=source_acc.account_id,
        destination_account_id=dest_acc.account_id,
        transaction_type=transaction_type.value,
        status=TransactionStatus.SETTLED.value,
        amount=quantized_amount,
        fee_amount=quantized_fee,
        currency=source_acc.currency,
        exchange_rate=Decimal("1.000000"),
        channel=channel,
        narration=narration,
        settled_at=now,
    )
    session.add(tx)
    session.flush()

    debit_leg = LedgerEntry(
        transaction_id=tx.transaction_id,
        account_id=source_acc.account_id,
        direction=EntryDirection.DEBIT.value,
        amount=total_debit,
        currency=source_acc.currency,
        balance_before=src_before,
        balance_after=src_after,
        entry_sequence=1,
        created_at=now,
    )
    credit_leg = LedgerEntry(
        transaction_id=tx.transaction_id,
        account_id=dest_acc.account_id,
        direction=EntryDirection.CREDIT.value,
        amount=total_debit,
        currency=dest_acc.currency,
        balance_before=dst_before,
        balance_after=dst_after,
        entry_sequence=2,
        created_at=now,
    )
    session.add_all([debit_leg, credit_leg])
    session.flush()

    _append_audit_log(
        session,
        transaction_id=tx.transaction_id,
        account_id=source_acc.account_id,
        actor_id=actor_id,
        actor_role="forge_core_service",
        event_type=f"TRANSACTION_{transaction_type.value}_SETTLED",
        previous_state={"src_balance": str(src_before), "dst_balance": str(dst_before)},
        new_state={
            "reference_code": reference_code,
            "amount": str(total_debit),
            "src_balance": str(src_after),
            "dst_balance": str(dst_after),
        },
    )

    if total_debit >= Decimal("10000000.0000"):
        alert = FraudAlert(
            transaction_id=tx.transaction_id,
            account_id=source_acc.account_id,
            rule_code="CBN_LARGE_VALUE_THRESHOLD",
            severity=RiskSeverity.MEDIUM.value,
            risk_score=Decimal("65.00"),
            details=f"High-value transaction of {total_debit} {source_acc.currency} settled.",
        )
        session.add(alert)
        session.flush()

    return tx


def process_deposit(
    session: Session,
    *,
    idempotency_key: str,
    reference_code: str,
    settlement_gl_account_id: str,
    customer_account_id: str,
    amount: Decimal | str | int,
    narration: str = "Customer account deposit funding",
    channel: str = "NIP_INWARD",
) -> Transaction:
    """Fund a customer account by debiting the Settlement GL and crediting the customer."""
    return transfer_funds(
        session,
        idempotency_key=idempotency_key,
        reference_code=reference_code,
        source_account_id=settlement_gl_account_id,
        destination_account_id=customer_account_id,
        amount=amount,
        narration=narration,
        transaction_type=TransactionType.DEPOSIT,
        channel=channel,
        actor_id="deposit.gateway",
    )


def process_withdrawal(
    session: Session,
    *,
    idempotency_key: str,
    reference_code: str,
    customer_account_id: str,
    settlement_gl_account_id: str,
    amount: Decimal | str | int,
    fee_amount: Decimal | str | int = Decimal("0.0000"),
    narration: str = "Customer withdrawal payout",
    channel: str = "ATM_POS",
) -> Transaction:
    """Process a customer withdrawal by debiting the customer and crediting the Settlement GL."""
    return transfer_funds(
        session,
        idempotency_key=idempotency_key,
        reference_code=reference_code,
        source_account_id=customer_account_id,
        destination_account_id=settlement_gl_account_id,
        amount=amount,
        fee_amount=fee_amount,
        narration=narration,
        transaction_type=TransactionType.WITHDRAWAL,
        channel=channel,
        actor_id="withdrawal.gateway",
    )


def reverse_failed_transaction(
    session: Session,
    *,
    original_transaction_id: str,
    reversal_idempotency_key: str,
    reversal_reference_code: str,
    reason: str,
    actor_id: str = "settlement.reconciler",
) -> Transaction:
    """Atomically reverse a settled transaction using compensating double-entry contra-legs.

    Rather than mutating or deleting historical ledger entries, this operation:
    1. Verifies the original transaction is SETTLED and has not already been REVERSED.
    2. Posts a new REVERSAL transaction in the opposite direction (destination -> source).
    3. Marks the original transaction status as REVERSED with the reversal reason.
    4. Emits a cryptographically hash-chained audit log entry.
    """
    existing_rev = session.scalar(
        select(Transaction).where(Transaction.idempotency_key == reversal_idempotency_key)
    )
    if existing_rev is not None:
        return existing_rev

    original_tx = session.get(Transaction, original_transaction_id)
    if original_tx is None:
        raise ReversalError(f"Original transaction '{original_transaction_id}' not found.")

    if original_tx.status == TransactionStatus.REVERSED.value:
        raise ReversalError(
            f"Transaction '{original_tx.reference_code}' has already been reversed."
        )
    if original_tx.status != TransactionStatus.SETTLED.value:
        raise ReversalError(
            f"Only SETTLED transactions require contra-entry reversal; "
            f"current status is '{original_tx.status}'."
        )

    total_original_amount = quantize_money(original_tx.amount) + quantize_money(
        original_tx.fee_amount
    )
    dest_acc, source_acc = _lock_accounts_canonically(
        session, original_tx.destination_account_id, original_tx.source_account_id
    )

    if dest_acc.account_type != AccountType.SETTLEMENT_GL.value and (
        quantize_money(dest_acc.available_balance) < total_original_amount
    ):
        raise InsufficientFundsError(
            f"Cannot reverse transaction {original_tx.reference_code}: destination account "
            f"{dest_acc.account_number} has insufficient funds for clawback."
        )

    dst_before = quantize_money(dest_acc.balance)
    src_before = quantize_money(source_acc.balance)
    dst_after = dst_before - total_original_amount
    src_after = src_before + total_original_amount

    dest_acc.balance = dst_after
    dest_acc.available_balance = quantize_money(dest_acc.available_balance) - total_original_amount
    dest_acc.version += 1
    dest_acc.updated_at = utc_now()

    source_acc.balance = src_after
    source_acc.available_balance = (
        quantize_money(source_acc.available_balance) + total_original_amount
    )
    source_acc.version += 1
    source_acc.updated_at = utc_now()

    original_tx.status = TransactionStatus.REVERSED.value
    original_tx.failure_reason = reason

    now = utc_now()
    reversal_tx = Transaction(
        idempotency_key=reversal_idempotency_key,
        reference_code=reversal_reference_code,
        source_account_id=dest_acc.account_id,
        destination_account_id=source_acc.account_id,
        transaction_type=TransactionType.REVERSAL.value,
        status=TransactionStatus.SETTLED.value,
        amount=quantize_money(original_tx.amount),
        fee_amount=quantize_money(original_tx.fee_amount),
        currency=original_tx.currency,
        exchange_rate=original_tx.exchange_rate,
        parent_transaction_id=original_tx.transaction_id,
        channel="REVERSAL_ENGINE",
        narration=f"REVERSAL of {original_tx.reference_code}: {reason}",
        failure_reason=reason,
        settled_at=now,
    )
    session.add(reversal_tx)
    session.flush()

    contra_debit = LedgerEntry(
        transaction_id=reversal_tx.transaction_id,
        account_id=dest_acc.account_id,
        direction=EntryDirection.DEBIT.value,
        amount=total_original_amount,
        currency=dest_acc.currency,
        balance_before=dst_before,
        balance_after=dst_after,
        entry_sequence=1,
        created_at=now,
    )
    contra_credit = LedgerEntry(
        transaction_id=reversal_tx.transaction_id,
        account_id=source_acc.account_id,
        direction=EntryDirection.CREDIT.value,
        amount=total_original_amount,
        currency=source_acc.currency,
        balance_before=src_before,
        balance_after=src_after,
        entry_sequence=2,
        created_at=now,
    )
    session.add_all([contra_debit, contra_credit])
    session.flush()

    _append_audit_log(
        session,
        transaction_id=reversal_tx.transaction_id,
        account_id=source_acc.account_id,
        actor_id=actor_id,
        actor_role="forge_core_service",
        event_type="TRANSACTION_REVERSED",
        previous_state={
            "original_tx_id": original_tx.transaction_id,
            "original_status": TransactionStatus.SETTLED.value,
        },
        new_state={
            "reversal_tx_id": reversal_tx.transaction_id,
            "original_tx_id": original_tx.transaction_id,
            "restored_amount": str(total_original_amount),
            "reason": reason,
        },
    )
    return reversal_tx


def audit_account_balances(session: Session) -> AuditReconciliationReport:
    """Reconcile all account balances against double-entry ledger legs and verify audit hash chain.

    Checks three critical financial invariants:
    1. Global zero-sum invariant: Sum(DEBIT) == Sum(CREDIT) across all ledger_entries.
    2. Per-account reconciliation:Each account's live `balance` equals Sum(CREDIT) - Sum(DEBIT).
    3. Cryptographic audit trail integrity: Every `transaction_audit_logs` record matches its
       expected `prev_hash` and recomputed `curr_hash`.
    """
    total_debits_raw = session.scalar(
        select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
            LedgerEntry.direction == EntryDirection.DEBIT.value
        )
    )
    total_credits_raw = session.scalar(
        select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
            LedgerEntry.direction == EntryDirection.CREDIT.value
        )
    )
    total_debits = quantize_money(total_debits_raw or 0)
    total_credits = quantize_money(total_credits_raw or 0)

    accounts = list(session.scalars(select(Account).order_by(Account.account_number)).all())
    mismatched_accounts: list[str] = []

    for acc in accounts:
        acc_credits = quantize_money(
            session.scalar(
                select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
                    LedgerEntry.account_id == acc.account_id,
                    LedgerEntry.direction == EntryDirection.CREDIT.value,
                )
            )
            or 0
        )
        acc_debits = quantize_money(
            session.scalar(
                select(func.coalesce(func.sum(LedgerEntry.amount), 0)).where(
                    LedgerEntry.account_id == acc.account_id,
                    LedgerEntry.direction == EntryDirection.DEBIT.value,
                )
            )
            or 0
        )
        expected_balance = acc_credits - acc_debits
        if quantize_money(acc.balance) != expected_balance:
            mismatched_accounts.append(acc.account_number)

    logs = list(
        session.scalars(
            select(TransactionAuditLog).order_by(TransactionAuditLog.log_id.asc())
        ).all()
    )
    tampered_log_ids: list[int] = []
    expected_prev_hash = GENESIS_HASH

    for log_entry in logs:
        recomputed_hash = build_audit_hash(
            prev_hash=log_entry.prev_hash,
            transaction_id=log_entry.transaction_id,
            account_id=log_entry.account_id,
            actor_id=log_entry.actor_id,
            event_type=log_entry.event_type,
            new_state_json=log_entry.new_state,
        )
        if log_entry.prev_hash != expected_prev_hash or log_entry.curr_hash != recomputed_hash:
            tampered_log_ids.append(log_entry.log_id)
        expected_prev_hash = log_entry.curr_hash

    is_balanced = (total_debits == total_credits) and (len(mismatched_accounts) == 0)
    hash_chain_valid = len(tampered_log_ids) == 0

    return AuditReconciliationReport(
        is_balanced=is_balanced,
        hash_chain_valid=hash_chain_valid,
        total_debits=total_debits,
        total_credits=total_credits,
        accounts_checked=len(accounts),
        mismatched_accounts=mismatched_accounts,
        tampered_log_ids=tampered_log_ids,
    )
