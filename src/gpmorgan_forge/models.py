"""SQLAlchemy 2.1 ORM and domain models for GPMorgan Forge (Portfolio 1: 3NF/BCNF Schema)."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class CustomerSegment(StrEnum):
    """Customer classification segments."""

    RETAIL = "RETAIL"
    SME = "SME"
    CORPORATE = "CORPORATE"
    INSTITUTIONAL = "INSTITUTIONAL"


class KYCTierLevel(StrEnum):
    """Regulatory KYC verification tiers."""

    TIER_1 = "TIER_1"
    TIER_2 = "TIER_2"
    TIER_3 = "TIER_3"


class AccountType(StrEnum):
    """Supported banking account types."""

    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    ESCROW = "ESCROW"
    SETTLEMENT_GL = "SETTLEMENT_GL"
    TREASURY = "TREASURY"


class AccountStatus(StrEnum):
    """Operational lifecycle states of an account."""

    ACTIVE = "ACTIVE"
    FROZEN = "FROZEN"
    DORMANT = "DORMANT"
    CLOSED = "CLOSED"


class CurrencyCode(StrEnum):
    """ISO 4217 supported currency codes."""

    NGN = "NGN"
    USD = "USD"
    GBP = "GBP"
    EUR = "EUR"


class TransactionType(StrEnum):
    """Financial transaction categories."""

    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    TRANSFER = "TRANSFER"
    REVERSAL = "REVERSAL"
    FEE_SETTLEMENT = "FEE_SETTLEMENT"


class TransactionStatus(StrEnum):
    """Lifecycle states of a financial transaction."""

    PENDING = "PENDING"
    SETTLED = "SETTLED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"
    FLAGGED = "FLAGGED"


class EntryDirection(StrEnum):
    """Double-entry accounting leg direction."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class RiskSeverity(StrEnum):
    """Fraud and AML alert severity levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


def utc_now() -> datetime:
    """Return current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def new_uuid_str() -> str:
    """Generate a new UUIDv4 string."""
    return str(uuid.uuid4())


def compute_sha256_hex(payload: str) -> str:
    """Compute a 64-character lowercase SHA-256 hex digest."""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class Base(DeclarativeBase):
    """Declarative base class for all GPMorgan Forge relational entities."""


class Customer(Base):
    """Customer identity profile normalised to BCNF."""

    __tablename__ = "customers"
    __table_args__ = (
        CheckConstraint("risk_rating >= 0 AND risk_rating <= 100", name="chk_customers_risk"),
    )

    customer_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    external_ref: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(160), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    phone_e164: Mapped[str] = mapped_column(String(24), unique=True, nullable=False)
    segment: Mapped[str] = mapped_column(
        String(24), nullable=False, default=CustomerSegment.RETAIL.value
    )
    kyc_tier: Mapped[str] = mapped_column(
        String(16), nullable=False, default=KYCTierLevel.TIER_1.value
    )
    bvn_sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False, default="NG")
    risk_rating: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("10.00")
    )
    is_anonymised: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    accounts: Mapped[list[Account]] = relationship(
        "Account", back_populates="customer", cascade="all, delete-orphan"
    )
    kyc_records: Mapped[list[KYCRecord]] = relationship(
        "KYCRecord", back_populates="customer", cascade="all, delete-orphan"
    )
    consents: Mapped[list[ConsentRecord]] = relationship(
        "ConsentRecord", back_populates="customer", cascade="all, delete-orphan"
    )
    beneficiaries: Mapped[list[Beneficiary]] = relationship(
        "Beneficiary", back_populates="customer", cascade="all, delete-orphan"
    )


class KYCRecord(Base):
    """Customer KYC verification document record (3NF/BCNF)."""

    __tablename__ = "kyc_records"

    kyc_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    customer_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("customers.customer_id", ondelete="RESTRICT"), nullable=False
    )
    document_type: Mapped[str] = mapped_column(String(40), nullable=False)
    document_ref_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    issuing_country: Mapped[str] = mapped_column(String(2), nullable=False, default="NG")
    verification_status: Mapped[str] = mapped_column(String(24), nullable=False, default="VERIFIED")
    verified_by: Mapped[str] = mapped_column(String(80), nullable=False)
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    customer: Mapped[Customer] = relationship("Customer", back_populates="kyc_records")


class ConsentRecord(Base):
    """Regulatory data processing consent record (NDPA 2023 & GDPR)."""

    __tablename__ = "consent_records"

    consent_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    customer_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("customers.customer_id", ondelete="RESTRICT"), nullable=False
    )
    regulation_code: Mapped[str] = mapped_column(String(24), nullable=False)
    purpose_code: Mapped[str] = mapped_column(String(64), nullable=False)
    consent_granted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    ip_address: Mapped[str] = mapped_column(String(45), nullable=False)
    granted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    customer: Mapped[Customer] = relationship("Customer", back_populates="consents")


class Account(Base):
    """Multi-currency bank account / wallet with optimistic locking version."""

    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint(
            "account_type = 'SETTLEMENT_GL' OR (balance >= 0 AND available_balance >= 0)",
            name="chk_accounts_non_negative_balance",
        ),
        CheckConstraint("daily_transfer_limit > 0", name="chk_accounts_daily_limit"),
        CheckConstraint("version >= 1", name="chk_accounts_version"),
    )

    account_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    customer_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("customers.customer_id", ondelete="RESTRICT"), nullable=False
    )
    account_number: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    account_name: Mapped[str] = mapped_column(String(160), nullable=False)
    account_type: Mapped[str] = mapped_column(
        String(24), nullable=False, default=AccountType.CHECKING.value
    )
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default=CurrencyCode.NGN.value)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, default=Decimal("0.0000")
    )
    available_balance: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, default=Decimal("0.0000")
    )
    daily_transfer_limit: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, default=Decimal("5000000.0000")
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=AccountStatus.ACTIVE.value
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    customer: Mapped[Customer] = relationship("Customer", back_populates="accounts")
    cards: Mapped[list[PaymentCard]] = relationship(
        "PaymentCard", back_populates="account", cascade="all, delete-orphan"
    )
    ledger_entries: Mapped[list[LedgerEntry]] = relationship(
        "LedgerEntry", back_populates="account"
    )


class Beneficiary(Base):
    """Saved transfer counterparty linked to a customer."""

    __tablename__ = "beneficiaries"
    __table_args__ = (
        UniqueConstraint(
            "customer_id", "bank_code", "account_number", name="uq_beneficiaries_cust_bank_acct"
        ),
    )

    beneficiary_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    customer_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("customers.customer_id", ondelete="CASCADE"), nullable=False
    )
    bank_code: Mapped[str] = mapped_column(String(16), nullable=False)
    account_number: Mapped[str] = mapped_column(String(10), nullable=False)
    beneficiary_name: Mapped[str] = mapped_column(String(160), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default=CurrencyCode.NGN.value)
    is_whitelisted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    customer: Mapped[Customer] = relationship("Customer", back_populates="beneficiaries")


class PaymentCard(Base):
    """PCI-DSS v4.0 masked and tokenised payment card."""

    __tablename__ = "payment_cards"
    __table_args__ = (
        CheckConstraint("expiry_month >= 1 AND expiry_month <= 12", name="chk_cards_month"),
    )

    card_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False
    )
    card_token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    masked_pan: Mapped[str] = mapped_column(String(19), nullable=False)
    card_scheme: Mapped[str] = mapped_column(String(20), nullable=False)
    expiry_month: Mapped[int] = mapped_column(Integer, nullable=False)
    expiry_year: Mapped[int] = mapped_column(Integer, nullable=False)
    spending_limit: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, default=Decimal("1000000.0000")
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    account: Mapped[Account] = relationship("Account", back_populates="cards")


class Transaction(Base):
    """Core financial transaction record supporting transfers, deposits, withdrawals, reversals."""

    __tablename__ = "transactions"
    __table_args__ = (
        CheckConstraint(
            "source_account_id <> destination_account_id", name="chk_tx_distinct_accounts"
        ),
        CheckConstraint(
            "amount > 0 AND fee_amount >= 0 AND exchange_rate > 0", name="chk_tx_positive_amounts"
        ),
    )

    transaction_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    reference_code: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    source_account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False
    )
    destination_account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False
    )
    transaction_type: Mapped[str] = mapped_column(String(24), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=TransactionStatus.PENDING.value
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    fee_amount: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, default=Decimal("0.0000")
    )
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default=CurrencyCode.NGN.value)
    exchange_rate: Mapped[Decimal] = mapped_column(
        Numeric(19, 6), nullable=False, default=Decimal("1.000000")
    )
    parent_transaction_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("transactions.transaction_id", ondelete="RESTRICT"), nullable=True
    )
    channel: Mapped[str] = mapped_column(String(32), nullable=False, default="API")
    narration: Mapped[str] = mapped_column(String(255), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metadata_json: Mapped[str] = mapped_column(String(2048), nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    ledger_entries: Mapped[list[LedgerEntry]] = relationship(
        "LedgerEntry", back_populates="transaction", cascade="all, delete-orphan"
    )


class LedgerEntry(Base):
    """Immutable double-entry accounting leg enforcing zero-sum financial integrity."""

    __tablename__ = "ledger_entries"
    __table_args__ = (
        UniqueConstraint("transaction_id", "entry_sequence", name="uq_ledger_tx_seq"),
        CheckConstraint("amount > 0", name="chk_ledger_positive_amount"),
    )

    entry_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    transaction_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("transactions.transaction_id", ondelete="RESTRICT"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False
    )
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    balance_before: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    entry_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )

    transaction: Mapped[Transaction] = relationship("Transaction", back_populates="ledger_entries")
    account: Mapped[Account] = relationship("Account", back_populates="ledger_entries")


class TransactionAuditLog(Base):
    """Cryptographic SHA-256 hash-chained immutable audit log."""

    __tablename__ = "transaction_audit_logs"

    log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("transactions.transaction_id", ondelete="RESTRICT"), nullable=True
    )
    account_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=True
    )
    actor_id: Mapped[str] = mapped_column(String(80), nullable=False)
    actor_role: Mapped[str] = mapped_column(String(40), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    ip_address: Mapped[str] = mapped_column(String(45), nullable=False, default="127.0.0.1")
    previous_state: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    new_state: Mapped[str] = mapped_column(String(2048), nullable=False)
    prev_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    curr_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )


class FraudAlert(Base):
    """Real-time AML and fraud anomaly alert linked to a transaction and account."""

    __tablename__ = "fraud_alerts"
    __table_args__ = (
        CheckConstraint("risk_score >= 0 AND risk_score <= 100", name="chk_fraud_score"),
    )

    alert_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid_str)
    transaction_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("transactions.transaction_id", ondelete="RESTRICT"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False
    )
    rule_code: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    risk_score: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    details: Mapped[str] = mapped_column(String(512), nullable=False)
    is_resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    resolved_by: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


@dataclass(frozen=True)
class AuditReconciliationReport:
    """Comprehensive result of auditing account balances and ledger hash-chain integrity."""

    is_balanced: bool
    hash_chain_valid: bool
    total_debits: Decimal
    total_credits: Decimal
    accounts_checked: int
    mismatched_accounts: list[str]
    tampered_log_ids: list[int]

    def to_dict(self) -> dict[str, Any]:
        """Serialize reconciliation report to dictionary."""
        return {
            "is_balanced": self.is_balanced,
            "hash_chain_valid": self.hash_chain_valid,
            "total_debits": str(self.total_debits),
            "total_credits": str(self.total_credits),
            "accounts_checked": self.accounts_checked,
            "mismatched_accounts": self.mismatched_accounts,
            "tampered_log_ids": self.tampered_log_ids,
        }


def build_audit_hash(
    prev_hash: str,
    transaction_id: str | None,
    account_id: str | None,
    actor_id: str,
    event_type: str,
    new_state_json: str,
) -> str:
    """Deterministically compute the SHA-256 hash chain link for an audit log entry."""
    canonical = json.dumps(
        {
            "prev_hash": prev_hash,
            "transaction_id": transaction_id or "",
            "account_id": account_id or "",
            "actor_id": actor_id,
            "event_type": event_type,
            "new_state": new_state_json,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return compute_sha256_hex(canonical)
