"""Shared SQLAlchemy relational fixtures for unit and integration tests."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from gpmorgan_forge.models import AccountType, Base, CurrencyCode
from gpmorgan_forge.operations import (
    create_account,
    onboard_customer,
    process_deposit,
    reverse_failed_transaction,
    transfer_funds,
)


@pytest.fixture
def db_session() -> Iterator[Session]:
    """Create a clean in-memory SQLite relational database session for testing."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.fixture
def seeded_session() -> Iterator[tuple[Session, str, str, str]]:
    """Create and seed an in-memory relational session returning account IDs."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        cust = onboard_customer(
            session,
            external_ref="CUST-QOPT-01",
            full_name="Optimiser Customer",
            email="opt@example.ng",
            phone_e164="+2348060000001",
            raw_bvn="22334455001",
        )
        gl_acc = create_account(
            session,
            customer_id=cust.customer_id,
            account_number="1000000001",
            account_name="Settlement GL",
            account_type=AccountType.SETTLEMENT_GL,
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("999999999.0000"),
        )
        corp_acc = create_account(
            session,
            customer_id=cust.customer_id,
            account_number="2000000002",
            account_name="Corporate Account",
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("50000000.0000"),
        )
        retail_acc = create_account(
            session,
            customer_id=cust.customer_id,
            account_number="2000000003",
            account_name="Retail Account",
            currency=CurrencyCode.NGN,
            daily_transfer_limit=Decimal("5000000.0000"),
        )

        process_deposit(
            session,
            idempotency_key="QOPT-DEP-1",
            reference_code="REF-QOPT-DEP-1",
            settlement_gl_account_id=gl_acc.account_id,
            customer_account_id=corp_acc.account_id,
            amount=Decimal("10000000.0000"),
        )
        tx1 = transfer_funds(
            session,
            idempotency_key="QOPT-TRF-1",
            reference_code="REF-QOPT-TRF-1",
            source_account_id=corp_acc.account_id,
            destination_account_id=retail_acc.account_id,
            amount=Decimal("50000.0000"),
            narration="Small transfer below threshold",
        )
        tx2 = transfer_funds(
            session,
            idempotency_key="QOPT-TRF-2",
            reference_code="REF-QOPT-TRF-2",
            source_account_id=corp_acc.account_id,
            destination_account_id=retail_acc.account_id,
            amount=Decimal("1500000.0000"),
            narration="Second transfer triggering velocity",
        )
        reverse_failed_transaction(
            session,
            original_transaction_id=tx1.transaction_id,
            reversal_idempotency_key="QOPT-REV-1",
            reversal_reference_code="REF-QOPT-REV-1",
            reason="Test reversal",
        )
        assert tx2.reference_code == "REF-QOPT-TRF-2"
        yield session, gl_acc.account_id, corp_acc.account_id, retail_acc.account_id
    engine.dispose()
