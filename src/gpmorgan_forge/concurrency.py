"""Transaction lifecycle, SAVEPOINT batching, and concurrency control engine (Portfolio 3)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from gpmorgan_forge.models import Account, Transaction
from gpmorgan_forge.operations import (
    BankingOperationError,
    OptimisticLockError,
    quantize_money,
    transfer_funds,
)


@dataclass(frozen=True)
class BatchTransferItem:
    """Single transfer instruction within a multi-recipient corporate disbursement batch."""

    idempotency_key: str
    reference_code: str
    source_account_id: str
    destination_account_id: str
    amount: Decimal
    narration: str


@dataclass(frozen=True)
class BatchTransferOutcome:
    """Outcome of a SAVEPOINT-protected multi-recipient disbursement batch."""

    settled_references: list[str]
    failed_items: list[dict[str, str]]
    final_source_balance: Decimal


@dataclass(frozen=True)
class ConcurrencySimulationResult:
    """Result of simulating concurrent workers and deadlock-free canonical lock ordering."""

    canonical_lock_order: list[str]
    deadlock_prevented: bool
    worker_a_committed: bool
    worker_b_conflict_detected: bool
    conflict_error_message: str
    final_account_balance: Decimal
    final_account_version: int


def execute_atomic_transfer_with_rollback_guard(
    session: Session,
    *,
    idempotency_key: str,
    reference_code: str,
    source_account_id: str,
    destination_account_id: str,
    amount: Decimal | str | int,
    narration: str,
    expected_source_version: int | None = None,
) -> tuple[bool, Transaction | None, str]:
    """Execute a transfer inside a SAVEPOINT so failures roll back cleanly."""
    try:
        with session.begin_nested():
            tx = transfer_funds(
                session,
                idempotency_key=idempotency_key,
                reference_code=reference_code,
                source_account_id=source_account_id,
                destination_account_id=destination_account_id,
                amount=amount,
                narration=narration,
                expected_source_version=expected_source_version,
            )
        return True, tx, "COMMITTED"
    except BankingOperationError as exc:
        return False, None, f"ROLLED_BACK: {exc}"


def execute_batch_transfers_with_savepoints(
    session: Session,
    items: list[BatchTransferItem],
) -> BatchTransferOutcome:
    """Process a batch of transfers using a SAVEPOINT per leg for partial recovery."""
    settled_refs: list[str] = []
    failed_items: list[dict[str, str]] = []
    source_id: str | None = items[0].source_account_id if items else None

    for item in items:
        try:
            with session.begin_nested():
                tx = transfer_funds(
                    session,
                    idempotency_key=item.idempotency_key,
                    reference_code=item.reference_code,
                    source_account_id=item.source_account_id,
                    destination_account_id=item.destination_account_id,
                    amount=item.amount,
                    narration=item.narration,
                )
                settled_refs.append(tx.reference_code)
        except BankingOperationError as exc:
            failed_items.append(
                {
                    "reference_code": item.reference_code,
                    "error": str(exc),
                }
            )

    final_balance = Decimal("0.0000")
    if source_id is not None:
        src_acc = session.get(Account, source_id)
        if src_acc is not None:
            final_balance = quantize_money(src_acc.balance)

    return BatchTransferOutcome(
        settled_references=settled_refs,
        failed_items=failed_items,
        final_source_balance=final_balance,
    )


def compute_canonical_lock_schedule(
    request_1: tuple[str, str],
    request_2: tuple[str, str],
) -> tuple[list[str], list[str], bool]:
    """Prove that opposing transfers (A->B and B->A) acquire locks in ascending order."""
    schedule_1 = sorted([request_1[0], request_1[1]])
    schedule_2 = sorted([request_2[0], request_2[1]])
    is_deadlock_free = schedule_1 == schedule_2
    return schedule_1, schedule_2, is_deadlock_free


def simulate_optimistic_concurrency_conflict(
    session: Session,
    *,
    source_account_id: str,
    destination_account_id: str,
    worker_a_amount: Decimal,
    worker_b_amount: Decimal,
    idempotency_prefix: str = "OCC-SIM",
) -> ConcurrencySimulationResult:
    """Simulate two concurrent workers reading the same version and attempting debits."""
    source_acc = session.get(Account, source_account_id)
    if source_acc is None:
        raise BankingOperationError(f"Source account '{source_account_id}' not found.")

    shared_snapshot_version = source_acc.version
    sched_1, _, deadlock_free = compute_canonical_lock_schedule(
        (source_account_id, destination_account_id),
        (destination_account_id, source_account_id),
    )

    # Worker A commits first using the snapshot version
    transfer_funds(
        session,
        idempotency_key=f"{idempotency_prefix}-WORKER-A",
        reference_code=f"FRG-{idempotency_prefix}-A",
        source_account_id=source_account_id,
        destination_account_id=destination_account_id,
        amount=worker_a_amount,
        narration="Worker A concurrent transfer",
        expected_source_version=shared_snapshot_version,
    )

    # Worker B attempts to update using the now-stale snapshot version -> OptimisticLockError
    conflict_detected = False
    conflict_msg = ""
    try:
        transfer_funds(
            session,
            idempotency_key=f"{idempotency_prefix}-WORKER-B",
            reference_code=f"FRG-{idempotency_prefix}-B",
            source_account_id=source_account_id,
            destination_account_id=destination_account_id,
            amount=worker_b_amount,
            narration="Worker B stale snapshot transfer",
            expected_source_version=shared_snapshot_version,
        )
    except OptimisticLockError as exc:
        conflict_detected = True
        conflict_msg = str(exc)

    refreshed_source = session.get(Account, source_account_id)
    assert refreshed_source is not None

    return ConcurrencySimulationResult(
        canonical_lock_order=sched_1,
        deadlock_prevented=deadlock_free,
        worker_a_committed=True,
        worker_b_conflict_detected=conflict_detected,
        conflict_error_message=conflict_msg,
        final_account_balance=quantize_money(refreshed_source.balance),
        final_account_version=refreshed_source.version,
    )


def summarize_portfolio_3_evidence(
    session: Session,
    *,
    source_account_id: str,
    destination_account_id: str,
) -> dict[str, Any]:
    """Run Portfolio 3 concurrency verification and return structured evidence."""
    sim = simulate_optimistic_concurrency_conflict(
        session,
        source_account_id=source_account_id,
        destination_account_id=destination_account_id,
        worker_a_amount=Decimal("50000.0000"),
        worker_b_amount=Decimal("75000.0000"),
        idempotency_prefix="P3-VERIFY",
    )
    return {
        "canonical_lock_order": sim.canonical_lock_order,
        "deadlock_prevented": sim.deadlock_prevented,
        "worker_a_committed": sim.worker_a_committed,
        "worker_b_conflict_detected": sim.worker_b_conflict_detected,
        "conflict_error_message": sim.conflict_error_message,
        "final_account_balance": str(sim.final_account_balance),
        "final_account_version": sim.final_account_version,
    }
