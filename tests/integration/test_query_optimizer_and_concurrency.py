"""Integration tests for Portfolio 2 query optimiser and Portfolio 3 concurrency engine."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from gpmorgan_forge.concurrency import (
    BatchTransferItem,
    execute_atomic_transfer_with_rollback_guard,
    execute_batch_transfers_with_savepoints,
    simulate_optimistic_concurrency_conflict,
    summarize_portfolio_3_evidence,
)
from gpmorgan_forge.operations import BankingOperationError
from gpmorgan_forge.query_optimizer import (
    compute_corridor_settlement_rollup,
    detect_outbound_velocity_anomalies,
    get_account_running_balance_statement,
    inspect_query_execution_plan,
    summarize_portfolio_2_benchmarks,
)


@pytest.mark.integration
def test_portfolio_2_query_optimizer(seeded_session: tuple[Session, str, str, str]) -> None:
    """Verify running balance statement, velocity anomalies, rollups, and plan inspection."""
    session, _, _, _ = seeded_session

    stmt_rows = get_account_running_balance_statement(session, "2000000002")
    assert len(stmt_rows) >= 3
    assert all(r.balance_drift == Decimal("0.0000") for r in stmt_rows)

    anomalies = detect_outbound_velocity_anomalies(
        session,
        min_tx_count_24h=5,
        min_volume_24h=Decimal("500000.0000"),
    )
    assert len(anomalies) == 1
    assert anomalies[0].reference_code == "REF-QOPT-TRF-2"

    rollups = compute_corridor_settlement_rollup(session)
    assert len(rollups) >= 2

    plan = inspect_query_execution_plan(session, "2000000002")
    assert plan.speedup_factor > 100.0
    assert len(plan.actual_plan_steps) >= 1

    summary = summarize_portfolio_2_benchmarks(session, "2000000002")
    assert summary["max_balance_drift"] == "0.0000"
    assert summary["benchmark"]["speedup_factor"] > 100.0


@pytest.mark.integration
def test_portfolio_3_concurrency_and_savepoint_batches(
    seeded_session: tuple[Session, str, str, str],
) -> None:
    """Verify SAVEPOINT rollback guards, batch partial failures, lock ordering, and OCC."""
    session, _, corp_id, retail_id = seeded_session

    ok, tx, status = execute_atomic_transfer_with_rollback_guard(
        session,
        idempotency_key="CONC-GUARD-OK",
        reference_code="REF-CONC-GUARD-OK",
        source_account_id=corp_id,
        destination_account_id=retail_id,
        amount=Decimal("25000.0000"),
        narration="Savepoint success",
    )
    assert ok is True
    assert tx is not None
    assert status == "COMMITTED"

    ok_fail, tx_fail, status_fail = execute_atomic_transfer_with_rollback_guard(
        session,
        idempotency_key="CONC-GUARD-FAIL",
        reference_code="REF-CONC-GUARD-FAIL",
        source_account_id=corp_id,
        destination_account_id=retail_id,
        amount=Decimal("-100.0000"),
        narration="Savepoint failure",
    )
    assert ok_fail is False
    assert tx_fail is None
    assert status_fail.startswith("ROLLED_BACK:")

    empty_outcome = execute_batch_transfers_with_savepoints(session, [])
    assert empty_outcome.settled_references == []
    assert empty_outcome.final_source_balance == Decimal("0.0000")

    missing_src_outcome = execute_batch_transfers_with_savepoints(
        session,
        [
            BatchTransferItem(
                idempotency_key="BATCH-MISSING-SRC",
                reference_code="REF-BATCH-MISSING",
                source_account_id="non-existent-source-id",
                destination_account_id=retail_id,
                amount=Decimal("100.0000"),
                narration="Missing source",
            )
        ],
    )
    assert len(missing_src_outcome.failed_items) == 1
    assert missing_src_outcome.final_source_balance == Decimal("0.0000")

    mixed_outcome = execute_batch_transfers_with_savepoints(
        session,
        [
            BatchTransferItem(
                idempotency_key="BATCH-LEG-1",
                reference_code="REF-BATCH-LEG-1",
                source_account_id=corp_id,
                destination_account_id=retail_id,
                amount=Decimal("10000.0000"),
                narration="Valid leg 1",
            ),
            BatchTransferItem(
                idempotency_key="BATCH-LEG-2-FAIL",
                reference_code="REF-BATCH-LEG-2-FAIL",
                source_account_id=corp_id,
                destination_account_id=corp_id,
                amount=Decimal("10000.0000"),
                narration="Invalid self-transfer leg 2",
            ),
            BatchTransferItem(
                idempotency_key="BATCH-LEG-3",
                reference_code="REF-BATCH-LEG-3",
                source_account_id=corp_id,
                destination_account_id=retail_id,
                amount=Decimal("15000.0000"),
                narration="Valid leg 3",
            ),
        ],
    )
    assert mixed_outcome.settled_references == ["REF-BATCH-LEG-1", "REF-BATCH-LEG-3"]
    assert len(mixed_outcome.failed_items) == 1
    assert mixed_outcome.final_source_balance > Decimal("0.0000")

    with pytest.raises(BankingOperationError, match="not found"):
        simulate_optimistic_concurrency_conflict(
            session,
            source_account_id="missing-source",
            destination_account_id=retail_id,
            worker_a_amount=Decimal("100.0000"),
            worker_b_amount=Decimal("100.0000"),
        )

    p3_evidence = summarize_portfolio_3_evidence(
        session,
        source_account_id=corp_id,
        destination_account_id=retail_id,
    )
    assert p3_evidence["deadlock_prevented"] is True
    assert p3_evidence["worker_a_committed"] is True
    assert p3_evidence["worker_b_conflict_detected"] is True
