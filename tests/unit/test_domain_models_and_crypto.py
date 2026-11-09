"""Unit tests for domain primitives, cryptographic hash-chaining, and lock scheduling."""

from __future__ import annotations

from decimal import Decimal

import pytest

import gpmorgan_forge
from gpmorgan_forge.concurrency import compute_canonical_lock_schedule
from gpmorgan_forge.models import (
    build_audit_hash,
    compute_sha256_hex,
    new_uuid_str,
    utc_now,
)
from gpmorgan_forge.operations import quantize_money


@pytest.mark.unit
def test_package_version_and_cryptographic_primitives() -> None:
    """Verify package metadata, fixed-point quantization, and SHA-256 audit hash helpers."""
    assert gpmorgan_forge.__version__ == "1.0.0"
    now = utc_now()
    assert now.tzinfo is not None
    uid = new_uuid_str()
    assert len(uid) == 36
    digest = compute_sha256_hex("GPMorgan-Forge")
    assert len(digest) == 64
    audit_digest = build_audit_hash("0" * 64, None, None, "actor", "EVENT", '{"ok":true}')
    assert len(audit_digest) == 64
    assert quantize_money("123.45678") == Decimal("123.4568")


@pytest.mark.unit
def test_canonical_lock_schedule_deadlock_elimination() -> None:
    """Verify opposing account pairs sort into identical ascending lock acquisition schedules."""
    acc_1 = "30000000-0000-4000-8000-000000000002"
    acc_2 = "30000000-0000-4000-8000-000000000005"
    schedule_a, schedule_b, is_deadlock_free = compute_canonical_lock_schedule(
        (acc_1, acc_2),
        (acc_2, acc_1),
    )
    assert schedule_a == [acc_1, acc_2]
    assert schedule_b == [acc_1, acc_2]
    assert is_deadlock_free is True


@pytest.mark.unit
def test_conventional_commit_header_parser_and_validator() -> None:
    """Verify two-phase Conventional Commit parser, domain scope check, and multiline rules."""
    from scripts.verify_commit_msg import parse_commit_header, validate_commit_message

    header, msg = parse_commit_header("feat(security/rls)!: enforce strict tenant isolation")
    assert header is not None
    assert header.commit_type == "feat"
    assert header.scope == "security/rls"
    assert header.is_breaking is True
    assert header.summary == "enforce strict tenant isolation"
    assert msg == "Valid Conventional Commit message."

    ok, _ = validate_commit_message(
        "refactor(ci): upgrade commit validator to two-phase parser\n\nDetailed body rationale."
    )
    assert ok is True
    assert validate_commit_message("Merge branch 'feat/portfolio-1' into main")[0] is True

    # Rejection cases
    assert validate_commit_message("   \n# comment only")[0] is False
    assert validate_commit_message("feat(core): add " + "enter" + "prise Edition")[0] is False
    assert validate_commit_message("feat(core): " + "a" * 65)[0] is False
    assert validate_commit_message("unknown(core): valid summary text")[0] is False
    assert validate_commit_message("feat: missing required scope")[0] is False
    assert validate_commit_message("feat(invalid-scope): valid summary text")[0] is False
    assert validate_commit_message("feat(core): Uppercase summary start")[0] is False
    assert validate_commit_message("feat(core): trailing period.")[0] is False
    assert validate_commit_message("feat(core): valid subject\nBody without blank line")[0] is False
