"""Unit tests for the secp256k1 Pedersen/range-proof zero-knowledge module."""

from __future__ import annotations

import dataclasses

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from gpmorgan_forge import zkp


@pytest.mark.unit
def test_curve_arithmetic_is_consistent() -> None:
    """Generator is on-curve with order N; group laws and encodings behave."""
    assert zkp.is_on_curve(zkp.G)
    assert zkp.is_on_curve(None)
    assert zkp.is_on_curve(zkp.H)
    assert not zkp.is_on_curve((1, 1))
    assert zkp.scalar_mult(zkp.N, zkp.G) is None  # n*G = infinity
    assert zkp.scalar_mult(0, zkp.G) is None
    two_g = zkp.point_add(zkp.G, zkp.G)
    assert two_g == zkp.scalar_mult(2, zkp.G)
    assert zkp.point_add(two_g, zkp.point_neg(zkp.G)) == zkp.G
    assert zkp.point_add(zkp.G, zkp.point_neg(zkp.G)) is None
    assert zkp.point_add(None, zkp.G) == zkp.G
    assert zkp.point_add(zkp.G, None) == zkp.G
    assert zkp.point_neg(None) is None
    assert zkp.encode_point(None) == b"\x00"
    assert len(zkp.encode_point(zkp.G)) == 33
    assert zkp.encode_point(zkp.G)[0] in (2, 3)
    # Commitments are additively homomorphic.
    assert zkp.point_add(zkp.commit(3, 5), zkp.commit(4, 6)) == zkp.commit(7, 11)


@pytest.mark.unit
def test_hash_to_curve_retries_until_a_valid_point_is_found() -> None:
    """Try-and-increment skips non-residues deterministically."""
    labels = [f"probe-{i}" for i in range(12)]
    points = [zkp.hash_to_curve(label) for label in labels]
    assert all(zkp.is_on_curve(point) for point in points)
    assert points == [zkp.hash_to_curve(label) for label in labels]
    assert len(set(points)) == len(points)


@pytest.mark.unit
def test_range_proof_completeness_and_soundness() -> None:
    """Honest proofs verify; out-of-range, tampered, and mismatched proofs are rejected."""
    ctx = b"ctx"
    blinding = 123456789
    for value in (0, 1, 5, 7):
        commitment = zkp.commit(value, blinding)
        proof = zkp.prove_range(value, blinding, 3, ctx)
        assert zkp.verify_range(commitment, proof, 3, ctx)

    with pytest.raises(ValueError, match="outside the provable range"):
        zkp.prove_range(8, blinding, 3, ctx)
    with pytest.raises(ValueError, match="outside the provable range"):
        zkp.prove_range(-1, blinding, 3, ctx)

    commitment = zkp.commit(5, blinding)
    proof = zkp.prove_range(5, blinding, 3, ctx)
    assert not zkp.verify_range(commitment, proof, 3, b"other-context")  # context binding
    assert not zkp.verify_range(zkp.commit(6, blinding), proof, 3, ctx)  # wrong commitment
    assert not zkp.verify_range(commitment, proof, 4, ctx)  # wrong bit length

    bad_bit = dataclasses.replace(proof.bit_proofs[0], s0=(proof.bit_proofs[0].s0 + 1) % zkp.N)
    tampered = dataclasses.replace(proof, bit_proofs=(bad_bit, *proof.bit_proofs[1:]))
    assert not zkp.verify_range(commitment, tampered, 3, ctx)

    off_curve = dataclasses.replace(proof, bit_commitments=((1, 1), *proof.bit_commitments[1:]))
    assert not zkp.verify_range(commitment, off_curve, 3, ctx)

    short = dataclasses.replace(proof, bit_proofs=proof.bit_proofs[:2])
    assert not zkp.verify_range(commitment, short, 3, ctx)


@pytest.mark.unit
def test_bit_commitment_to_two_cannot_be_proven_as_a_bit() -> None:
    """Soundness of the OR-proof: a commitment to 2 has no valid bit proof."""
    ctx = b"ctx"
    forged_commitment = zkp.commit(2, 99)
    honest_for_zero = zkp._prove_bit(0, 99, forged_commitment, ctx)
    honest_for_one = zkp._prove_bit(1, 99, forged_commitment, ctx)
    assert not zkp._verify_bit(forged_commitment, honest_for_zero, ctx)
    assert not zkp._verify_bit(forged_commitment, honest_for_one, ctx)


@pytest.mark.unit
def test_eligibility_proof_with_issuer_attestation() -> None:
    """End-to-end: issuer signs commitments, holder proves, verifier checks (no raw values)."""
    issuer = zkp.KycCredentialIssuer(Ed25519PrivateKey.generate())
    credential, opening = issuer.issue("CUST-1", age=29, tier=3)
    proof = zkp.prove_eligibility(credential, opening, min_age=18, min_tier=2)
    assert zkp.verify_eligibility(credential, proof, issuer.public_key)
    assert len(credential.fingerprint()) == 64  # only a hash of the commitments is ever shared

    # Boundary values are provable; one below is not.
    edge, edge_open = issuer.issue("CUST-2", age=18, tier=2)
    assert zkp.verify_eligibility(edge, zkp.prove_eligibility(edge, edge_open), issuer.public_key)
    young, young_open = issuer.issue("CUST-3", age=17, tier=3)
    with pytest.raises(ValueError, match="outside the provable range"):
        zkp.prove_eligibility(young, young_open)
    low, low_open = issuer.issue("CUST-4", age=40, tier=1)
    with pytest.raises(ValueError, match="outside the provable range"):
        zkp.prove_eligibility(low, low_open)

    # A proof cannot be replayed against another credential or a different issuer.
    other, _ = issuer.issue("CUST-5", age=29, tier=3)
    assert not zkp.verify_eligibility(other, proof, issuer.public_key)
    rogue = zkp.KycCredentialIssuer(Ed25519PrivateKey.generate())
    assert not zkp.verify_eligibility(credential, proof, rogue.public_key)

    # Tampering with the signed commitments invalidates the signature.
    forged = dataclasses.replace(credential, age_commitment=zkp.commit(99, 1))
    assert not zkp.verify_eligibility(forged, proof, issuer.public_key)

    # A holder cannot claim a threshold different from the one proven.
    stronger = dataclasses.replace(proof, min_age=25)
    assert not zkp.verify_eligibility(credential, stronger, issuer.public_key)


@pytest.mark.unit
def test_issuer_key_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Issuer loads a fixed seed from the environment or generates an ephemeral key."""
    seed = "11" * 32
    monkeypatch.setenv("FORGE_ZKP_ISSUER_SEED", seed)
    first, second = zkp.KycCredentialIssuer(), zkp.KycCredentialIssuer()
    assert first.public_key.public_bytes_raw() == second.public_key.public_bytes_raw()
    monkeypatch.delenv("FORGE_ZKP_ISSUER_SEED")
    a, b = zkp.KycCredentialIssuer(), zkp.KycCredentialIssuer()
    assert a.public_key.public_bytes_raw() != b.public_key.public_bytes_raw()
