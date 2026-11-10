"""Zero-knowledge range proofs over secp256k1 (pure Python, Fiat-Shamir NIZK).

Implements a real (not simulated) zero-knowledge proof that a committed secret attribute satisfies
``value >= threshold`` without revealing the value:

* **Pedersen commitments** ``C = v*G + r*H`` on secp256k1. ``H`` is derived by hash-to-curve from a
  public label, so nobody knows ``log_G(H)`` and the commitment is computationally binding and
  perfectly hiding.
* **Range proof by bit decomposition**: the secret ``v - threshold`` is written in ``n`` bits, each
  bit is committed, every bit commitment is proven to open to 0 or 1 with a Cramer-Damgard-
  Schoenmakers OR-proof (Sigma protocol made non-interactive with Fiat-Shamir), and the verifier
  checks that the weighted bit commitments sum to the public commitment.
* **Issuer attestation**: an Ed25519 signature by the KYC authority binds the commitments to the
  customer, so a holder cannot commit to an arbitrary age or tier.

The cost is linear in the bit length and is meant for small ranges (age and KYC tier).
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from dataclasses import dataclass
from typing import Final

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

P: Final[int] = 2**256 - 2**32 - 977
N: Final[int] = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
GX: Final[int] = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
GY: Final[int] = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8

type Point = tuple[int, int] | None  # None is the point at infinity

G: Final[Point] = (GX, GY)


def is_on_curve(point: Point) -> bool:
    """Return whether ``point`` satisfies ``y^2 = x^3 + 7`` (infinity counts as on-curve)."""
    if point is None:
        return True
    x, y = point
    return (y * y - (x * x * x + 7)) % P == 0


def point_add(a: Point, b: Point) -> Point:
    """Add two curve points (affine coordinates)."""
    if a is None:
        return b
    if b is None:
        return a
    (x1, y1), (x2, y2) = a, b
    if x1 == x2 and (y1 + y2) % P == 0:
        return None
    doubling = a == b
    numerator = 3 * x1 * x1 if doubling else y2 - y1
    denominator = 2 * y1 if doubling else x2 - x1
    slope = numerator * pow(denominator, -1, P) % P
    x3 = (slope * slope - x1 - x2) % P
    return (x3, (slope * (x1 - x3) - y1) % P)


def point_neg(a: Point) -> Point:
    """Negate a curve point."""
    return None if a is None else (a[0], (-a[1]) % P)


def scalar_mult(k: int, point: Point) -> Point:
    """Multiply a point by a scalar with double-and-add."""
    k %= N
    result: Point = None
    addend = point
    while k:
        if k & 1:
            result = point_add(result, addend)
        addend = point_add(addend, addend)
        k >>= 1
    return result


def encode_point(point: Point) -> bytes:
    """Serialise a point in SEC1 compressed form (single zero byte for infinity)."""
    if point is None:
        return b"\x00"
    x, y = point
    return bytes([2 + (y & 1)]) + x.to_bytes(32, "big")


def hash_to_curve(label: str) -> Point:
    """Derive a curve point from ``label`` by try-and-increment (nothing-up-my-sleeve)."""
    counter = 0
    while True:
        digest = hashlib.sha256(f"{label}|{counter}".encode()).digest()
        x = int.from_bytes(digest, "big") % P
        rhs = (x * x * x + 7) % P
        y = pow(rhs, (P + 1) // 4, P)  # P % 4 == 3
        if y * y % P == rhs:
            return (x, y)
        counter += 1


H: Final[Point] = hash_to_curve("gpmorgan-forge/pedersen/H/v1")


def commit(value: int, blinding: int) -> Point:
    """Pedersen commitment ``value*G + blinding*H``."""
    return point_add(scalar_mult(value, G), scalar_mult(blinding, H))


def challenge(context: bytes, *points: Point) -> int:
    """Fiat-Shamir challenge: hash of the context and every transcript point, reduced mod N."""
    hasher = hashlib.sha256(context)
    for point in points:
        hasher.update(encode_point(point))
    return int.from_bytes(hasher.digest(), "big") % N


@dataclass(frozen=True)
class BitProof:
    """OR-proof that a commitment opens to 0 or 1 (challenge/response for both branches)."""

    c0: int
    s0: int
    c1: int
    s1: int


@dataclass(frozen=True)
class RangeProof:
    """Non-interactive proof that a committed value lies in ``[0, 2**n_bits)``."""

    bit_commitments: tuple[Point, ...]
    bit_proofs: tuple[BitProof, ...]


def _prove_bit(bit: int, blinding: int, commitment: Point, context: bytes) -> BitProof:
    """Prove ``commitment`` opens to ``bit`` in {0, 1} without revealing which."""
    shifted = point_add(commitment, point_neg(G))  # C - G, equals r*H when bit == 1
    k = secrets.randbelow(N - 1) + 1
    if bit == 0:
        c1, s1 = secrets.randbelow(N), secrets.randbelow(N)
        r0 = scalar_mult(k, H)
        r1 = point_add(scalar_mult(s1, H), point_neg(scalar_mult(c1, shifted)))
        c0 = (challenge(context, commitment, r0, r1) - c1) % N
        s0 = (k + c0 * blinding) % N
    else:
        c0, s0 = secrets.randbelow(N), secrets.randbelow(N)
        r0 = point_add(scalar_mult(s0, H), point_neg(scalar_mult(c0, commitment)))
        r1 = scalar_mult(k, H)
        c1 = (challenge(context, commitment, r0, r1) - c0) % N
        s1 = (k + c1 * blinding) % N
    return BitProof(c0=c0, s0=s0, c1=c1, s1=s1)


def _verify_bit(commitment: Point, proof: BitProof, context: bytes) -> bool:
    """Verify a bit OR-proof."""
    shifted = point_add(commitment, point_neg(G))
    r0 = point_add(scalar_mult(proof.s0, H), point_neg(scalar_mult(proof.c0, commitment)))
    r1 = point_add(scalar_mult(proof.s1, H), point_neg(scalar_mult(proof.c1, shifted)))
    return (proof.c0 + proof.c1) % N == challenge(context, commitment, r0, r1)


def prove_range(value: int, blinding: int, n_bits: int, context: bytes) -> RangeProof:
    """Prove ``commit(value, blinding)`` hides a value in ``[0, 2**n_bits)``.

    Raises ``ValueError`` if the value is outside the range (an honest prover cannot lie).
    """
    if not 0 <= value < 2**n_bits:
        raise ValueError("value is outside the provable range")
    bits = [(value >> i) & 1 for i in range(n_bits)]
    blindings = [secrets.randbelow(N) for _ in range(n_bits - 1)]
    partial = sum((1 << i) * r for i, r in enumerate(blindings)) % N
    last_weight_inverse = pow(1 << (n_bits - 1), -1, N)
    blindings.append((blinding - partial) * last_weight_inverse % N)

    commitments = tuple(commit(b, r) for b, r in zip(bits, blindings, strict=True))
    proofs = tuple(
        _prove_bit(b, r, c, context + bytes([i]))
        for i, (b, r, c) in enumerate(zip(bits, blindings, commitments, strict=True))
    )
    return RangeProof(bit_commitments=commitments, bit_proofs=proofs)


def verify_range(commitment: Point, proof: RangeProof, n_bits: int, context: bytes) -> bool:
    """Verify that ``commitment`` hides a value in ``[0, 2**n_bits)``."""
    if len(proof.bit_commitments) != n_bits or len(proof.bit_proofs) != n_bits:
        return False
    if not all(is_on_curve(c) for c in proof.bit_commitments):
        return False
    total: Point = None
    for i, bit_commitment in enumerate(proof.bit_commitments):
        total = point_add(total, scalar_mult(1 << i, bit_commitment))
    if total != commitment:
        return False
    return all(
        _verify_bit(c, p, context + bytes([i]))
        for i, (c, p) in enumerate(zip(proof.bit_commitments, proof.bit_proofs, strict=True))
    )


AGE_BITS: Final[int] = 8
TIER_BITS: Final[int] = 2


@dataclass(frozen=True)
class KycCredential:
    """Issuer-signed commitments to a customer's age and KYC tier (no raw attributes)."""

    customer_id: str
    age_commitment: Point
    tier_commitment: Point
    signature: bytes

    def signed_payload(self) -> bytes:
        """Canonical bytes covered by the issuer signature."""
        return json.dumps(
            {
                "customer_id": self.customer_id,
                "age": encode_point(self.age_commitment).hex(),
                "tier": encode_point(self.tier_commitment).hex(),
            },
            sort_keys=True,
        ).encode()

    def fingerprint(self) -> str:
        """SHA-256 over the signed commitments (safe to log and share)."""
        return hashlib.sha256(self.signed_payload()).hexdigest()


@dataclass(frozen=True)
class CredentialOpening:
    """Holder-only secrets: the attribute values and their commitment blinding factors."""

    age: int
    tier: int
    age_blinding: int
    tier_blinding: int


@dataclass(frozen=True)
class EligibilityProof:
    """Zero-knowledge proof of ``age >= min_age AND tier >= min_tier`` for a credential."""

    min_age: int
    min_tier: int
    age_proof: RangeProof
    tier_proof: RangeProof


class KycCredentialIssuer:
    """KYC authority that signs attribute commitments with Ed25519."""

    def __init__(self, private_key: Ed25519PrivateKey | None = None) -> None:
        """Use ``private_key`` or load a 32-byte seed from ``FORGE_ZKP_ISSUER_SEED`` (hex)."""
        if private_key is None:
            seed_hex = os.environ.get("FORGE_ZKP_ISSUER_SEED")
            private_key = (
                Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex))
                if seed_hex
                else Ed25519PrivateKey.generate()
            )
        self._private_key = private_key
        self.public_key: Ed25519PublicKey = private_key.public_key()

    def issue(
        self, customer_id: str, age: int, tier: int
    ) -> tuple[KycCredential, CredentialOpening]:
        """Commit to the attributes, sign the commitments, and return the holder's openings."""
        opening = CredentialOpening(
            age=age,
            tier=tier,
            age_blinding=secrets.randbelow(N),
            tier_blinding=secrets.randbelow(N),
        )
        unsigned = KycCredential(
            customer_id=customer_id,
            age_commitment=commit(age, opening.age_blinding),
            tier_commitment=commit(tier, opening.tier_blinding),
            signature=b"",
        )
        signature = self._private_key.sign(unsigned.signed_payload())
        return (
            KycCredential(
                customer_id=customer_id,
                age_commitment=unsigned.age_commitment,
                tier_commitment=unsigned.tier_commitment,
                signature=signature,
            ),
            opening,
        )


def _shift(commitment: Point, threshold: int) -> Point:
    """Public commitment to ``value - threshold`` (homomorphic subtraction of ``threshold*G``)."""
    return point_add(commitment, point_neg(scalar_mult(threshold, G)))


def _context(credential: KycCredential, label: str, threshold: int) -> bytes:
    """Bind a proof to the credential, attribute and threshold (prevents proof replay)."""
    return f"{credential.fingerprint()}|{label}|{threshold}".encode()


def prove_eligibility(
    credential: KycCredential,
    opening: CredentialOpening,
    *,
    min_age: int = 18,
    min_tier: int = 2,
) -> EligibilityProof:
    """Prove ``age >= min_age AND tier >= min_tier``; raises ``ValueError`` if it is false."""
    return EligibilityProof(
        min_age=min_age,
        min_tier=min_tier,
        age_proof=prove_range(
            opening.age - min_age,
            opening.age_blinding,
            AGE_BITS,
            _context(credential, "age", min_age),
        ),
        tier_proof=prove_range(
            opening.tier - min_tier,
            opening.tier_blinding,
            TIER_BITS,
            _context(credential, "tier", min_tier),
        ),
    )


def verify_eligibility(
    credential: KycCredential, proof: EligibilityProof, issuer_public_key: Ed25519PublicKey
) -> bool:
    """Verify the issuer signature and both range proofs. Learns nothing else about the values."""
    try:
        issuer_public_key.verify(credential.signature, credential.signed_payload())
    except InvalidSignature:
        return False
    return verify_range(
        _shift(credential.age_commitment, proof.min_age),
        proof.age_proof,
        AGE_BITS,
        _context(credential, "age", proof.min_age),
    ) and verify_range(
        _shift(credential.tier_commitment, proof.min_tier),
        proof.tier_proof,
        TIER_BITS,
        _context(credential, "tier", proof.min_tier),
    )
