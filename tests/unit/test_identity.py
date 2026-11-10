"""Unit tests for TOTP multi-factor authentication and OIDC single sign-on."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from gpmorgan_forge.distributed_privacy import SecurityAccessDeniedError
from gpmorgan_forge.identity import (
    AuthenticationError,
    OidcTokenValidator,
    SsoSession,
    TotpVerifier,
    authorize,
    generate_totp_secret,
    hotp,
    totp_code,
)

ISSUER = "https://idp.forge.example/realms/forge"
AUDIENCE = "forge-core"
NOW = 1_800_000_000.0


def b64(data: bytes) -> str:
    """Base64url without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def make_token(
    claims: dict[str, Any],
    *,
    alg: str = "EdDSA",
    kid: str = "k1",
    key: Ed25519PrivateKey | rsa.RSAPrivateKey,
) -> str:
    """Build and sign a compact JWT."""
    header = b64(json.dumps({"alg": alg, "kid": kid, "typ": "JWT"}).encode())
    payload = b64(json.dumps(claims).encode())
    signing_input = f"{header}.{payload}".encode()
    if isinstance(key, Ed25519PrivateKey):
        signature = key.sign(signing_input)
    else:
        signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{payload}.{b64(signature)}"


def base_claims(**overrides: Any) -> dict[str, Any]:
    """Valid claim set for an administrator who completed MFA."""
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "user-1",
        "email": "ada@example.ng",
        "exp": NOW + 600,
        "nbf": NOW - 10,
        "nonce": "n-1",
        "groups": ["forge-admins", "unmapped-group"],
        "amr": ["pwd", "otp"],
    }
    claims.update(overrides)
    return claims


@pytest.mark.unit
def test_hotp_and_totp_match_rfc_test_vectors() -> None:
    """RFC 4226 / RFC 6238 published vectors (secret '12345678901234567890')."""
    secret = base64.b32encode(b"12345678901234567890").decode()
    expected_hotp = ["755224", "287082", "359152", "969429", "338314", "254676"]
    assert [hotp(secret, i) for i in range(6)] == expected_hotp
    # RFC 6238 Appendix B (SHA-1), 8 digits.
    assert totp_code(secret, 59, digits=8) == "94287082"
    assert totp_code(secret, 1111111109, digits=8) == "07081804"
    assert len(generate_totp_secret()) == 32


@pytest.mark.unit
def test_totp_verifier_window_replay_and_wrong_code() -> None:
    """Codes verify within the skew window, only once, and never when wrong."""
    secret = generate_totp_secret()
    verifier = TotpVerifier(window=1)
    code = totp_code(secret, NOW)
    assert verifier.verify("u1", secret, code, NOW)
    assert not verifier.verify("u1", secret, code, NOW)  # replay rejected
    assert not verifier.verify("u1", secret, "abcdef", NOW + 30)  # non-numeric can never match
    later = totp_code(secret, NOW + 30)
    assert verifier.verify("u1", secret, later, NOW + 30)  # next step accepted
    assert not verifier.verify("u1", secret, totp_code(secret, NOW - 300), NOW)  # outside window
    # Another user is tracked independently.
    assert TotpVerifier().verify("u2", secret, code, NOW + 25)  # +1 step skew tolerated


@pytest.mark.unit
def test_oidc_validation_eddsa_and_rs256() -> None:
    """Valid EdDSA and RS256 tokens yield a session with mapped roles and MFA status."""
    ed_key = Ed25519PrivateKey.generate()
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    validator = OidcTokenValidator(
        issuer=ISSUER,
        audience=AUDIENCE,
        public_keys={"k1": ed_key.public_key(), "k2": rsa_key.public_key()},
    )
    session = validator.validate(
        make_token(base_claims(), key=ed_key), now=NOW, expected_nonce="n-1"
    )
    assert session.subject == "user-1"
    assert session.roles == ("forge_admin",)
    assert session.mfa_verified is True

    rs_session = validator.validate(
        make_token(base_claims(aud=[AUDIENCE, "other"]), alg="RS256", kid="k2", key=rsa_key),
        now=NOW,
    )
    assert rs_session.roles == ("forge_admin",)


@pytest.mark.unit
def test_oidc_rejects_invalid_tokens() -> None:
    """Bad structure, algorithm, key, signature, and claims are all refused."""
    key = Ed25519PrivateKey.generate()
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    validator = OidcTokenValidator(
        issuer=ISSUER,
        audience=AUDIENCE,
        public_keys={"k1": key.public_key(), "k2": rsa_key.public_key()},
    )
    good = make_token(base_claims(), key=key)

    def rejected(token: str, match: str, **kw: Any) -> None:
        with pytest.raises(AuthenticationError, match=match):
            validator.validate(token, now=kw.pop("now", NOW), **kw)

    rejected("a.b", "Malformed")
    rejected("!!!.@@@.###", "Malformed")
    header_none = b64(json.dumps({"alg": "none", "kid": "k1"}).encode())
    rejected(f"{header_none}.{b64(b'{}')}.", "Unsupported JWT algorithm")
    rejected(make_token(base_claims(), kid="missing", key=key), "Unknown signing key")
    rejected(make_token(base_claims(), alg="RS256", kid="k1", key=key), "does not match")
    head, _payload, sig = good.split(".")
    rejected(
        f"{head}.{b64(json.dumps(base_claims(sub='evil')).encode())}.{sig}", "Invalid JWT signature"
    )
    rejected(make_token(base_claims(iss="https://evil"), key=key), "issuer")
    rejected(make_token(base_claims(aud="other"), key=key), "audience")
    rejected(make_token(base_claims(aud=None), key=key), "audience")
    rejected(make_token(base_claims(exp=NOW - 1000), key=key), "expired")
    rejected(make_token(base_claims(nbf=NOW + 1000), key=key), "not yet valid")
    rejected(make_token(base_claims(), key=key), "Nonce mismatch", expected_nonce="other")
    rejected(make_token(base_claims(sub=""), key=key), "no subject")


@pytest.mark.unit
def test_authorize_enforces_step_up_mfa_expiry_and_rbac() -> None:
    """Privileged permissions need MFA; roles come from IdP groups; expiry is enforced."""
    admin = SsoSession(
        subject="a",
        email=None,
        groups=("forge-admins",),
        amr=("pwd", "otp"),
        expires_at=NOW + 100,
        roles=("forge_admin",),
    )
    assert authorize(admin, "DDL_ADMIN", now=NOW) == "forge_admin"

    no_mfa = SsoSession(
        subject="a",
        email=None,
        groups=("forge-admins",),
        amr=("pwd",),
        expires_at=NOW + 100,
        roles=("forge_admin",),
    )
    assert no_mfa.mfa_verified is False
    with pytest.raises(SecurityAccessDeniedError, match="requires multi-factor"):
        authorize(no_mfa, "DDL_ADMIN", now=NOW)
    assert authorize(no_mfa, "WRITE_OLTP", now=NOW) == "forge_admin"  # non-privileged: no MFA

    with pytest.raises(AuthenticationError, match="Session expired"):
        authorize(admin, "DDL_ADMIN", now=NOW + 200)

    analyst = SsoSession(
        subject="b",
        email=None,
        groups=("forge-analysts",),
        amr=("otp",),
        expires_at=NOW + 100,
        roles=("forge_analyst",),
    )
    with pytest.raises(SecurityAccessDeniedError, match="No mapped role grants"):
        authorize(analyst, "DDL_ADMIN", now=NOW)

    # Multi-role users: first role that grants the permission wins (analyst denied, DPO allowed).
    multi = SsoSession(
        subject="c",
        email=None,
        groups=(),
        amr=("mfa",),
        expires_at=NOW + 100,
        roles=("forge_analyst", "forge_compliance_dpo"),
    )
    assert authorize(multi, "ANONYMISE", now=NOW) == "forge_compliance_dpo"
