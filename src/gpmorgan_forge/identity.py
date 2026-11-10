"""Authentication for GPMorgan Forge: TOTP multi-factor auth and OIDC single sign-on.

* ``TotpVerifier`` implements RFC 6238 time-based one-time passwords (HMAC-SHA1, 30 s step) with
  clock-skew windows, constant-time comparison, and per-user replay protection.
* ``OidcTokenValidator`` validates OpenID Connect ID tokens (JWT compact serialisation) signed with
  RS256 or EdDSA: algorithm allow-list (never ``none``), key lookup by ``kid``, and ``iss``/``aud``/
  ``exp``/``nbf``/``nonce`` checks. IdP groups map to database roles.
* ``authorize`` enforces step-up MFA (the OIDC ``amr`` claim must contain ``mfa`` or ``otp``) for
  privileged permissions before delegating to ``AccessControlEngine`` (RBAC/ABAC/RLS).

An external identity provider (for example Keycloak or Entra ID) issues the tokens; this module is
the relying-party side that the application and database gateway run.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import struct
from dataclasses import dataclass, field
from typing import Any, Final

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from gpmorgan_forge.distributed_privacy import AccessControlEngine, SecurityAccessDeniedError

TOTP_STEP_SECONDS: Final[int] = 30
TOTP_DIGITS: Final[int] = 6
MFA_AMR_VALUES: Final[frozenset[str]] = frozenset({"mfa", "otp"})
MFA_REQUIRED_PERMISSIONS: Final[frozenset[str]] = frozenset(
    {"DDL_ADMIN", "ANONYMISE", "READ_RAW", "READ_AUDIT", "RESOLVE_ALERT"}
)
GROUP_ROLE_MAP: Final[dict[str, str]] = {
    "forge-admins": "forge_admin",
    "forge-services": "forge_core_service",
    "forge-dpo": "forge_compliance_dpo",
    "forge-analysts": "forge_analyst",
    "forge-customers": "forge_customer",
}
JWT_PARTS: Final[int] = 3
ALLOWED_JWT_ALGORITHMS: Final[frozenset[str]] = frozenset({"RS256", "EdDSA"})


class AuthenticationError(PermissionError):
    """Raised when a token, one-time password, or session fails authentication."""


def generate_totp_secret() -> str:
    """Return a fresh 160-bit base32 TOTP shared secret."""
    return base64.b32encode(secrets.token_bytes(20)).decode("ascii")


def totp_code(secret_b32: str, timestamp: float, *, digits: int = TOTP_DIGITS) -> str:
    """Compute the RFC 6238 TOTP for ``timestamp`` (HOTP counter = floor(t / 30))."""
    return hotp(secret_b32, int(timestamp // TOTP_STEP_SECONDS), digits=digits)


def hotp(secret_b32: str, counter: int, *, digits: int = TOTP_DIGITS) -> str:
    """Compute the RFC 4226 HOTP value for ``counter``."""
    key = base64.b32decode(secret_b32, casefold=True)
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(binary % 10**digits).zfill(digits)


class TotpVerifier:
    """Verifies TOTP codes with a skew window and single-use (replay-protected) counters."""

    def __init__(self, window: int = 1) -> None:
        """Accept codes from ``window`` steps before and after the current step."""
        self._window = window
        self._last_used_counter: dict[str, int] = {}

    def verify(self, user_id: str, secret_b32: str, code: str, now: float) -> bool:
        """Return ``True`` once per valid code; a replayed or stale code is rejected."""
        current = int(now // TOTP_STEP_SECONDS)
        last_used = self._last_used_counter.get(user_id, -1)
        for counter in range(current - self._window, current + self._window + 1):
            if counter <= last_used:
                continue
            if hmac.compare_digest(hotp(secret_b32, counter), code):
                self._last_used_counter[user_id] = counter
                return True
        return False


@dataclass(frozen=True)
class SsoSession:
    """Authenticated principal derived from a validated OIDC ID token."""

    subject: str
    email: str | None
    groups: tuple[str, ...]
    amr: tuple[str, ...]
    expires_at: float
    roles: tuple[str, ...] = field(default=())

    @property
    def mfa_verified(self) -> bool:
        """Whether the IdP asserted a second factor (``amr`` contains ``mfa`` or ``otp``)."""
        return bool(MFA_AMR_VALUES.intersection(self.amr))


def _b64url_decode(segment: str) -> bytes:
    """Decode a base64url segment with optional missing padding."""
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


class OidcTokenValidator:
    """Relying-party validation of OIDC ID tokens against trusted issuer keys."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        public_keys: dict[str, RSAPublicKey | Ed25519PublicKey],
        leeway_seconds: int = 60,
    ) -> None:
        """Trust ``public_keys`` (by ``kid``) for ``issuer`` tokens addressed to ``audience``."""
        self._issuer = issuer
        self._audience = audience
        self._public_keys = public_keys
        self._leeway = leeway_seconds

    def validate(self, token: str, *, now: float, expected_nonce: str | None = None) -> SsoSession:
        """Verify signature and claims; return the session or raise ``AuthenticationError``."""
        parts = token.split(".")
        if len(parts) != JWT_PARTS:
            raise AuthenticationError("Malformed JWT.")
        try:
            header = json.loads(_b64url_decode(parts[0]))
            claims = json.loads(_b64url_decode(parts[1]))
            signature = _b64url_decode(parts[2])
        except (ValueError, binascii.Error) as exc:
            raise AuthenticationError("Malformed JWT.") from exc

        algorithm = header.get("alg")
        if algorithm not in ALLOWED_JWT_ALGORITHMS:
            raise AuthenticationError(f"Unsupported JWT algorithm: {algorithm}.")
        public_key = self._public_keys.get(str(header.get("kid")))
        if public_key is None:
            raise AuthenticationError("Unknown signing key.")
        self._verify_signature(algorithm, public_key, f"{parts[0]}.{parts[1]}".encode(), signature)
        return self._validate_claims(claims, now, expected_nonce)

    @staticmethod
    def _verify_signature(
        algorithm: str,
        public_key: RSAPublicKey | Ed25519PublicKey,
        signing_input: bytes,
        signature: bytes,
    ) -> None:
        """Check the signature using the algorithm that matches the trusted key type."""
        try:
            if algorithm == "RS256" and isinstance(public_key, RSAPublicKey):
                public_key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
            elif algorithm == "EdDSA" and isinstance(public_key, Ed25519PublicKey):
                public_key.verify(signature, signing_input)
            else:
                raise AuthenticationError("JWT algorithm does not match the trusted key type.")
        except InvalidSignature as exc:
            raise AuthenticationError("Invalid JWT signature.") from exc

    def _validate_claims(
        self, claims: dict[str, Any], now: float, expected_nonce: str | None
    ) -> SsoSession:
        """Validate registered claims and build the session."""
        if claims.get("iss") != self._issuer:
            raise AuthenticationError("Unexpected token issuer.")
        audience = claims.get("aud")
        audiences = [audience] if isinstance(audience, str) else list(audience or [])
        if self._audience not in audiences:
            raise AuthenticationError("Token audience mismatch.")
        expires_at = float(claims.get("exp", 0))
        if now - self._leeway >= expires_at:
            raise AuthenticationError("Token expired.")
        if "nbf" in claims and now + self._leeway < float(claims["nbf"]):
            raise AuthenticationError("Token not yet valid.")
        if expected_nonce is not None and claims.get("nonce") != expected_nonce:
            raise AuthenticationError("Nonce mismatch (possible replay).")
        subject = claims.get("sub")
        if not subject:
            raise AuthenticationError("Token has no subject.")

        groups = tuple(str(g) for g in claims.get("groups", []))
        return SsoSession(
            subject=str(subject),
            email=claims.get("email"),
            groups=groups,
            amr=tuple(str(a) for a in claims.get("amr", [])),
            expires_at=expires_at,
            roles=tuple(GROUP_ROLE_MAP[g] for g in groups if g in GROUP_ROLE_MAP),
        )


def authorize(
    session: SsoSession,
    permission: str,
    *,
    now: float,
    actor_customer_id: str | None = None,
    target_customer_id: str | None = None,
    actor_country: str | None = None,
    resource_country: str | None = None,
) -> str:
    """Authorise ``permission`` for an SSO session; returns the role that granted it.

    Enforces session expiry and step-up MFA for privileged permissions, then asks the RBAC/ABAC/RLS
    engine for each mapped role in turn.
    """
    if now >= session.expires_at:
        raise AuthenticationError("Session expired.")
    if permission in MFA_REQUIRED_PERMISSIONS and not session.mfa_verified:
        raise SecurityAccessDeniedError(f"{permission} requires multi-factor authentication.")
    for role in session.roles:
        try:
            AccessControlEngine.check_access(
                role=role,
                permission=permission,
                actor_customer_id=actor_customer_id,
                target_customer_id=target_customer_id,
                actor_country=actor_country,
                resource_country=resource_country,
            )
        except SecurityAccessDeniedError:
            continue
        return role
    raise SecurityAccessDeniedError(f"No mapped role grants {permission} to {session.subject}.")
