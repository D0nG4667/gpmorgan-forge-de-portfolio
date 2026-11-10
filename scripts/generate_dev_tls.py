"""Generate a development CA and server certificate for TLS between GPMorgan Forge services.

Writes ``ca.crt``, ``server.crt`` and ``server.key`` into the output directory (default
``portfolio-5-distributed-cloud/infra/tls``, git-ignored). The server certificate carries every
compose service hostname in its SAN list so clients can use ``sslmode=verify-full``.

Development use only: production deployments must use a managed PKI / cloud certificate service.

Usage: ``uv run python scripts/generate_dev_tls.py [output_dir]``
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

SERVICE_HOSTNAMES: tuple[str, ...] = (
    "localhost",
    "postgres-primary",
    "postgres-replica",
    "forge-postgres-primary",
    "forge-postgres-replica",
)
VALIDITY_DAYS = 365


def _name(common_name: str) -> x509.Name:
    """Build a subject/issuer name."""
    return x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "GPMorgan Forge (development)"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


def generate(output_dir: Path) -> list[Path]:
    """Create the CA and server certificate/key pair and return the written paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("Forge Development CA"))
        .issuer_name(_name("Forge Development CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=VALIDITY_DAYS))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    server_key = ec.generate_private_key(ec.SECP256R1())
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("forge-postgres"))
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=VALIDITY_DAYS))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(h) for h in SERVICE_HOSTNAMES]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    written = {
        "ca.crt": ca_cert.public_bytes(serialization.Encoding.PEM),
        "server.crt": server_cert.public_bytes(serialization.Encoding.PEM),
        "server.key": server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    }
    paths: list[Path] = []
    for filename, data in written.items():
        path = output_dir / filename
        path.write_bytes(data)
        paths.append(path)
    return paths


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = argv if argv is not None else sys.argv[1:]
    default_dir = Path(__file__).resolve().parent.parent / "portfolio-5-distributed-cloud/infra/tls"
    for path in generate(Path(args[0]) if args else default_dir):
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
