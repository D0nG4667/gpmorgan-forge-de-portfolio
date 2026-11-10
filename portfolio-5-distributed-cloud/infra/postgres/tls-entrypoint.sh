#!/bin/bash
# Install TLS material with the ownership/permissions PostgreSQL requires, then exec the command.
# Bind-mounted files from the host cannot satisfy PostgreSQL's key-permission check, so they are
# copied into a postgres-owned directory first.
set -euo pipefail

TLS_SRC="${TLS_SRC:-/run/forge-tls}"
TLS_DST="/var/lib/postgresql-tls"

install -d -m 700 -o postgres -g postgres "$TLS_DST"
if [ ! -f "$TLS_SRC/server.key" ] || [ ! -f "$TLS_SRC/server.crt" ] || [ ! -f "$TLS_SRC/ca.crt" ]; then
  echo "ERROR: Missing TLS material in $TLS_SRC. Run 'uv run python scripts/generate_dev_tls.py' on the host before starting containers." >&2
  exit 1
fi
install -m 600 -o postgres -g postgres "$TLS_SRC/server.key" "$TLS_DST/server.key"
install -m 644 -o postgres -g postgres "$TLS_SRC/server.crt" "$TLS_DST/server.crt"
install -m 644 -o postgres -g postgres "$TLS_SRC/ca.crt" "$TLS_DST/ca.crt"

exec "$@"
