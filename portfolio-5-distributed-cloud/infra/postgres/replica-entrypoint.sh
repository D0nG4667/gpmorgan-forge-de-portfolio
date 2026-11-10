#!/bin/bash
# Hot-standby bootstrap: clone the primary with pg_basebackup on first start (over TLS), then run as
# a read-only streaming replica (standby.signal + primary_conninfo written by -R).
set -euo pipefail

: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD must be set}"
PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"
PRIMARY_HOST="${PRIMARY_HOST:-postgres-primary}"
TLS_CA="/var/lib/postgresql-tls/ca.crt"

/bin/bash /opt/forge/tls-entrypoint.sh true

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  chown -R postgres:postgres "$(dirname "$PGDATA")"
  chmod 700 "$PGDATA"
  until pg_isready -h "$PRIMARY_HOST" -U replicator >/dev/null 2>&1; do
    echo "waiting for primary ${PRIMARY_HOST}..."
    sleep 2
  done
  echo "cloning ${PRIMARY_HOST} with pg_basebackup (sslmode=verify-full)"
  gosu postgres env PGPASSWORD="$REPLICATION_PASSWORD" PGSSLMODE=verify-full \
    PGSSLROOTCERT="$TLS_CA" pg_basebackup \
    -h "$PRIMARY_HOST" -U replicator -D "$PGDATA" \
    -R -X stream -C -S forge_replica_slot --checkpoint=fast --progress
fi

exec docker-entrypoint.sh postgres -c hot_standby=on -c hot_standby_feedback=on \
  -c ssl=on -c ssl_cert_file=/var/lib/postgresql-tls/server.crt \
  -c ssl_key_file=/var/lib/postgresql-tls/server.key \
  -c ssl_ca_file="$TLS_CA" -c ssl_min_protocol_version=TLSv1.3
