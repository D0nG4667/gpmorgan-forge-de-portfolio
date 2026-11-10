#!/bin/bash
# Primary-side bootstrap for physical streaming replication (runs once, after schema init).
# Creates the replication role and a least-privilege read-only application role.
set -euo pipefail

: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD must be set}"
: "${POSTGRES_REPLICA_PASSWORD:?POSTGRES_REPLICA_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v repl_pw="$REPLICATION_PASSWORD" -v ro_pw="$POSTGRES_REPLICA_PASSWORD" <<-'EOSQL'
    CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD :'repl_pw';
    CREATE ROLE forge_readonly WITH LOGIN PASSWORD :'ro_pw';
    GRANT pg_read_all_data TO forge_readonly;
EOSQL

# Enforce TLS: every TCP connection (`host`) must use SSL (`hostssl`), including loopback.
sed -i 's/^host[[:space:]]/hostssl /' "$PGDATA/pg_hba.conf"
# Allow the replica to stream WAL over TLS (scram-sha-256, never trust).
echo "hostssl replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
