#!/bin/bash
# Cassandra 5.0 entrypoint wrapper: launches a background bootstrapper that applies
# /docker-entrypoint-initdb.d/*.cql once the CQL native transport port (9042) is ready.
set -euo pipefail

(
  until cqlsh --request-timeout=60 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
    sleep 3
  done
  ACTUAL_DC=$(cqlsh --request-timeout=60 -e "SELECT data_center FROM system.local;" | awk 'NR==4 {gsub(/[[:space:]]/, "", $0); print $0}')
  until cqlsh --request-timeout=60 -e "DESCRIBE TABLE forge_timeline.customer_transaction_timeline" >/dev/null 2>&1; do
    for f in /docker-entrypoint-initdb.d/*.cql; do
      [ -e "$f" ] || continue
      echo "Applying Cassandra 5.0 CQL schema: $f (datacenter=${ACTUAL_DC:-dc_lagos_ng})"
      if [ -n "${ACTUAL_DC:-}" ] && [ "$ACTUAL_DC" != "dc_lagos_ng" ]; then
        sed "s/dc_lagos_ng/$ACTUAL_DC/g" "$f" | cqlsh --request-timeout=60 || true
      else
        cqlsh --request-timeout=60 -f "$f" || true
      fi
    done
    sleep 3
  done
) &

exec docker-entrypoint.sh "$@"
