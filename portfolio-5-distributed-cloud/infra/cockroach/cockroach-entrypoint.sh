#!/bin/bash
# CockroachDB entrypoint wrapper: starts single-node cluster and applies
# /opt/forge/init-multi-region.sql once SQL endpoint is ready.
set -euo pipefail

(
  until /cockroach/cockroach sql --insecure --execute="SELECT 1;" >/dev/null 2>&1; do
    sleep 2
  done
  if [ -f /opt/forge/init-multi-region.sql ]; then
    echo "Applying CockroachDB multi-region schema: /opt/forge/init-multi-region.sql"
    /cockroach/cockroach sql --insecure -f /opt/forge/init-multi-region.sql >/dev/null
  fi
) &

exec /cockroach/cockroach "$@"
