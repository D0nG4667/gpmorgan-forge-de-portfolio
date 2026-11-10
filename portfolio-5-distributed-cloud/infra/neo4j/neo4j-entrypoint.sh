#!/bin/bash
# Neo4j 5.26 LTS entrypoint wrapper: launches a background bootstrapper that executes
# /opt/forge/neo4j_fraud_graph.cypher via cypher-shell once Bolt is ready.
set -euo pipefail

(
  NEO_USER="${NEO4J_AUTH%%/*}"
  NEO_PASS="${NEO4J_AUTH#*/}"
  until cypher-shell -u "$NEO_USER" -p "$NEO_PASS" "RETURN 1;" >/dev/null 2>&1; do
    sleep 3
  done
  if [ -f /opt/forge/neo4j_fraud_graph.cypher ]; then
    echo "Seeding Neo4j 5.26 AML fraud graph from /opt/forge/neo4j_fraud_graph.cypher..."
    cypher-shell -u "$NEO_USER" -p "$NEO_PASS" -f /opt/forge/neo4j_fraud_graph.cypher >/dev/null
  fi
) &

if [ $# -eq 0 ]; then
  set -- neo4j
fi
exec /startup/docker-entrypoint.sh "$@"
