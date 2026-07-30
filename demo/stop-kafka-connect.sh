#!/usr/bin/env bash
# Tears down the Kafka Connect worker and the Kafka broker started by start-kafka-connect.sh.
# Leaves the shared spanner-connector-it-network Docker network alone, since integration test
# runs reuse it too.
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE_FILE="../src/test/java/io/debezium/connector/spanner/util/docker-compose.yml"
CONNECT_CONTAINER_NAME="${DEMO_CONNECT_CONTAINER_NAME:-kafka-connect-worker}"

echo "Stopping Kafka Connect worker..."
docker rm -f "${CONNECT_CONTAINER_NAME}" >/dev/null 2>&1 || true

echo "Stopping Kafka broker..."
docker compose -f "${COMPOSE_FILE}" down

echo "Stopped."
