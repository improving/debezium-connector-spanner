#!/usr/bin/env bash
# Starts the local Kafka broker (via the same docker-compose.yml the integration tests use)
# and a real, REST-reachable Kafka Connect worker, for the standalone MUTABLE_KEY_RANGE demo -
# without invoking `mvn verify`/the failsafe lifecycle. Mirrors the `real-connect` Maven
# profile's docker-maven-plugin/exec-maven-plugin executions in pom.xml exactly (env for env),
# so the demo behaves the same way the integration tests' real-connect mode does.
#
# Requires the connector plugin image to already be built: `mvn package -DskipTests` from the
# repo root (re-run after any connector source change).
set -euo pipefail
cd "$(dirname "$0")"

NETWORK_NAME="${DEMO_DOCKER_NETWORK:-spanner-connector-it-network}"
COMPOSE_FILE="../src/test/java/io/debezium/connector/spanner/util/docker-compose.yml"
CONNECT_IMAGE="${DEMO_CONNECT_IMAGE:-kafka-spanner-connector:latest}"
CONNECT_CONTAINER_NAME="${DEMO_CONNECT_CONTAINER_NAME:-kafka-connect-worker}"
CONNECT_REST_PORT="${DEMO_CONNECT_REST_PORT:-8083}"
READY_TIMEOUT_SECONDS="${DEMO_CONNECT_READY_TIMEOUT_SECONDS:-120}"

echo "Creating Docker network ${NETWORK_NAME} (if it doesn't already exist)..."
docker network create "${NETWORK_NAME}" 2>/dev/null || true

echo "Starting Kafka broker via docker compose..."
docker compose -f "${COMPOSE_FILE}" up -d

echo "Starting Kafka Connect worker (${CONNECT_IMAGE})..."
docker rm -f "${CONNECT_CONTAINER_NAME}" >/dev/null 2>&1 || true
docker run -d --name "${CONNECT_CONTAINER_NAME}" \
  --network "${NETWORK_NAME}" \
  -p "${CONNECT_REST_PORT}:8083" \
  -e CONNECT_BOOTSTRAP_SERVERS=broker:29092 \
  -e CONNECT_GROUP_ID=spanner-real-connect-cluster \
  -e CONNECT_CONFIG_STORAGE_TOPIC=_kafka-connect-configs \
  -e CONNECT_OFFSET_STORAGE_TOPIC=_kafka-connect-offsets \
  -e CONNECT_STATUS_STORAGE_TOPIC=_kafka-connect-status \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=true \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=true \
  -e CONNECT_REST_PORT=8083 \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-worker \
  -e CONNECT_PLUGIN_PATH=/usr/share/java \
  "${CONNECT_IMAGE}"

echo "Waiting up to ${READY_TIMEOUT_SECONDS}s for the Connect REST API to become ready..."
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until curl -sf "http://localhost:${CONNECT_REST_PORT}/connectors" >/dev/null 2>&1; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "Timed out waiting for Kafka Connect worker to become ready. Check 'docker logs ${CONNECT_CONTAINER_NAME}'." >&2
    exit 1
  fi
  sleep 2
done

echo "Kafka Connect worker is ready at http://localhost:${CONNECT_REST_PORT}"
