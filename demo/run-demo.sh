#!/usr/bin/env bash
# Runs the full MUTABLE_KEY_RANGE change stream demo end to end: starts Kafka Connect, creates
# the demo schema, deploys the connector, runs the deterministic order sequence (including a
# forced key-range split), consumes and validates the resulting events, then tears everything
# down - Kafka Connect/broker containers and the Spanner objects the demo created.
#
# See demo/README.md for prerequisites (real Spanner credentials, the connector image) and for
# running individual steps by hand.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}"

PROJECT_ID="${GCP_SPANNER_PROJECT_ID:-improvingvancouver}"
INSTANCE_ID="${GCP_SPANNER_INSTANCE_ID:-spanner-kafka-connector}"
CONNECT_CONTAINER_NAME="${DEMO_CONNECT_CONTAINER_NAME:-kafka-connect-worker}"
SPANNER_ARGS=(-Dgcp.spanner.project.id="${PROJECT_ID}" -Dgcp.spanner.instance.id="${INSTANCE_ID}")

# Always try to stop the Kafka Connect worker/broker on exit, success or failure, so a failed
# run doesn't leave containers running. Spanner-side cleanup happens inside demo-validate
# itself; if an earlier step fails before that runs, the next run's schema phase cleans up any
# leftover table/change stream defensively (see DemoDataGenerator).
# Absolute path here (not "./stop-kafka-connect.sh"): this trap fires after the `cd ..` below,
# so a relative path would no longer resolve from the repo root.
cleanup_containers() {
  "${SCRIPT_DIR}/stop-kafka-connect.sh" || true
}
trap cleanup_containers EXIT

./start-kafka-connect.sh

cd ..

echo ""
echo "=== Creating demo schema ==="
mvn -q -Pdemo exec:java@demo-schema "${SPANNER_ARGS[@]}"

echo ""
echo "=== Deploying connector ==="
mvn -q -Pdemo exec:java@demo-connect-deploy "${SPANNER_ARGS[@]}" -Ddemo.connect.container.name="${CONNECT_CONTAINER_NAME}"

echo ""
echo "=== Running order sequence ==="
mvn -q -Pdemo exec:java@demo-events "${SPANNER_ARGS[@]}"

echo ""
echo "=== Consuming and validating ==="
mvn -q -Pdemo exec:java@demo-validate "${SPANNER_ARGS[@]}"

echo ""
echo "=== Demo complete ==="
