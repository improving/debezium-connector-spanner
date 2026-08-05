#!/usr/bin/env bash
#
# placement_stall_repro.sh
#
# Reproduction harness for a reported connector stall against a real placement
# table (MUTABLE_KEY_RANGE, six placements on the same instance partition).
# Automates everything that was previously done by hand:
#   1. Create a scratch Spanner database.
#   2. Apply the reported DDL: BenchmarkPlacementUsers table, six placements
#      (p0-p5) all on the "default" instance partition, and the "mycs"
#      change stream (partition_mode = MUTABLE_KEY_RANGE).
#   3. Start the local -Preal-connect infrastructure: the docker network, the
#      Kafka broker (docker-compose), and the kafka-connect-worker container
#      built from src/test/docker/Dockerfile.
#   4. Register the SpannerConnector against that database/change stream via
#      the Kafka Connect REST API. Credentials are never read/embedded by
#      this script - the worker container gets Application Default
#      Credentials via a read-only bind mount and GOOGLE_APPLICATION_CREDENTIALS,
#      and the connector's own JVM resolves them (DatabaseClientFactory falls
#      back to ServiceAccountCredentials.getApplicationDefault() when neither
#      gcp.spanner.credentials.json nor .path is set).
#   5. Drive load via verify/gcsb/gcsb_users_workload_runner.py with
#      --milestone placement, which populates the table's NOT NULL
#      PlacementKey column on every INSERT/UPDATE (its own built-in
#      placement-table mode - also triggers automatically whenever the
#      target table name contains "placement", which BenchmarkPlacementUsers
#      does).
#
# Usage:
#   ./placement_stall_repro.sh up          # provision DDL + start containers + register connector
#   ./placement_stall_repro.sh workload     # run the GCSB workload against the placement table
#   ./placement_stall_repro.sh logs         # tail the connect worker's logs
#   ./placement_stall_repro.sh status       # print connector + task status
#   ./placement_stall_repro.sh down         # tear everything down (including the Spanner database)
#   ./placement_stall_repro.sh redeploy     # recreate just the worker container from a new image -
#                                            # never drops the Spanner schema, so it's a fast no-op
#                                            # on an existing database, or a normal first-time
#                                            # provision if there isn't one yet
#
# Requires: gcloud (authenticated with access to PROJECT/INSTANCE), docker,
# docker compose, python3, and this repo's Maven build already producing
# target/*-plugin.tar.gz (run `mvn package -DskipTests` once beforehand so
# `docker build` on src/test/docker/Dockerfile has something to ADD).

set -euo pipefail

# ---- Configuration (override via environment) ------------------------------
PROJECT="${PROJECT:-improvingvancouver}"
INSTANCE="${INSTANCE:-spanner-kafka-connector}"
DATABASE="${DATABASE:-placement_repro}"
INSTANCE_PARTITION="${INSTANCE_PARTITION:-default}"
TABLE="${TABLE:-BenchmarkPlacementUsers}"
CHANGE_STREAM="${CHANGE_STREAM:-mycs}"
PLACEMENTS=(p0 p1 p2 p3 p4 p5)
# >1 is required to exercise cross-task sync merging (SyncEventMerger) at all -
# with a single task there's never a second TaskSyncContext whose events it
# has to reconcile, so the currentTask==null merge branch is unreachable.
TASKS_MAX="${TASKS_MAX:-2}"

DOCKER_NETWORK="${DOCKER_NETWORK:-spanner-connector-it-network}"
WORKER_IMAGE="${WORKER_IMAGE:-kafka-spanner-connector:latest}"
WORKER_CONTAINER="${WORKER_CONTAINER:-kafka-connect-worker}"
CONNECT_REST_URL="${CONNECT_REST_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-cdc-spanner-repro}"
ADC_PATH="${ADC_PATH:-$HOME/.config/gcloud/application_default_credentials.json}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/src/test/java/io/debezium/connector/spanner/util/docker-compose.yml"
CONNECTOR_CONFIG_FILE="/tmp/${CONNECTOR_NAME}-config.json"

WORKLOAD_ARGS=(
  --project "$PROJECT"
  --instance "$INSTANCE"
  --database "$DATABASE"
  --table "$TABLE"
  --milestone placement
  --bucket "${WORKLOAD_BUCKET:-placement-repro-scratch}"
  --run-id "${WORKLOAD_RUN_ID:-placement_repro_$(date +%s)}"
  --operations "${WORKLOAD_OPERATIONS:-50}"
  --threads "${WORKLOAD_THREADS:-4}"
  --key-file /nonexistent-key-file.json
)

# ---- Single-instance lock ----------------------------------------------------
# Two concurrent invocations racing DDL against the same database previously
# caused duplicate DROP/CREATE statements to queue up serially on Spanner's
# side, turning a ~3.5 minute teardown into a multi-hour one (see the
# FAILED_PRECONDITION "is not usable because it is being dropped" errors this
# produced). flock isn't available by default on macOS, so use a portable
# mkdir-based lock (mkdir is atomic on POSIX filesystems) instead.
LOCK_DIR="${LOCK_DIR:-/tmp/placement_stall_repro.lock.d}"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  if [[ -f "$LOCK_DIR/pid" ]] && kill -0 "$(cat "$LOCK_DIR/pid")" 2>/dev/null; then
    echo "ERROR: another placement_stall_repro.sh invocation (pid $(cat "$LOCK_DIR/pid")) is already running." >&2
    echo "Refusing to start a second one - concurrent invocations previously caused hours of duplicate," >&2
    echo "queued Spanner DDL operations. Wait for it to finish, or confirm it's actually dead and remove" >&2
    echo "$LOCK_DIR yourself if you're sure." >&2
    exit 1
  fi
  echo "Found a stale lock (owning process no longer running) - removing it." >&2
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi
echo $$ >"$LOCK_DIR/pid"
trap 'rm -rf "$LOCK_DIR"' EXIT

# ---- Helpers -----------------------------------------------------------------
log() { echo "[$(date '+%H:%M:%S')] $*"; }

ddl_update() {
  gcloud spanner databases ddl update "$DATABASE" \
    --instance="$INSTANCE" --project="$PROJECT" --ddl="$1"
}

wait_for_connect_rest() {
  log "Waiting for Kafka Connect REST API at $CONNECT_REST_URL ..."
  for _ in $(seq 1 40); do
    if curl -s -o /dev/null -w '%{http_code}' "$CONNECT_REST_URL/connectors" | grep -q 200; then
      log "Kafka Connect worker is up."
      return 0
    fi
    sleep 3
  done
  echo "Kafka Connect worker did not become healthy in time" >&2
  return 1
}

# ---- Phases ------------------------------------------------------------------
# All of these are safe to re-run: each checks whether its resource already
# exists before creating it, so running the whole "up" sequence twice in a
# row (e.g. from run_stall_repro.sh) just no-ops on anything already there.
current_ddl() {
  gcloud spanner databases ddl describe "$DATABASE" --instance="$INSTANCE" --project="$PROJECT" 2>/dev/null || true
}

provision_spanner() {
  if gcloud spanner databases describe "$DATABASE" --instance="$INSTANCE" --project="$PROJECT" >/dev/null 2>&1; then
    log "Database $DATABASE already exists, continuing."
  else
    log "Creating Spanner database $DATABASE on $INSTANCE ..."
    gcloud spanner databases create "$DATABASE" --instance="$INSTANCE" --project="$PROJECT"
  fi

  local ddl
  ddl="$(current_ddl)"

  if echo "$ddl" | grep -qE "CREATE TABLE $TABLE($|[^A-Za-z0-9_])"; then
    log "Table $TABLE already exists, skipping."
  else
    log "Creating table $TABLE ..."
    ddl_update "
CREATE TABLE $TABLE (
  UserId INT64 NOT NULL,
  PlacementKey STRING(MAX) NOT NULL PLACEMENT KEY,
  UserName STRING(256),
  UserEmail STRING(256),
  AccountBalance NUMERIC,
  Metadata JSON,
  LastLogin TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  IsActive BOOL,
  BinarySignature BYTES(MAX)
) PRIMARY KEY (UserId)"
    ddl="$(current_ddl)"
  fi

  local args=()
  for p in "${PLACEMENTS[@]}"; do
    if echo "$ddl" | grep -qE "CREATE PLACEMENT $p($|[^A-Za-z0-9_])"; then
      log "Placement $p already exists, skipping."
    else
      args+=(--ddl="CREATE PLACEMENT $p OPTIONS (instance_partition = '$INSTANCE_PARTITION')")
    fi
  done
  if [ "${#args[@]}" -gt 0 ]; then
    log "Creating placements: ${args[*]} ..."
    gcloud spanner databases ddl update "$DATABASE" --instance="$INSTANCE" --project="$PROJECT" "${args[@]}"
    ddl="$(current_ddl)"
  fi

  if echo "$ddl" | grep -qE "CREATE CHANGE STREAM $CHANGE_STREAM($|[^A-Za-z0-9_])"; then
    log "Change stream $CHANGE_STREAM already exists, skipping."
  else
    log "Creating change stream $CHANGE_STREAM (MUTABLE_KEY_RANGE) ..."
    ddl_update "CREATE CHANGE STREAM $CHANGE_STREAM FOR $TABLE OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')"
  fi
}

ensure_worker_image() {
  if docker image inspect "$WORKER_IMAGE" >/dev/null 2>&1; then
    return 0
  fi
  log "Image $WORKER_IMAGE not found - building it via 'mvn package -DskipTests' ..."
  (cd "$REPO_ROOT" && mvn package -DskipTests)
}

start_containers() {
  log "Creating docker network $DOCKER_NETWORK (ignored if it already exists) ..."
  docker network create "$DOCKER_NETWORK" 2>/dev/null || true

  log "Starting Kafka broker via docker compose (no-op if already running) ..."
  docker compose -f "$COMPOSE_FILE" up -d

  ensure_worker_image

  if docker ps --filter "name=^${WORKER_CONTAINER}$" --format '{{.Names}}' | grep -q "$WORKER_CONTAINER"; then
    log "Kafka Connect worker container already running, skipping."
  else
    docker rm -f "$WORKER_CONTAINER" >/dev/null 2>&1 || true
    log "Starting Kafka Connect worker ($WORKER_CONTAINER) with ADC mounted read-only from $ADC_PATH ..."
    docker run -d --name "$WORKER_CONTAINER" \
      --network "$DOCKER_NETWORK" \
      -p 8083:8083 \
      -v "$ADC_PATH:/tmp/adc.json:ro" \
      -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json \
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
      -e CONNECT_REST_ADVERTISED_HOST_NAME="$WORKER_CONTAINER" \
      -e CONNECT_PLUGIN_PATH=/usr/share/java \
      "$WORKER_IMAGE"
  fi

  wait_for_connect_rest
}

register_connector() {
  log "Writing connector config to $CONNECTOR_CONFIG_FILE ..."
  cat >"$CONNECTOR_CONFIG_FILE" <<EOF
{
  "connector.class": "io.debezium.connector.spanner.SpannerConnector",
  "tasks.max": "$TASKS_MAX",
  "gcp.spanner.project.id": "$PROJECT",
  "gcp.spanner.instance.id": "$INSTANCE",
  "gcp.spanner.database.id": "$DATABASE",
  "gcp.spanner.change.stream": "$CHANGE_STREAM",
  "connector.spanner.sync.kafka.bootstrap.servers": "broker:29092",
  "connector.spanner.sync.publisher.wait.timeout": "5000",
  "topic.creation.default.partitions": "10",
  "topic.creation.default.replication.factor": "1",
  "heartbeat.interval.ms": "1000",
  "connector.spanner.max.missed.heartbeats": "600"
}
EOF

  log "Registering connector $CONNECTOR_NAME ..."
  curl -s -X PUT -H "Content-Type: application/json" \
    "$CONNECT_REST_URL/connectors/$CONNECTOR_NAME/config" \
    -d @"$CONNECTOR_CONFIG_FILE"
  echo
  sleep 5
  curl -s "$CONNECT_REST_URL/connectors/$CONNECTOR_NAME/status"
  echo
}

run_workload() {
  log "Running gcsb_users_workload_runner.py against $TABLE (--milestone placement) ..."
  python3 "$REPO_ROOT/verify/gcsb/gcsb_users_workload_runner.py" "${WORKLOAD_ARGS[@]}"
}

print_status() {
  curl -s "$CONNECT_REST_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
}

tail_logs() {
  docker logs -f "$WORKER_CONTAINER"
}

teardown() {
  log "Deleting connector $CONNECTOR_NAME (if present) ..."
  curl -s -X DELETE "$CONNECT_REST_URL/connectors/$CONNECTOR_NAME" >/dev/null || true

  log "Stopping and removing containers ..."
  docker rm -f "$WORKER_CONTAINER" >/dev/null 2>&1 || true
  docker compose -f "$COMPOSE_FILE" down >/dev/null 2>&1 || true
  docker network rm "$DOCKER_NETWORK" >/dev/null 2>&1 || true

  # Deleting the database removes its change stream, table, and placements
  # along with it - dropping them individually first (as this used to do) is
  # pure overhead: 6 separate DROP PLACEMENT calls alone have historically
  # taken anywhere from ~3.5 minutes to multiple hours (see the concurrent-
  # invocation warning above), for no benefit over just deleting the database.
  log "Deleting scratch database $DATABASE (also removes its change stream, table, and placements) ..."
  gcloud spanner databases delete "$DATABASE" --instance="$INSTANCE" --project="$PROJECT" --quiet 2>/dev/null || true

  log "Teardown complete."
}

redeploy() {
  # Container-only refresh for picking up a new Java build: recreates just the
  # worker container (and re-registers the connector against it). A Java
  # source change has no bearing on the Spanner schema, so there's no reason
  # to pay for the slow DROP/CREATE PLACEMENT cycle (which has taken anywhere
  # from ~3.5 minutes to multiple hours) just to pick it up - use 'down' then
  # 'up' instead when you actually need a fresh database.
  #
  # Still calls provision_spanner() first: it's pure create-if-missing (never
  # drops anything), so it's a handful of fast existence checks and a no-op
  # on the common case where the schema already exists - but it means this
  # also works correctly on a first-ever run with no prior 'up', instead of
  # registering a connector against a database that doesn't exist yet and
  # leaving the task to fail silently at runtime.
  log "Redeploying worker container only (won't touch an already-existing Spanner database/schema) ..."
  provision_spanner

  log "Deleting connector $CONNECTOR_NAME (if present) ..."
  curl -s -X DELETE "$CONNECT_REST_URL/connectors/$CONNECTOR_NAME" >/dev/null || true

  log "Removing worker container (leaving broker and Spanner database as-is) ..."
  docker rm -f "$WORKER_CONTAINER" >/dev/null 2>&1 || true

  start_containers
  register_connector
}

# ---- Entry point --------------------------------------------------------------
case "${1:-}" in
  up)
    provision_spanner
    start_containers
    register_connector
    ;;
  workload)
    run_workload
    ;;
  status)
    print_status
    ;;
  logs)
    tail_logs
    ;;
  down)
    teardown
    ;;
  redeploy)
    redeploy
    ;;
  *)
    echo "Usage: $0 {up|workload|status|logs|down|redeploy}" >&2
    exit 1
    ;;
esac
