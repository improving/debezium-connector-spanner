#!/usr/bin/env bash
#
# build_connector_image.sh
#
# Handles just the Java/Docker side of getting a source change into an
# image: compiling the connector, packaging the Kafka Connect plugin
# tarball, and baking it into kafka-spanner-connector:latest. Does not touch
# any running containers - run this on its own when you only need a fresh
# image (e.g. to inspect it, or before a manual container recreate), or let
# rebuild_and_redeploy.sh call this and then handle the container side.
#
#   1. mvn package -DskipTests -Passembly
#        Produces target/debezium-connector-spanner-<version>-plugin.tar.gz.
#        Plain `mvn package` (no -Passembly) does NOT produce this file, and
#        IntelliJ's "Rebuild Project" doesn't produce it either - that only
#        compiles to the IDE's own output directory.
#   2. docker build ... -t kafka-spanner-connector:latest
#        Bakes that tarball into a new image layer via
#        src/test/docker/Dockerfile's ADD instruction.
#
# Usage:
#   ./build_connector_image.sh
#
# Note: the image tag doesn't change between builds, so a container already
# running from a previous build of this image won't pick up the new one on
# its own - it needs to be recreated (see rebuild_and_redeploy.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

PROJECT_VERSION="$(cd "$REPO_ROOT" && grep -m1 -oE '<version>[^<]+</version>' pom.xml | sed -E 's#</?version>##g')"

log "== mvn package -DskipTests -Passembly (version $PROJECT_VERSION) =="
(cd "$REPO_ROOT" && mvn package -DskipTests -Passembly -q)

TARBALL="$REPO_ROOT/target/debezium-connector-spanner-${PROJECT_VERSION}-plugin.tar.gz"
if [[ ! -f "$TARBALL" ]]; then
  echo "ERROR: expected plugin tarball not found: $TARBALL" >&2
  exit 1
fi
log "Built $TARBALL ($(du -h "$TARBALL" | cut -f1))"

log "== docker build kafka-spanner-connector:latest =="
(cd "$REPO_ROOT" && docker build -f src/test/docker/Dockerfile \
  --build-arg "projectVersion=$PROJECT_VERSION" \
  -t kafka-spanner-connector:latest .)

log "Image kafka-spanner-connector:latest built from current source."
