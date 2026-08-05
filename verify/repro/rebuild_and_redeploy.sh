#!/usr/bin/env bash
#
# rebuild_and_redeploy.sh
#
# Takes a local source change (e.g. editing SyncEventMerger.java) all the way
# into the running kafka-connect-worker container. IntelliJ's "Rebuild
# Project" only compiles to the IDE's own output directory - it does not
# touch any of the steps below, so a plain rebuild in the IDE never reaches
# the container. This script runs the full chain:
#
#   1. build_connector_image.sh
#        Packages the plugin tarball and bakes it into a new
#        kafka-spanner-connector:latest image layer. See that script for why
#        both the Maven -Passembly profile and a Docker rebuild are needed.
#   2. placement_stall_repro.sh redeploy
#        Recreates just the worker container (and re-registers the
#        connector) so kafka-connect-worker actually starts from the new
#        image. The image tag doesn't change between builds, so a plain
#        `docker restart` would keep using the old image ID. This
#        deliberately does NOT touch the Spanner database/schema - a Java
#        code change has no bearing on it, and dropping/recreating the table,
#        placements, and change stream is the slow part (anywhere from
#        ~3.5 minutes to multiple hours observed). Use
#        `placement_stall_repro.sh down` then `up` yourself if you actually
#        need a fresh database.
#
# Usage:
#   ./rebuild_and_redeploy.sh
#
# If you only need a fresh image and don't need the container recreated yet
# (e.g. to inspect the built jar), run build_connector_image.sh directly
# instead. Step 2 here is exactly placement_stall_repro.sh's own 'redeploy'
# phase, so it inherits that script's idempotency, its single-instance lock,
# and the same PROJECT/INSTANCE/DATABASE/... environment variable overrides.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPRO="$SCRIPT_DIR/placement_stall_repro.sh"
BUILD="$SCRIPT_DIR/build_connector_image.sh"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "== Step 1/2: building the connector image (Java + Docker) =="
"$BUILD"

log "== Step 2/2: recreating containers so the worker starts from the new image =="
"$REPRO" redeploy

log "Done. kafka-connect-worker is now running the image built from your current source."
log "Run '$SCRIPT_DIR/run_stall_repro.sh' (or '$REPRO workload') to exercise it."
