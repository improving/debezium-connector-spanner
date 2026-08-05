#!/usr/bin/env bash
#
# run_stall_repro.sh
#
# One-shot wrapper: run with no arguments and it drives the whole placement
# stall reproduction end-to-end using placement_stall_repro.sh's idempotent
# phases, then watches the Kafka Connect worker's logs for the exact stall
# message the report described and prints a clear pass/fail summary.
#
# It does NOT tear anything down when finished - the environment (Spanner
# database, containers, connector) is left running so you can keep
# investigating. Run `./placement_stall_repro.sh down` when you're done.
#
# Usage:
#   ./run_stall_repro.sh
#
# Override any setting via environment variables before running - see
# placement_stall_repro.sh's "Configuration" section for the full list
# (PROJECT, INSTANCE, DATABASE, WORKLOAD_OPERATIONS, WORKLOAD_THREADS, ...).
# WATCH_SECONDS controls how long this script watches logs after the
# workload run before summarizing (default 60).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPRO="$SCRIPT_DIR/placement_stall_repro.sh"
WORKER_CONTAINER="${WORKER_CONTAINER:-kafka-connect-worker}"
WATCH_SECONDS="${WATCH_SECONDS:-60}"
LOG_CAPTURE="$(mktemp -t placement-stall-repro-logs.XXXXXX)"

# Captured before anything runs, so the log watch below replays the entire
# run (provisioning through workload) rather than only whatever's emitted
# after the workload script has already returned.
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "== Step 1/3: provisioning DDL + starting containers + registering connector =="
"$REPRO" up

echo
echo "== Step 2/3: running the GCSB workload =="
"$REPRO" workload || echo "(workload run reported failures - see output above; continuing to watch logs anyway)"

echo
echo "== Step 3/3: replaying $WORKER_CONTAINER logs since the run started, then watching ${WATCH_SECONDS}s more =="
# Not using GNU `timeout` here - it isn't installed by default on macOS.
docker logs -f --since "$START_TS" "$WORKER_CONTAINER" >"$LOG_CAPTURE" 2>&1 &
log_pid=$!
sleep "$WATCH_SECONDS"
kill "$log_pid" 2>/dev/null || true
wait "$log_pid" 2>/dev/null || true

echo
echo "======================================================================"
# "waiting for source(s) MoveOut" and "parents are not finished" both fire
# transiently as normal, expected steps of MoveIn/MoveOut choreography - a
# destination briefly waits for its source's MoveOut before it's cleared to
# stream, and that's not evidence of a stall on its own.
# The real signal is a partition that logs one of these waits and is *never*
# later resolved within the watch window - so track per-partition tokens
# instead of just grepping for bare pattern presence.
python3 - "$LOG_CAPTURE" "$WATCH_SECONDS" <<'PYEOF' || true
import re
import sys

log_path, watch_seconds = sys.argv[1], sys.argv[2]

waiting_pat = re.compile(r"waiting for source\(s\) MoveOut, taskUid: (\S+), partition ([^\s,]+),")
resolved_pat = re.compile(r"source\(s\) processed MoveOut, taskUid: (\S+), partition ([^\s,]+)")
pnf_pat = re.compile(r"since parents are not finished, taskUid: (\S+), partition ([^\s,]+),")
pnf_resolved_pat = re.compile(r"Task takes partition for streaming,.*taskUid: (\S+), partition ([^\s,]+)")

waiting, resolved, pnf, pnf_resolved = {}, set(), {}, set()

with open(log_path) as f:
    for line in f:
        m = waiting_pat.search(line)
        if m:
            waiting[m.group(2)] = line.rstrip()
        m = resolved_pat.search(line)
        if m:
            resolved.add(m.group(2))
        m = pnf_pat.search(line)
        if m:
            pnf[m.group(2)] = line.rstrip()
        m = pnf_resolved_pat.search(line)
        if m:
            pnf_resolved.add(m.group(2))

stuck_moveout = {tok: line for tok, line in waiting.items() if tok not in resolved}
stuck_pnf = {tok: line for tok, line in pnf.items() if tok not in pnf_resolved}
stuck = {**stuck_moveout, **stuck_pnf}

print(f"Partitions that hit a MoveIn/MoveOut wait: {len(waiting)} ({len(waiting) - len(stuck_moveout)} resolved within the watch window)")
print(f"Partitions that hit a parents-not-finished wait: {len(pnf)} ({len(pnf) - len(stuck_pnf)} resolved within the watch window)")
print()

if stuck:
    print(f"RESULT: Reproduced - {len(stuck)} partition(s) never resolved within the last {watch_seconds}s:")
    print("----------------------------------------------------------------------")
    for line in stuck.values():
        print(line)
    print("----------------------------------------------------------------------")
    sys.exit(1)
else:
    print(f"RESULT: Stall not observed - every MoveIn/MoveOut and parents-not-finished wait resolved within the last {watch_seconds}s.")
    sys.exit(0)
PYEOF
echo "Full captured log window: $LOG_CAPTURE"
echo "======================================================================"
echo
echo "Environment left running for further investigation:"
echo "  $REPRO status   # connector/task status"
echo "  $REPRO logs      # live-tail the worker"
echo "  $REPRO down      # tear everything down when done"
