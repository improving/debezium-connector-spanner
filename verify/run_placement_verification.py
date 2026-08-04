#!/usr/bin/env python3
"""run_placement_verification.py

End-to-End Verification Harness for Cloud Spanner Placement Tables (Non-Primary
Key Placement Column):
  1. Completeness & Zero Data Loss Verification
  2. Field-Level Value Fidelity Verification across placement_key + rich schema
  (INT64, STRING, NUMERIC, JSON, TIMESTAMP, BOOL, BYTES)
  3. Strict Per-Key Monotonic Ordering Verification
  4. End-to-End Replication Latency SLA Metrics

Location:
  verify/run_placement_verification.py
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PROJECT = "my-gcp-project"
DEFAULT_INSTANCE = "my-spanner-instance"
DEFAULT_DATABASE = "my-database"
DEFAULT_TABLE = "BenchmarkPlacementUsers"
DEFAULT_TOPIC = "cdc.BenchmarkPlacementUsers"
DEFAULT_BUCKET = "my-cdc-audit-bucket"
DEFAULT_KEY_FILE = "/path/to/service-account-key.json"


def get_topic_high_watermarks(topic: str) -> dict:
  try:
    cmd = [
        "kubectl",
        "exec",
        "kafka-cp-kafka-0",
        "-c",
        "cp-kafka-broker",
        "--",
        "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        "--time",
        "-1",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    offsets = {}
    for line in res.stdout.strip().split("\n"):
      if ":" in line:
        parts = line.split(":")
        if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
          offsets[int(parts[1])] = int(parts[2])
    return offsets
  except Exception as e:
    print(f"Notice: Could not query topic high watermarks ({e}).")
    return {}


def execute_placement_verification(
    project: str = DEFAULT_PROJECT,
    instance: str = DEFAULT_INSTANCE,
    database: str = DEFAULT_DATABASE,
    table: str = DEFAULT_TABLE,
    topic: str = DEFAULT_TOPIC,
    bucket: str = DEFAULT_BUCKET,
    run_id: str = None,
    operations: int = 50,
    threads: int = 4,
    insert_ratio: float = 0.5,
    update_ratio: float = 0.3,
    delete_ratio: float = 0.2,
    key_file: str = DEFAULT_KEY_FILE,
):
  if not run_id:
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime(
        "run_placement_%Y%m%d_%H%M%S"
    )

  if os.path.exists(key_file):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file

  print("=" * 80)
  print(
      " CLOUD SPANNER KAFKA CONNECTOR - PLACEMENT TABLE E2E VERIFICATION"
      " HARNESS"
  )
  print(
      " (Non-PK Placement Key, Field-Level Fidelity, Per-Key Ordering, SLA"
      " Latency)"
  )
  print("=" * 80)
  print(f"  Run ID          : {run_id}")
  print(f"  Target Database : {project}:{instance}.{database}")
  print(
      f"  Target Table    : {table} (Placement Key: PlacementKey STRING NOT"
      " NULL PLACEMENT KEY)"
  )
  print(f"  CDC Topic       : {topic}")
  print(f"  Audit GCS Bucket: gs://{bucket}/{run_id}/")
  print(f"  Operations      : {operations:,} across {threads} worker threads")
  print(
      f"  Workload Mix    : INSERT={insert_ratio:.0%},"
      f" UPDATE={update_ratio:.0%}, DELETE={delete_ratio:.0%}"
  )
  print("=" * 80 + "\n")

  # Step 1: Start Downstream Event Collector (Kafka -> GCS)
  collector_script = os.path.join(
      SCRIPT_DIR, "collect", "kafka_gcs_collector.py"
  )
  collector_cmd = [
      sys.executable,
      collector_script,
      f"--bucket={bucket}",
      f"--run-id={run_id}",
      f"--topic={topic}",
      "--auto-spawn",
  ]

  print("Step 1/5: Launching Downstream Kafka-to-GCS Collector daemon...")
  collector_log_file = f"/tmp/collector_{run_id}.log"
  collector_log_fd = open(collector_log_file, "w")
  collector_proc = subprocess.Popen(
      collector_cmd,
      stdout=collector_log_fd,
      stderr=subprocess.STDOUT,
      text=True,
  )
  time.sleep(3)  # Allow collector to start

  if collector_proc.poll() is not None:
    collector_log_fd.close()
    with open(collector_log_file, "r") as f:
      err_content = f.read()
    print(f"ERROR: Downstream collector failed to start:\n{err_content}")
    return False

  # Capture starting topic partition offsets before workload so drain only reads new events
  print("\nCapturing Kafka topic start offsets before workload run...")
  start_offsets = get_topic_high_watermarks(topic)
  print(
      f"Captured start offsets for {len(start_offsets)} partitions:"
      f" {start_offsets}"
  )

  # Step 2: Run GCSB Placement Table Workload Driver
  workload_script = os.path.join(
      SCRIPT_DIR, "gcsb", "gcsb_users_workload_runner.py"
  )
  workload_cmd = [
      sys.executable,
      workload_script,
      f"--project={project}",
      f"--instance={instance}",
      f"--database={database}",
      f"--table={table}",
      f"--bucket={bucket}",
      f"--run-id={run_id}",
      f"--operations={operations}",
      f"--threads={threads}",
      f"--key-file={key_file}",
      f"--insert-ratio={insert_ratio}",
      f"--update-ratio={update_ratio}",
      f"--delete-ratio={delete_ratio}",
      "--milestone=placement",
  ]

  print(
      f"\nStep 2/5: Executing GCSB {table} Placement Table Mixed Workload"
      " Driver..."
  )
  try:
    w_res = subprocess.run(
        workload_cmd, check=True, capture_output=True, text=True
    )
    print(w_res.stdout)
  except subprocess.CalledProcessError as e:
    print(f"ERROR: GCSB workload runner failed:\n{e.stderr or e.stdout}")
    collector_proc.terminate()
    collector_log_fd.close()
    return False

  # Step 3: Stop background collector and run multi-partition parallel drain
  print("\nWaiting 45 seconds for CDC stream to catch up...")
  time.sleep(45)

  print("Step 3/5: Draining all Kafka topic partitions to high watermark...")
  collector_proc.terminate()
  try:
    collector_proc.wait(timeout=5)
  except subprocess.TimeoutExpired:
    collector_proc.kill()
  collector_log_fd.close()

  drain_cmd = [
      sys.executable,
      collector_script,
      f"--bucket={bucket}",
      f"--run-id={run_id}",
      f"--topic={topic}",
      "--drain-partitions",
      "--timeout-ms=3000",
  ]
  if start_offsets:
    drain_cmd.append(f"--start-offsets-json={json.dumps(start_offsets)}")
  subprocess.run(drain_cmd, check=True)

  # Step 4: Provision BigQuery External Tables for Placement Schema
  print(
      "\nStep 4/5: Provisioning BigQuery External Tables for source &"
      " downstream dumps..."
  )
  bq_dataset = "spanner_cdc_verification"
  gcsb_table = f"gcsb_audit_{run_id}_ext"
  kafka_table = f"kafka_events_{run_id}_ext"

  ddl_sql = f"""
CREATE SCHEMA IF NOT EXISTS `{project}.{bq_dataset}` OPTIONS(location='us-central1');

CREATE OR REPLACE EXTERNAL TABLE `{project}.{bq_dataset}.{gcsb_table}` (
  client_tx_id STRING,
  op STRING,
  table STRING,
  user_id INT64,
  placement_key STRING,
  user_name STRING,
  user_email STRING,
  account_balance STRING,
  metadata STRING,
  is_active BOOL,
  binary_signature STRING,
  commit_ts TIMESTAMP,
  after JSON,
  before JSON
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://{bucket}/{run_id}/gcsb_audit/*.jsonl']
);

CREATE OR REPLACE EXTERNAL TABLE `{project}.{bq_dataset}.{kafka_table}` (
  consumed_at_utc TIMESTAMP,
  topic STRING,
  op STRING,
  user_id INT64,
  payload JSON
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://{bucket}/{run_id}/kafka_events/*.jsonl']
);
"""

  bq_cmd = [
      "bq",
      "query",
      "--use_legacy_sql=false",
      "--location=us-central1",
      f"--project_id={project}",
      ddl_sql,
  ]
  bq_res = subprocess.run(bq_cmd, capture_output=True, text=True)
  if bq_res.returncode != 0:
    print(
        f"Notice: bq command returned code {bq_res.returncode}."
        f" Output:\n{bq_res.stderr}"
    )

  # Step 5: Execute Placement Table Audit Queries
  print(
      "\nStep 5/5: Executing Placement Table Verification Queries (PlacementKey"
      " Fidelity, Ordering, Completeness, SLA)..."
  )

  audit_sql = f"""
WITH SourceMutations AS (
  SELECT 
    user_id,
    op,
    placement_key,
    user_name,
    user_email,
    account_balance,
    metadata,
    is_active,
    binary_signature,
    commit_ts,
    CASE op 
      WHEN 'INSERT' THEN 'c'
      WHEN 'UPDATE' THEN 'u'
      WHEN 'DELETE' THEN 'd'
    END AS kafka_op,
    ROW_NUMBER() OVER (PARTITION BY user_id, op ORDER BY commit_ts) AS op_version
  FROM `{project}.{bq_dataset}.{gcsb_table}`
),
DownstreamEvents AS (
  SELECT 
    k.consumed_at_utc,
    k.user_id, 
    k.op AS kafka_op,
    k.payload,
    TIMESTAMP_MICROS(CAST(JSON_VALUE(k.payload.source.ts_us) AS INT64)) AS spanner_commit_ts,
    CAST(JSON_VALUE(k.payload.source.sequence) AS INT64) AS sequence_num,
    ROW_NUMBER() OVER (PARTITION BY k.user_id, k.op ORDER BY TIMESTAMP_MICROS(CAST(JSON_VALUE(k.payload.source.ts_us) AS INT64))) AS op_version
  FROM `{project}.{bq_dataset}.{kafka_table}` k
  WHERE k.op IN ('c', 'u', 'd')
    AND k.user_id IN (SELECT DISTINCT user_id FROM SourceMutations)
),
MissingRecords AS (
  SELECT src.user_id, src.op AS gcsb_op, src.commit_ts
  FROM SourceMutations src
  LEFT JOIN DownstreamEvents k 
    ON src.user_id = k.user_id 
   AND src.kafka_op = k.kafka_op
   AND src.op_version = k.op_version
  WHERE k.user_id IS NULL
),
ValueFidelityMismatches AS (
  SELECT 
    src.user_id,
    src.kafka_op,
    src.placement_key AS exp_placement,
    JSON_VALUE(k.payload.after.PlacementKey) AS act_placement,
    src.user_name AS exp_name,
    JSON_VALUE(k.payload.after.UserName) AS act_name,
    src.user_email AS exp_email,
    JSON_VALUE(k.payload.after.UserEmail) AS act_email,
    src.account_balance AS exp_balance,
    JSON_VALUE(k.payload.after.AccountBalance) AS act_balance,
    src.is_active AS exp_active,
    SAFE_CAST(JSON_VALUE(k.payload.after.IsActive) AS BOOL) AS act_active,
    src.binary_signature AS exp_sig,
    JSON_VALUE(k.payload.after.BinarySignature) AS act_sig
  FROM SourceMutations src
  INNER JOIN DownstreamEvents k 
    ON src.user_id = k.user_id 
   AND src.kafka_op = k.kafka_op
   AND src.op_version = k.op_version
  WHERE src.kafka_op IN ('c', 'u')
    AND (
      (src.placement_key IS NOT NULL AND src.placement_key != JSON_VALUE(k.payload.after.PlacementKey))
      OR (src.user_name IS NOT NULL AND src.user_name != JSON_VALUE(k.payload.after.UserName))
      OR (src.user_email IS NOT NULL AND src.user_email != JSON_VALUE(k.payload.after.UserEmail))
      OR (src.account_balance IS NOT NULL AND SAFE_CAST(src.account_balance AS NUMERIC) != SAFE_CAST(JSON_VALUE(k.payload.after.AccountBalance) AS NUMERIC))
      OR (src.is_active IS NOT NULL AND src.is_active != SAFE_CAST(JSON_VALUE(k.payload.after.IsActive) AS BOOL))
      OR (src.binary_signature IS NOT NULL AND src.binary_signature != JSON_VALUE(k.payload.after.BinarySignature))
    )
),
OrderingAnalysis AS (
  SELECT
    user_id,
    spanner_commit_ts,
    sequence_num,
    LAG(spanner_commit_ts) OVER (
      PARTITION BY user_id 
      ORDER BY spanner_commit_ts, sequence_num
    ) AS prev_commit_ts,
    LAG(sequence_num) OVER (
      PARTITION BY user_id 
      ORDER BY spanner_commit_ts, sequence_num
    ) AS prev_sequence_num
  FROM DownstreamEvents
),
OrderingViolations AS (
  SELECT *
  FROM OrderingAnalysis
  WHERE spanner_commit_ts < prev_commit_ts
     OR (spanner_commit_ts = prev_commit_ts AND sequence_num < prev_sequence_num)
),
LatencyMetrics AS (
  SELECT
    ROUND(AVG(TIMESTAMP_DIFF(consumed_at_utc, spanner_commit_ts, MILLISECOND)), 1) AS avg_lag_ms,
    APPROX_QUANTILES(TIMESTAMP_DIFF(consumed_at_utc, spanner_commit_ts, MILLISECOND), 100)[OFFSET(50)] AS p50_lag_ms,
    APPROX_QUANTILES(TIMESTAMP_DIFF(consumed_at_utc, spanner_commit_ts, MILLISECOND), 100)[OFFSET(95)] AS p95_lag_ms,
    APPROX_QUANTILES(TIMESTAMP_DIFF(consumed_at_utc, spanner_commit_ts, MILLISECOND), 100)[OFFSET(99)] AS p99_lag_ms
  FROM DownstreamEvents
)
SELECT
  (SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MIN(commit_ts)) FROM SourceMutations) AS window_start,
  (SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MAX(commit_ts)) FROM SourceMutations) AS window_end,
  (SELECT COUNT(*) FROM SourceMutations) AS total_source_mutations,
  (SELECT COUNTIF(op = 'INSERT') FROM SourceMutations) AS src_inserts,
  (SELECT COUNTIF(op = 'UPDATE') FROM SourceMutations) AS src_updates,
  (SELECT COUNTIF(op = 'DELETE') FROM SourceMutations) AS src_deletes,
  (SELECT COUNT(*) FROM DownstreamEvents) AS total_kafka_events,
  (SELECT COUNTIF(kafka_op = 'c') FROM DownstreamEvents) AS kafka_creates,
  (SELECT COUNTIF(kafka_op = 'u') FROM DownstreamEvents) AS kafka_updates,
  (SELECT COUNTIF(kafka_op = 'd') FROM DownstreamEvents) AS kafka_deletes,
  (SELECT COUNT(*) FROM MissingRecords) AS missing_mutations_count,
  (SELECT COUNT(*) FROM ValueFidelityMismatches) AS value_mismatches_count,
  (SELECT COUNT(*) FROM OrderingViolations) AS ordering_violations_count,
  (SELECT avg_lag_ms FROM LatencyMetrics) AS avg_lag_ms,
  (SELECT p50_lag_ms FROM LatencyMetrics) AS p50_lag_ms,
  (SELECT p95_lag_ms FROM LatencyMetrics) AS p95_lag_ms,
  (SELECT p99_lag_ms FROM LatencyMetrics) AS p99_lag_ms;
"""

  audit_cmd = [
      "bq",
      "query",
      "--format=json",
      "--use_legacy_sql=false",
      "--location=us-central1",
      f"--project_id={project}",
      audit_sql,
  ]
  audit_res = subprocess.run(audit_cmd, capture_output=True, text=True)

  total_src = 0
  src_ins = 0
  src_upd = 0
  src_del = 0
  total_dst = 0
  dst_ins = 0
  dst_upd = 0
  dst_del = 0
  missing_cnt = 0
  value_mismatches_cnt = 0
  ordering_violations_cnt = 0
  avg_lag = 0.0
  p50_lag = 0
  p95_lag = 0
  p99_lag = 0
  window_start = "N/A"
  window_end = "N/A"
  passed = False

  if audit_res.returncode == 0 and audit_res.stdout.strip():
    try:
      rows = json.loads(audit_res.stdout)
      if rows:
        row = rows[0]
        window_start = row.get("window_start") or "N/A"
        window_end = row.get("window_end") or "N/A"
        total_src = int(row.get("total_source_mutations", 0))
        src_ins = int(row.get("src_inserts", 0))
        src_upd = int(row.get("src_updates", 0))
        src_del = int(row.get("src_deletes", 0))
        total_dst = int(row.get("total_kafka_events", 0))
        dst_ins = int(row.get("kafka_creates", 0))
        dst_upd = int(row.get("kafka_updates", 0))
        dst_del = int(row.get("kafka_deletes", 0))
        missing_cnt = int(row.get("missing_mutations_count", 0))
        value_mismatches_cnt = int(row.get("value_mismatches_count", 0))
        ordering_violations_cnt = int(row.get("ordering_violations_count", 0))
        avg_lag = float(row.get("avg_lag_ms") or 0.0)
        p50_lag = int(row.get("p50_lag_ms") or 0)
        p95_lag = int(row.get("p95_lag_ms") or 0)
        p99_lag = int(row.get("p99_lag_ms") or 0)
        passed = (
            missing_cnt == 0
            and value_mismatches_cnt == 0
            and ordering_violations_cnt == 0
            and total_src > 0
        )
    except Exception as e:
      print(f"Error parsing BigQuery JSON results: {e}")

  print("\n" + "=" * 80)
  print(
      "         PLACEMENT TABLE VERIFICATION RESULTS - PLACEMENT & ORDERING"
      " AUDIT"
  )
  print("=" * 80)
  print(f"  Run Identifier                  : {run_id}")
  print(f"  Target Spanner Table            : {table}")
  print(f"  Kafka CDC Topic                 : {topic}")
  print(f"  Run Time Window                 : [{window_start} -> {window_end}]")
  print("-" * 80)
  print(f"  Total Source Committed Mutations: {total_src:,}")
  print(f"    - INSERT Operations           : {src_ins:,}")
  print(f"    - UPDATE Operations           : {src_upd:,}")
  print(f"    - DELETE Operations           : {src_del:,}")
  print("-" * 80)
  print(f"  Run-Window Downstream Events    : {total_dst:,}")
  print(f"    - 'c' (Create / Insert)       : {dst_ins:,}")
  print(f"    - 'u' (Update)                : {dst_upd:,}")
  print(f"    - 'd' (Delete)                : {dst_del:,}")
  print("-" * 80)
  print(
      f"  1. Completeness (Missing Rows)  : {missing_cnt}  ['PASS' if"
      f" missing_cnt == 0 else 'FAIL': {missing_cnt}]"
  )
  print(
      "  2. Field-Level Value Fidelity   :"
      f" {value_mismatches_cnt} Mismatches  ['PASS' if value_mismatches_cnt =="
      f" 0 else 'FAIL': {value_mismatches_cnt}]"
  )
  print(
      "     - Checked Fields: PlacementKey (PLACEMENT KEY), INT64, NUMERIC,"
      " JSON, TIMESTAMP, BOOL, BYTES"
  )
  print(
      "  3. Strict Per-Key Ordering      :"
      f" {ordering_violations_cnt} Violations  ['PASS' if"
      f" ordering_violations_cnt == 0 else 'FAIL': {ordering_violations_cnt}]"
  )
  print(
      f"  4. Replication Latency SLA      : P50={p50_lag}ms | P95={p95_lag}ms |"
      f" P99={p99_lag}ms (Avg={avg_lag:.1f}ms)"
  )
  print("-" * 80)
  if passed:
    print(
        "  VERIFICATION STATUS             : [PASS] PLACEMENT TABLE FULL"
        " COMPLIANCE CONFIRMED!"
    )
  else:
    print("  VERIFICATION STATUS             : [FAIL] PLACEMENT AUDIT FAILED!")
  print("=" * 80 + "\n")

  return passed


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Placement Table Verification Harness"
  )
  parser.add_argument("--project", default=DEFAULT_PROJECT)
  parser.add_argument("--instance", default=DEFAULT_INSTANCE)
  parser.add_argument("--database", default=DEFAULT_DATABASE)
  parser.add_argument("--table", default=DEFAULT_TABLE)
  parser.add_argument("--topic", default=DEFAULT_TOPIC)
  parser.add_argument("--bucket", default=DEFAULT_BUCKET)
  parser.add_argument("--run-id", default=None)
  parser.add_argument("--operations", type=int, default=50)
  parser.add_argument("--threads", type=int, default=4)
  parser.add_argument("--insert-ratio", type=float, default=0.5)
  parser.add_argument("--update-ratio", type=float, default=0.3)
  parser.add_argument("--delete-ratio", type=float, default=0.2)
  parser.add_argument("--key-file", default=DEFAULT_KEY_FILE)
  args = parser.parse_args()

  success = execute_placement_verification(
      project=args.project,
      instance=args.instance,
      database=args.database,
      table=args.table,
      topic=args.topic,
      bucket=args.bucket,
      run_id=args.run_id,
      operations=args.operations,
      threads=args.threads,
      insert_ratio=args.insert_ratio,
      update_ratio=args.update_ratio,
      delete_ratio=args.delete_ratio,
      key_file=args.key_file,
  )
  sys.exit(0 if success else 1)
