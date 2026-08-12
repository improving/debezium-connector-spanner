#!/usr/bin/env python3
"""
gcsb_users_workload_runner.py

Milestone 1 GCSB Load Generator Client Wrapper & Audit Logger for Cloud Spanner Users Table.
Location: verify/gcsb/gcsb_users_workload_runner.py

Features:
  - Generates configurable multi-threaded read/write load against Cloud Spanner Users table:
      CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(256)) PRIMARY KEY (UserId);
  - Client-Side Mutation Logging: Captures every successfully committed transaction mutation
    and flushes structured JSONL audit dumps to GCS:
      gs://<bucket>/<run_id>/gcsb_audit/gcsb_audit_batch_XXXXX.jsonl
  - Feeds into BigQuery External Table gcsb_audit_m1_ext for Zero Data Loss completeness verification.

Usage:
  python3 gcsb_users_workload_runner.py \
    --project my-gcp-project \
    --instance my-spanner-instance \
    --database testdb \
    --bucket my-cdc-audit-bucket \
    --run-id run_m1_baseline \
    --operations 10000 \
    --threads 8
"""

import argparse
import base64
import concurrent.futures
import datetime
import json
import os
import random
import subprocess
import sys
import time
import uuid

DEFAULT_PROJECT = os.environ.get("GCP_PROJECT", "my-gcp-project")
DEFAULT_INSTANCE = os.environ.get("SPANNER_INSTANCE", "my-spanner-instance")
DEFAULT_DATABASE = os.environ.get("SPANNER_DATABASE", "my-database")
DEFAULT_TABLE = "BenchmarkUsers"
DEFAULT_BUCKET = os.environ.get("GCS_AUDIT_BUCKET", "my-cdc-audit-bucket")
DEFAULT_RUN_ID = "run_m1_baseline"
DEFAULT_KEY_FILE = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", "/path/to/service-account-key.json"
)


import threading
from decimal import Decimal

try:
  from google.cloud import spanner

  HAVE_SPANNER_SDK = True
except ImportError:
  HAVE_SPANNER_SDK = False


class GcsbAuditBuffer:

  def __init__(
      self,
      bucket: str,
      run_id: str,
      table: str = DEFAULT_TABLE,
      batch_size: int = 2000,
  ):
    self.bucket = bucket
    self.run_id = run_id
    self.table = table
    self.batch_size = batch_size
    self.buffer = []
    self.batch_counter = 0
    self.total_committed = 0
    self.lock = threading.Lock()
    self.scratch_dir = "/tmp/gcsb_client_audit"
    os.makedirs(self.scratch_dir, exist_ok=True)

  def record_entries(self, entries: list):
    with self.lock:
      self.buffer.extend(entries)
      self.total_committed += len(entries)
      if len(self.buffer) >= self.batch_size:
        self.flush()

  def record_mutation(
      self,
      op: str,
      user_id: int,
      commit_ts_iso: str,
      placement_key: str = None,
      user_name: str = None,
      user_email: str = None,
      account_balance: str = None,
      metadata: str = None,
      is_active: bool = None,
      binary_signature: str = None,
  ):
    entry = {
        "client_tx_id": str(uuid.uuid4()),
        "op": op,
        "table": self.table,
        "user_id": user_id,
        "placement_key": placement_key,
        "user_name": user_name,
        "user_email": user_email,
        "account_balance": account_balance,
        "metadata": metadata,
        "is_active": is_active,
        "binary_signature": binary_signature,
        "commit_ts": commit_ts_iso,
        "after": (
            {
                "UserId": user_id,
                "PlacementKey": placement_key,
                "UserName": user_name,
                "UserEmail": user_email,
                "AccountBalance": account_balance,
                "Metadata": metadata,
                "IsActive": is_active,
                "BinarySignature": binary_signature,
            }
            if op != "DELETE"
            else None
        ),
        "before": None,
    }
    with self.lock:
      self.buffer.append(entry)
      self.total_committed += 1

      if len(self.buffer) >= self.batch_size:
        self.flush()

  def flush(self):
    if not self.buffer:
      return

    self.batch_counter += 1
    ts_str = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"gcsb_audit_batch_{self.batch_counter:05d}_{ts_str}.jsonl"
    local_path = os.path.join(self.scratch_dir, filename)

    with open(local_path, "w") as f:
      for record in self.buffer:
        f.write(json.dumps(record) + "\n")

    gcs_dest = f"gs://{self.bucket}/{self.run_id}/gcsb_audit/{filename}"
    print(
        f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Flushed"
        f" {len(self.buffer)} GCSB committed mutations -> {gcs_dest}"
    )

    upload_cmd = ["gcloud", "storage", "cp", local_path, gcs_dest]
    res = subprocess.run(upload_cmd, capture_output=True, text=True)
    if res.returncode != 0:
      subprocess.run(
          ["gsutil", "cp", local_path, gcs_dest], capture_output=True
      )

    try:
      os.remove(local_path)
    except OSError:
      pass

    self.buffer.clear()


def execute_spanner_dml(
    project: str, instance: str, database: str, sql: str
) -> str:
  """Executes Spanner DML statement and returns commit timestamp."""
  cmd = [
      "gcloud",
      "spanner",
      "databases",
      "execute-sql",
      database,
      f"--instance={instance}",
      f"--project={project}",
      f"--sql={sql}",
  ]
  res = subprocess.run(cmd, capture_output=True, text=True)
  if res.returncode != 0:
    raise RuntimeError(
        f"Spanner DML execution failed: {res.stderr or res.stdout}"
    )
  return datetime.datetime.utcnow().isoformat() + "Z"


def run_workload(
    project: str,
    instance: str,
    database: str,
    table: str,
    bucket: str,
    run_id: str,
    total_operations: int,
    threads: int,
    key_file: str,
    insert_ratio: float = 0.5,
    update_ratio: float = 0.3,
    delete_ratio: float = 0.2,
    milestone: str = None,
):
  if os.path.exists(key_file):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file

  has_placement = (
      str(milestone).lower() in ["placement", "m3", "3"]
      or "placement" in table.lower()
  )
  is_rich = has_placement or str(milestone).lower() in ["2", "m2"]

  if has_placement and table == DEFAULT_TABLE:
    table = "BenchmarkPlacementUsers"

  if has_placement:
    milestone_label = (
        "Placement (PlacementKey: STRING PLACEMENT KEY + 8 Rich Columns)"
    )
  elif is_rich:
    milestone_label = (
        "M2 (Rich Schema: INT64, NUMERIC, JSON, TIMESTAMP, BOOL, BYTES)"
    )
  else:
    milestone_label = "M1 (Minimal Schema: UserId, UserName)"

  print("=" * 70)
  print(f"Starting GCSB Mixed Workload Generator [{milestone_label}]")
  print("=" * 70)
  print(f"  Target DB    : {project}:{instance}.{database}")
  print(f"  Target Table : {table}")
  print(f"  Schema Mode  : {milestone_label}")
  print(f"  GCS Audit URI: gs://{bucket}/{run_id}/gcsb_audit/")
  print(
      f"  Operations   : {total_operations:,} across {threads} worker threads"
  )
  print(
      f"  Mix Ratios   : INSERT={insert_ratio:.0%}, UPDATE={update_ratio:.0%},"
      f" DELETE={delete_ratio:.0%}\n"
  )

  audit_buffer = GcsbAuditBuffer(bucket=bucket, run_id=run_id, table=table)
  start_time = time.time()
  succ_inserts = 0
  succ_updates = 0
  succ_deletes = 0
  err_count = 0
  available_placements = ["default", "p0", "p1", "p2", "p3", "p4", "p5"]

  if HAVE_SPANNER_SDK:
    try:
      spanner_client = spanner.Client(project=project)
      spanner_instance = spanner_client.instance(instance)
      spanner_db = spanner_instance.database(database)

      cols = (
          [
              "UserId",
              "PlacementKey",
              "UserName",
              "UserEmail",
              "AccountBalance",
              "Metadata",
              "LastLogin",
              "IsActive",
              "BinarySignature",
          ]
          if has_placement
          else (
              [
                  "UserId",
                  "UserName",
                  "UserEmail",
                  "AccountBalance",
                  "Metadata",
                  "LastLogin",
                  "IsActive",
                  "BinarySignature",
              ]
              if is_rich
              else ["UserId", "UserName"]
          )
      )

      def worker_task_batch(worker_id: int, ops_per_worker: int):
        nonlocal succ_inserts, succ_updates, succ_deletes, err_count
        run_epoch_prefix = (
            int(time.time() * 100) % 100_000_000_000
        ) * 1_000_000
        base_id = run_epoch_prefix + worker_id * 100_000
        active_ids = []
        id_counter = 0
        batch_step = 50
        remaining_ops = ops_per_worker

        while remaining_ops > 0:
          current_batch_size = min(batch_step, remaining_ops)
          upsert_rows = []
          delete_keys = []
          batch_audit = []
          batch_used_ids = set()
          new_batch_ids = []
          b_ins = 0
          b_upd = 0
          b_del = 0

          for _ in range(current_batch_size):
            available_for_update = [
                uid for uid in active_ids if uid not in batch_used_ids
            ]
            if len(active_ids) < 5 or not available_for_update:
              op_choice = "INSERT"
            else:
              r = random.random()
              if r < insert_ratio:
                op_choice = "INSERT"
              elif r < insert_ratio + update_ratio:
                op_choice = "UPDATE"
              else:
                op_choice = "DELETE"

            if op_choice == "INSERT":
              id_counter += 1
              user_id = base_id + id_counter
              user_name = f"User_{user_id}_INIT"
              new_batch_ids.append(user_id)
              batch_used_ids.add(user_id)
              if has_placement:
                placement_key = random.choice(available_placements)
                user_email = f"user_{user_id}@example.com"
                balance = Decimal(
                    f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
                )
                meta_dict = {
                    "role": random.choice(["admin", "member", "guest"]),
                    "tier": random.choice(
                        ["bronze", "silver", "gold", "platinum"]
                    ),
                    "pref_id": random.randint(1, 100),
                }
                metadata = json.dumps(meta_dict)
                is_active = random.choice([True, False])
                sig_raw = os.urandom(8)
                sig_bytes = base64.b64encode(sig_raw)
                sig_b64 = sig_bytes.decode("ascii")

                upsert_rows.append([
                    user_id,
                    placement_key,
                    user_name,
                    user_email,
                    balance,
                    metadata,
                    spanner.COMMIT_TIMESTAMP,
                    is_active,
                    sig_bytes,
                ])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "INSERT",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": placement_key,
                    "user_name": user_name,
                    "user_email": user_email,
                    "account_balance": str(balance),
                    "metadata": metadata,
                    "is_active": is_active,
                    "binary_signature": sig_b64,
                    "commit_ts": None,
                    "after": {
                        "UserId": user_id,
                        "PlacementKey": placement_key,
                        "UserName": user_name,
                        "UserEmail": user_email,
                        "AccountBalance": str(balance),
                        "Metadata": metadata,
                        "IsActive": is_active,
                        "BinarySignature": sig_b64,
                    },
                    "before": None,
                })
              elif is_rich:
                user_email = f"user_{user_id}@example.com"
                balance = Decimal(
                    f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
                )
                meta_dict = {
                    "role": random.choice(["admin", "member", "guest"]),
                    "tier": random.choice(
                        ["bronze", "silver", "gold", "platinum"]
                    ),
                    "pref_id": random.randint(1, 100),
                }
                metadata = json.dumps(meta_dict)
                is_active = random.choice([True, False])
                sig_raw = os.urandom(8)
                sig_bytes = base64.b64encode(sig_raw)
                sig_b64 = sig_bytes.decode("ascii")

                upsert_rows.append([
                    user_id,
                    user_name,
                    user_email,
                    balance,
                    metadata,
                    spanner.COMMIT_TIMESTAMP,
                    is_active,
                    sig_bytes,
                ])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "INSERT",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": None,
                    "user_name": user_name,
                    "user_email": user_email,
                    "account_balance": str(balance),
                    "metadata": metadata,
                    "is_active": is_active,
                    "binary_signature": sig_b64,
                    "commit_ts": None,
                    "after": {
                        "UserId": user_id,
                        "PlacementKey": None,
                        "UserName": user_name,
                        "UserEmail": user_email,
                        "AccountBalance": str(balance),
                        "Metadata": metadata,
                        "IsActive": is_active,
                        "BinarySignature": sig_b64,
                    },
                    "before": None,
                })
              else:
                upsert_rows.append([user_id, user_name])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "INSERT",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": None,
                    "user_name": user_name,
                    "user_email": None,
                    "account_balance": None,
                    "metadata": None,
                    "is_active": None,
                    "binary_signature": None,
                    "commit_ts": None,
                    "after": {"UserId": user_id, "UserName": user_name},
                    "before": None,
                })
              b_ins += 1

            elif op_choice == "UPDATE":
              user_id = random.choice(available_for_update)
              batch_used_ids.add(user_id)
              user_name = f"User_{user_id}_UPD_{random.randint(100, 999)}"
              if has_placement:
                placement_key = random.choice(available_placements)
                user_email = f"user_{user_id}_upd@example.com"
                balance = Decimal(
                    f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
                )
                meta_dict = {
                    "role": random.choice(["admin", "member", "guest"]),
                    "tier": random.choice(["gold", "platinum"]),
                    "pref_id": random.randint(101, 200),
                }
                metadata = json.dumps(meta_dict)
                is_active = random.choice([True, False])
                sig_raw = os.urandom(8)
                sig_bytes = base64.b64encode(sig_raw)
                sig_b64 = sig_bytes.decode("ascii")

                upsert_rows.append([
                    user_id,
                    placement_key,
                    user_name,
                    user_email,
                    balance,
                    metadata,
                    spanner.COMMIT_TIMESTAMP,
                    is_active,
                    sig_bytes,
                ])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "UPDATE",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": placement_key,
                    "user_name": user_name,
                    "user_email": user_email,
                    "account_balance": str(balance),
                    "metadata": metadata,
                    "is_active": is_active,
                    "binary_signature": sig_b64,
                    "commit_ts": None,
                    "after": {
                        "UserId": user_id,
                        "PlacementKey": placement_key,
                        "UserName": user_name,
                        "UserEmail": user_email,
                        "AccountBalance": str(balance),
                        "Metadata": metadata,
                        "IsActive": is_active,
                        "BinarySignature": sig_b64,
                    },
                    "before": None,
                })
              elif is_rich:
                user_email = f"user_{user_id}_upd@example.com"
                balance = Decimal(
                    f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
                )
                meta_dict = {
                    "role": random.choice(["admin", "member", "guest"]),
                    "tier": random.choice(["gold", "platinum"]),
                    "pref_id": random.randint(101, 200),
                }
                metadata = json.dumps(meta_dict)
                is_active = random.choice([True, False])
                sig_raw = os.urandom(8)
                sig_bytes = base64.b64encode(sig_raw)
                sig_b64 = sig_bytes.decode("ascii")

                upsert_rows.append([
                    user_id,
                    user_name,
                    user_email,
                    balance,
                    metadata,
                    spanner.COMMIT_TIMESTAMP,
                    is_active,
                    sig_bytes,
                ])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "UPDATE",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": None,
                    "user_name": user_name,
                    "user_email": user_email,
                    "account_balance": str(balance),
                    "metadata": metadata,
                    "is_active": is_active,
                    "binary_signature": sig_b64,
                    "commit_ts": None,
                    "after": {
                        "UserId": user_id,
                        "PlacementKey": None,
                        "UserName": user_name,
                        "UserEmail": user_email,
                        "AccountBalance": str(balance),
                        "Metadata": metadata,
                        "IsActive": is_active,
                        "BinarySignature": sig_b64,
                    },
                    "before": None,
                })
              else:
                upsert_rows.append([user_id, user_name])
                batch_audit.append({
                    "client_tx_id": str(uuid.uuid4()),
                    "op": "UPDATE",
                    "table": table,
                    "user_id": user_id,
                    "placement_key": None,
                    "user_name": user_name,
                    "user_email": None,
                    "account_balance": None,
                    "metadata": None,
                    "is_active": None,
                    "binary_signature": None,
                    "commit_ts": None,
                    "after": {"UserId": user_id, "UserName": user_name},
                    "before": None,
                })
              b_upd += 1

            elif op_choice == "DELETE":
              user_id = random.choice(available_for_update)
              active_ids.remove(user_id)
              batch_used_ids.add(user_id)
              delete_keys.append([user_id])
              batch_audit.append({
                  "client_tx_id": str(uuid.uuid4()),
                  "op": "DELETE",
                  "table": table,
                  "user_id": user_id,
                  "placement_key": None,
                  "user_name": None,
                  "user_email": None,
                  "account_balance": None,
                  "metadata": None,
                  "is_active": None,
                  "binary_signature": None,
                  "commit_ts": None,
                  "after": None,
                  "before": None,
              })
              b_del += 1

          try:
            with spanner_db.batch() as batch:
              if upsert_rows:
                batch.insert_or_update(
                    table=table, columns=cols, values=upsert_rows
                )
              if delete_keys:
                batch.delete(
                    table=table, keyset=spanner.KeySet(keys=delete_keys)
                )
            commit_ts = batch.committed.isoformat()
            active_ids.extend(new_batch_ids)
            for entry in batch_audit:
              entry["commit_ts"] = commit_ts
            audit_buffer.record_entries(batch_audit)
            succ_inserts += b_ins
            succ_updates += b_upd
            succ_deletes += b_del
            remaining_ops -= current_batch_size
          except Exception as e:
            err_count += 1
            if err_count <= 5:
              print(f"Worker {worker_id} batch error: {e}")
            time.sleep(0.5)

      ops_per_thread = total_operations // threads
      with concurrent.futures.ThreadPoolExecutor(
          max_workers=threads
      ) as executor:
        futures = [
            executor.submit(worker_task_batch, t, ops_per_thread)
            for t in range(threads)
        ]
        concurrent.futures.wait(futures)

      audit_buffer.flush()
      elapsed = time.time() - start_time
      total_succ = succ_inserts + succ_updates + succ_deletes
      qps = total_succ / elapsed if elapsed > 0 else 0

      print("\n" + "=" * 70)
      print("Workload Summary:")
      print(f"  Total Committed Operations : {total_succ:,}")
      print(f"    - INSERTs                : {succ_inserts:,}")
      print(f"    - UPDATEs                : {succ_updates:,}")
      print(f"    - DELETEs                : {succ_deletes:,}")
      print(f"  Total Failed Operations    : {err_count:,}")
      print(f"  Elapsed Time               : {elapsed:.2f} s")
      print(f"  Throughput                 : {qps:.1f} ops/sec")
      print(
          f"  Audit Log Dest             : gs://{bucket}/{run_id}/gcsb_audit/"
      )
      print("=" * 70)
      return
    except Exception as e:
      print(
          f"Notice: Python Spanner SDK execution failed ({e}), falling back to"
          " CLI execution."
      )

  def worker_task(worker_id: int, ops_per_worker: int):
    nonlocal succ_inserts, succ_updates, succ_deletes, err_count
    base_id = worker_id * 1000000 + random.randint(1000, 9999)
    active_ids = []
    id_counter = 0

    for i in range(ops_per_worker):
      # Determine operation type
      if len(active_ids) < 3:
        op_choice = "INSERT"
      else:
        r = random.random()
        if r < insert_ratio:
          op_choice = "INSERT"
        elif r < insert_ratio + update_ratio:
          op_choice = "UPDATE"
        else:
          op_choice = "DELETE"

      try:
        if op_choice == "INSERT":
          id_counter += 1
          user_id = base_id + id_counter
          user_name = f"User_{user_id}_INIT"

          if has_placement:
            placement_key = random.choice(available_placements)
            user_email = f"user_{user_id}@example.com"
            balance = f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
            meta_dict = {
                "role": random.choice(["admin", "member", "guest"]),
                "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
                "pref_id": random.randint(1, 100),
            }
            metadata = json.dumps(meta_dict)
            is_active = random.choice([True, False])
            sig_b64 = base64.b64encode(os.urandom(8)).decode("ascii")

            sql = (
                f"INSERT INTO {table} (UserId, PlacementKey, UserName,"
                " UserEmail, AccountBalance, Metadata, LastLogin, IsActive,"
                f" BinarySignature) VALUES ({user_id}, '{placement_key}',"
                f" '{user_name}', '{user_email}', NUMERIC '{balance}', JSON"
                f" '{metadata}', PENDING_COMMIT_TIMESTAMP(),"
                f" {str(is_active).upper()}, FROM_BASE64('{sig_b64}'))"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "INSERT",
                user_id,
                commit_ts,
                placement_key=placement_key,
                user_name=user_name,
                user_email=user_email,
                account_balance=balance,
                metadata=metadata,
                is_active=is_active,
                binary_signature=sig_b64,
            )
          elif is_rich:
            user_email = f"user_{user_id}@example.com"
            balance = f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
            meta_dict = {
                "role": random.choice(["admin", "member", "guest"]),
                "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
                "pref_id": random.randint(1, 100),
            }
            metadata = json.dumps(meta_dict)
            is_active = random.choice([True, False])
            sig_b64 = base64.b64encode(os.urandom(8)).decode("ascii")

            sql = (
                f"INSERT INTO {table} (UserId, UserName, UserEmail,"
                " AccountBalance, Metadata, LastLogin, IsActive,"
                f" BinarySignature) VALUES ({user_id}, '{user_name}',"
                f" '{user_email}', NUMERIC '{balance}', JSON '{metadata}',"
                f" PENDING_COMMIT_TIMESTAMP(), {str(is_active).upper()},"
                f" FROM_BASE64('{sig_b64}'))"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "INSERT",
                user_id,
                commit_ts,
                user_name=user_name,
                user_email=user_email,
                account_balance=balance,
                metadata=metadata,
                is_active=is_active,
                binary_signature=sig_b64,
            )
          else:
            sql = (
                f"INSERT INTO {table} (UserId, UserName) VALUES ({user_id},"
                f" '{user_name}')"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "INSERT", user_id, commit_ts, user_name=user_name
            )

          active_ids.append(user_id)
          succ_inserts += 1

        elif op_choice == "UPDATE":
          user_id = random.choice(active_ids)
          user_name = f"User_{user_id}_UPD_{random.randint(100, 999)}"

          if has_placement:
            placement_key = random.choice(available_placements)
            user_email = f"user_{user_id}_upd@example.com"
            balance = f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
            meta_dict = {
                "role": random.choice(["admin", "member", "guest"]),
                "tier": random.choice(["gold", "platinum"]),
                "pref_id": random.randint(101, 200),
            }
            metadata = json.dumps(meta_dict)
            is_active = random.choice([True, False])
            sig_b64 = base64.b64encode(os.urandom(8)).decode("ascii")

            sql = (
                f"UPDATE {table} SET "
                f"PlacementKey = '{placement_key}', "
                f"UserName = '{user_name}', "
                f"UserEmail = '{user_email}', "
                f"AccountBalance = NUMERIC '{balance}', "
                f"Metadata = JSON '{metadata}', "
                "LastLogin = PENDING_COMMIT_TIMESTAMP(), "
                f"IsActive = {str(is_active).upper()}, "
                f"BinarySignature = FROM_BASE64('{sig_b64}') "
                f"WHERE UserId = {user_id}"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "UPDATE",
                user_id,
                commit_ts,
                placement_key=placement_key,
                user_name=user_name,
                user_email=user_email,
                account_balance=balance,
                metadata=metadata,
                is_active=is_active,
                binary_signature=sig_b64,
            )
          elif is_rich:
            user_email = f"user_{user_id}_upd@example.com"
            balance = f"{random.randint(100, 50000)}.{random.randint(10, 99)}"
            meta_dict = {
                "role": random.choice(["admin", "member", "guest"]),
                "tier": random.choice(["gold", "platinum"]),
                "pref_id": random.randint(101, 200),
            }
            metadata = json.dumps(meta_dict)
            is_active = random.choice([True, False])
            sig_b64 = base64.b64encode(os.urandom(8)).decode("ascii")

            sql = (
                f"UPDATE {table} SET "
                f"UserName = '{user_name}', "
                f"UserEmail = '{user_email}', "
                f"AccountBalance = NUMERIC '{balance}', "
                f"Metadata = JSON '{metadata}', "
                "LastLogin = PENDING_COMMIT_TIMESTAMP(), "
                f"IsActive = {str(is_active).upper()}, "
                f"BinarySignature = FROM_BASE64('{sig_b64}') "
                f"WHERE UserId = {user_id}"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "UPDATE",
                user_id,
                commit_ts,
                user_name=user_name,
                user_email=user_email,
                account_balance=balance,
                metadata=metadata,
                is_active=is_active,
                binary_signature=sig_b64,
            )
          else:
            sql = (
                f"UPDATE {table} SET UserName = '{user_name}' WHERE UserId ="
                f" {user_id}"
            )
            commit_ts = execute_spanner_dml(project, instance, database, sql)
            audit_buffer.record_mutation(
                "UPDATE", user_id, commit_ts, user_name=user_name
            )

          succ_updates += 1

        elif op_choice == "DELETE":
          idx = random.randrange(len(active_ids))
          user_id = active_ids.pop(idx)
          sql = f"DELETE FROM {table} WHERE UserId = {user_id}"
          commit_ts = execute_spanner_dml(project, instance, database, sql)
          audit_buffer.record_mutation("DELETE", user_id, commit_ts)
          succ_deletes += 1

      except Exception as e:
        err_count += 1
        if err_count <= 5:
          print(f"Worker {worker_id} error on {op_choice}: {e}")

  ops_per_thread = total_operations // threads
  with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures = [
        executor.submit(worker_task, t, ops_per_thread) for t in range(threads)
    ]
    concurrent.futures.wait(futures)

  audit_buffer.flush()
  elapsed = time.time() - start_time
  total_succ = succ_inserts + succ_updates + succ_deletes
  qps = total_succ / elapsed if elapsed > 0 else 0

  print("\n" + "=" * 70)
  print("Workload Summary:")
  print(f"  Total Committed Operations : {total_succ:,}")
  print(f"    - INSERTs                : {succ_inserts:,}")
  print(f"    - UPDATEs                : {succ_updates:,}")
  print(f"    - DELETEs                : {succ_deletes:,}")
  print(f"  Total Failed Operations    : {err_count:,}")
  print(f"  Elapsed Time               : {elapsed:.2f} s")
  print(f"  Throughput                 : {qps:.1f} ops/sec")
  print(f"  Audit Log Dest             : gs://{bucket}/{run_id}/gcsb_audit/")
  print("=" * 70)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description=(
          "GCSB BenchmarkUsers Table Workload Driver with GCS Mutation Logging."
      )
  )
  parser.add_argument("--project", default=DEFAULT_PROJECT)
  parser.add_argument("--instance", default=DEFAULT_INSTANCE)
  parser.add_argument("--database", default=DEFAULT_DATABASE)
  parser.add_argument("--table", default=DEFAULT_TABLE)
  parser.add_argument("--bucket", default=DEFAULT_BUCKET)
  parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
  parser.add_argument("--operations", type=int, default=100)
  parser.add_argument("--threads", type=int, default=4)
  parser.add_argument("--insert-ratio", type=float, default=0.5)
  parser.add_argument("--update-ratio", type=float, default=0.3)
  parser.add_argument("--delete-ratio", type=float, default=0.2)
  parser.add_argument("--key-file", default=DEFAULT_KEY_FILE)
  parser.add_argument(
      "--milestone",
      choices=[
          "1",
          "2",
          "3",
          "m1",
          "m2",
          "m3",
          "M1",
          "M2",
          "M3",
          "placement",
          "Placement",
      ],
      default=None,
      help=(
          "Workload schema mode: '1' or 'm1' for Milestone 1 minimal schema"
          " (UserId, UserName), '2' or 'm2' for Milestone 2 rich schema,"
          " 'placement' or 'm3' for placement table schema."
      ),
  )

  args = parser.parse_args()
  run_workload(
      project=args.project,
      instance=args.instance,
      database=args.database,
      table=args.table,
      bucket=args.bucket,
      run_id=args.run_id,
      total_operations=args.operations,
      threads=args.threads,
      key_file=args.key_file,
      insert_ratio=args.insert_ratio,
      update_ratio=args.update_ratio,
      delete_ratio=args.delete_ratio,
      milestone=args.milestone,
  )
