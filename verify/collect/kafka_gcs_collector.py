#!/usr/bin/env python3
"""
kafka_gcs_collector.py

Standalone Downstream Event Collector for Milestone 1 of the Spanner CDC verification framework.

This service acts as a zero-dependency alternative to the Kafka GCS Sink Connector plugin.
It continuously streams JSON CDC records from the downstream Kafka broker (e.g., inside the
GKE Kafka pod or via port-forwarding) and flushes batches as newline-delimited JSON (JSONL) files
directly to Cloud Storage (gs://<bucket_name>/<run_id>/kafka_events/).

Milestone 1 Scope:
  - Single simple table focus (cdc...Users)
  - Prepares downstream events for BigQuery external table comparison (Completeness Check)

Usage:
  # Option A: Run inside GKE via kubectl exec piped stream
  kubectl exec kafka-cp-kafka-0 -c cp-kafka-broker -- \
    kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic cdc.BenchmarkUsers \
    --from-beginning | python3 kafka_gcs_collector.py --bucket my-cdc-audit-bucket --run-id run_001

  # Option B: Run in direct subprocess mode (launches kafka-console-consumer locally via kubectl)
  python3 kafka_gcs_collector.py --auto-spawn --bucket my-cdc-audit-bucket --run-id run_001
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time

DEFAULT_CREDENTIALS = "/path/to/service-account-key.json"
DEFAULT_BUCKET = "my-cdc-audit-bucket"
DEFAULT_RUN_ID = "run_m1_baseline"
DEFAULT_TOPIC = "cdc.BenchmarkUsers"
DEFAULT_BATCH_SIZE = 5000  # Flush every 5,000 records
DEFAULT_FLUSH_INTERVAL_SEC = 10  # Or flush every 10 seconds


class GcsEventCollector:

  def __init__(
      self,
      bucket: str,
      run_id: str,
      topic: str,
      credentials_path: str = DEFAULT_CREDENTIALS,
      batch_size: int = DEFAULT_BATCH_SIZE,
      flush_interval_sec: int = DEFAULT_FLUSH_INTERVAL_SEC,
      local_scratch_dir: str = "/tmp/cdc_kafka_collector",
  ):
    self.bucket = bucket
    self.run_id = run_id
    self.topic = topic
    self.credentials_path = credentials_path
    self.batch_size = batch_size
    self.flush_interval_sec = flush_interval_sec
    self.local_scratch_dir = local_scratch_dir

    self.buffer = []
    self.last_flush_time = time.time()
    self.batch_counter = 0
    self.total_records_processed = 0

    os.makedirs(self.local_scratch_dir, exist_ok=True)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path

  def process_line(self, line: str):
    line = line.strip()
    if not line:
      return

    try:
      record = json.loads(line)
      if not isinstance(record, dict):
        return
      # Extract payload wrapper if present (Debezium JSON converter output)
      payload = record.get("payload", record)
      if not isinstance(payload, dict):
        return
      op = payload.get("op")

      # Milestone 1: Record row mutation operations (c=Create, u=Update, d=Delete)
      # Metadata heartbeats (op='m') can optionally be filtered or logged
      if op in ("c", "u", "d") or "after" in payload or "before" in payload:
        after_obj = payload.get("after") or {}
        before_obj = payload.get("before") or {}
        uid = (
            after_obj.get("UserId")
            if "UserId" in after_obj
            else after_obj.get(
                "user_id", before_obj.get("UserId", before_obj.get("user_id"))
            )
        )

        flattened_event = {
            "consumed_at_utc": (
                datetime.datetime.now(datetime.timezone.utc).isoformat()
            ),
            "topic": self.topic,
            "op": op,
            "user_id": uid,
            "payload": payload,
        }
        self.buffer.append(flattened_event)
        self.total_records_processed += 1

    except json.JSONDecodeError:
      # Skip malformed log noise if any
      pass

    now = time.time()
    if (
        len(self.buffer) >= self.batch_size
        or (now - self.last_flush_time) >= self.flush_interval_sec
    ):
      self.flush_batch()

  def flush_batch(self):
    if not self.buffer:
      self.last_flush_time = time.time()
      return

    self.batch_counter += 1
    timestamp_str = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"events_batch_{self.batch_counter:05d}_{timestamp_str}.jsonl"
    local_filepath = os.path.join(self.local_scratch_dir, filename)

    # Write newline-delimited JSON (JSONL) format
    with open(local_filepath, "w") as f:
      for event in self.buffer:
        f.write(json.dumps(event) + "\n")

    gcs_dest_uri = f"gs://{self.bucket}/{self.run_id}/kafka_events/{filename}"
    print(
        f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Flushing"
        f" {len(self.buffer)} events -> {gcs_dest_uri}"
    )

    # Upload using gsutil / gcloud storage CLI
    upload_cmd = ["gcloud", "storage", "cp", local_filepath, gcs_dest_uri]
    res = subprocess.run(upload_cmd, capture_output=True, text=True)
    if res.returncode != 0:
      # Fallback to legacy gsutil if gcloud storage is unavailable
      fallback_cmd = ["gsutil", "cp", local_filepath, gcs_dest_uri]
      res_fb = subprocess.run(fallback_cmd, capture_output=True, text=True)
      if res_fb.returncode != 0:
        print(f"ERROR: Failed to upload to GCS: {res_fb.stderr}")
        return

    # Clean local scratch chunk file upon successful GCS upload
    try:
      os.remove(local_filepath)
    except OSError:
      pass

    self.buffer.clear()
    self.last_flush_time = time.time()

  def run_stdin_loop(self):
    print(f"Starting Standalone Kafka-to-GCS Collector Daemon...")
    print(
        "  Target GCS Destination:"
        f" gs://{self.bucket}/{self.run_id}/kafka_events/"
    )
    print(
        f"  Batch Flush Trigger: {self.batch_size} records or"
        f" {self.flush_interval_sec} seconds"
    )
    print(f"  Listening on stdin for Kafka console consumer output...\n")

    try:
      for line in sys.stdin:
        self.process_line(line)
    except KeyboardInterrupt:
      print("\nStopping collector, flushing remaining events...")
    finally:
      self.flush_batch()
      print(
          "Finished. Total CDC records uploaded:"
          f" {self.total_records_processed}"
      )

  def run_auto_spawn(
      self,
      kube_pod: str = "kafka-cp-kafka-0",
      container: str = "cp-kafka-broker",
      timeout_ms: int = None,
  ):
    cmd = [
        "kubectl",
        "exec",
        "-i",
        kube_pod,
        "-c",
        container,
        "--",
        "kafka-console-consumer",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        self.topic,
        "--from-beginning",
    ]
    if timeout_ms:
      cmd.extend(["--timeout-ms", str(timeout_ms)])

    print(f"Spawning Kafka consumer process: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    def handle_shutdown(signum, frame):
      print("\nSignal received, flushing remaining in-memory events to GCS...")
      try:
        proc.terminate()
      except Exception:
        pass
      self.flush_batch()
      sys.exit(0)

    import signal

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
      for line in iter(proc.stdout.readline, ""):
        self.process_line(line)
    finally:
      self.flush_batch()
      print(f"Total processed: {self.total_records_processed}")

  def get_topic_high_watermarks(
      self,
      kube_pod: str = "kafka-cp-kafka-0",
      container: str = "cp-kafka-broker",
  ) -> dict:
    try:
      cmd = [
          "kubectl",
          "exec",
          kube_pod,
          "-c",
          container,
          "--",
          "kafka-run-class",
          "kafka.tools.GetOffsetShell",
          "--bootstrap-server",
          "localhost:9092",
          "--topic",
          self.topic,
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

  def get_topic_partition_count(
      self,
      kube_pod: str = "kafka-cp-kafka-0",
      container: str = "cp-kafka-broker",
  ) -> int:
    try:
      cmd = [
          "kubectl",
          "exec",
          kube_pod,
          "-c",
          container,
          "--",
          "kafka-topics",
          "--bootstrap-server",
          "localhost:9092",
          "--topic",
          self.topic,
          "--describe",
      ]
      res = subprocess.run(cmd, capture_output=True, text=True, check=True)
      m = re.search(r"PartitionCount:\s*(\d+)", res.stdout)
      if m:
        return int(m.group(1))
    except Exception as e:
      print(
          f"Notice: Could not auto-detect partition count ({e}), falling back"
          " to 10."
      )
    return 10

  def drain_all_partitions(
      self,
      num_partitions: int = None,
      start_offsets: dict = None,
      kube_pod: str = "kafka-cp-kafka-0",
      container: str = "cp-kafka-broker",
      timeout_ms: int = 3000,
  ):
    from concurrent.futures import ThreadPoolExecutor

    if num_partitions is None:
      num_partitions = self.get_topic_partition_count(
          kube_pod=kube_pod, container=container
      )

    def drain_partition(p):
      cmd = [
          "kubectl",
          "exec",
          "-i",
          kube_pod,
          "-c",
          container,
          "--",
          "kafka-console-consumer",
          "--bootstrap-server",
          "localhost:9092",
          "--topic",
          self.topic,
          "--partition",
          str(p),
      ]
      if start_offsets and p in start_offsets and start_offsets[p] is not None:
        cmd.extend(["--offset", str(start_offsets[p])])
      elif (
          start_offsets
          and str(p) in start_offsets
          and start_offsets[str(p)] is not None
      ):
        cmd.extend(["--offset", str(start_offsets[str(p)])])
      else:
        cmd.append("--from-beginning")
      cmd.extend(["--timeout-ms", str(timeout_ms)])
      proc = subprocess.Popen(
          cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
      )
      p_events = []
      for line in iter(proc.stdout.readline, ""):
        line = line.strip()
        if not line:
          continue
        try:
          record = json.loads(line)
          if not isinstance(record, dict):
            continue
          payload = record.get("payload", record)
          if not isinstance(payload, dict):
            continue
          op = payload.get("op")
          if op in ("c", "u", "d") or "after" in payload or "before" in payload:
            after_obj = payload.get("after") or {}
            before_obj = payload.get("before") or {}
            uid = (
                after_obj.get("UserId")
                if "UserId" in after_obj
                else after_obj.get(
                    "user_id",
                    before_obj.get("UserId", before_obj.get("user_id")),
                )
            )
            p_events.append({
                "consumed_at_utc": (
                    datetime.datetime.now(datetime.timezone.utc).isoformat()
                ),
                "topic": self.topic,
                "op": op,
                "user_id": uid,
                "payload": payload,
            })
        except Exception:
          pass
      proc.wait()
      return p_events

    if start_offsets:
      print(
          f"Draining all {num_partitions} topic partitions from start offsets"
          f" {start_offsets} to high-watermark offsets..."
      )
    else:
      print(
          f"Draining all {num_partitions} topic partitions (--from-beginning)"
          " concurrently to high-watermark offsets..."
      )
    with ThreadPoolExecutor(max_workers=num_partitions) as executor:
      partition_results = executor.map(drain_partition, range(num_partitions))
      for events in partition_results:
        for ev in events:
          self.buffer.append(ev)
          self.total_records_processed += 1
          if len(self.buffer) >= self.batch_size:
            self.flush_batch()

    self.flush_batch()
    print(
        "Partition drain complete. Total CDC records uploaded:"
        f" {self.total_records_processed}"
    )


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Downstream Event Collector (Kafka to GCS JSONL)"
  )
  parser.add_argument(
      "--bucket", default=DEFAULT_BUCKET, help="Target GCS bucket name"
  )
  parser.add_argument(
      "--run-id", default=DEFAULT_RUN_ID, help="Verification run ID in GCS"
  )
  parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name")
  parser.add_argument(
      "--batch-size", type=int, default=500, help="Flush batch record size"
  )
  parser.add_argument(
      "--flush-interval",
      type=int,
      default=DEFAULT_FLUSH_INTERVAL_SEC,
      help="Flush interval in seconds",
  )
  parser.add_argument(
      "--auto-spawn",
      action="store_true",
      help="Auto-spawn kubectl kafka-console-consumer process",
  )
  parser.add_argument(
      "--drain-partitions",
      action="store_true",
      help="Concurrently drain all topic partitions to high watermark",
  )
  parser.add_argument(
      "--num-partitions",
      type=int,
      default=10,
      help="Number of topic partitions",
  )
  parser.add_argument(
      "--timeout-ms",
      type=int,
      default=None,
      help="Kafka consumer timeout in ms",
  )
  parser.add_argument(
      "--start-offsets-json",
      default=None,
      help="JSON string of {partition: start_offset} for draining",
  )
  args = parser.parse_args()

  collector = GcsEventCollector(
      bucket=args.bucket,
      run_id=args.run_id,
      topic=args.topic,
      batch_size=args.batch_size,
      flush_interval_sec=args.flush_interval,
  )

  if args.drain_partitions:
    start_offsets = (
        json.loads(args.start_offsets_json) if args.start_offsets_json else None
    )
    collector.drain_all_partitions(
        num_partitions=args.num_partitions,
        start_offsets=start_offsets,
        timeout_ms=args.timeout_ms or 3000,
    )
  elif args.auto_spawn:
    collector.run_auto_spawn(timeout_ms=args.timeout_ms)
  else:
    collector.run_stdin_loop()
