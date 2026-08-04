#!/usr/bin/env python3
"""generate_source_json.py

Generates the Spanner Connector source.json configuration file with embedded credentials JSON.
"""

import argparse
import json
import os
import sys

DEFAULT_PROJECT = "my-gcp-project"
DEFAULT_INSTANCE = "my-spanner-instance"
DEFAULT_DATABASE = "my-database"
DEFAULT_CHANGE_STREAM = "mycs"
DEFAULT_KEY_FILE = "/path/to/service-account-key.json"
DEFAULT_OUTPUT = "source.json"


def generate_source_json(
    project: str,
    instance: str,
    database: str,
    change_stream: str,
    key_file: str,
    output: str,
):
  if not os.path.exists(key_file):
    print(
        f"Notice: Key file {key_file} not found. Using placeholder"
        " credentials."
    )
    key_string = '{"type": "service_account", "project_id": "' + project + '"}'
  else:
    with open(key_file, "r") as f:
      key_content = json.load(f)
    key_string = json.dumps(key_content)

  config = {
      "name": "cdc-spanner-connector",
      "config": {
          "connector.class": "io.debezium.connector.spanner.SpannerConnector",
          "gcp.spanner.change.stream": change_stream,
          "gcp.spanner.project.id": project,
          "gcp.spanner.instance.id": instance,
          "gcp.spanner.database.id": database,
          "gcp.spanner.low-watermark.enabled": "true",
          "gcp.spanner.low-watermark.update-period.ms": "1000",
          "tasks.max": "10",
          "connector.spanner.sync.kafka.bootstrap.servers": (
              "kafka-cp-kafka:9092"
          ),
          "connector.spanner.sync.publisher.wait.timeout": "5000",
          "gcp.spanner.stream.event.queue.capacity": "2000000",
          "topic.creation.default.partitions": "10",
          "topic.creation.default.replication.factor": "1",
          "max.queue.size": "2000000",
          "connector.spanner.max.missed.heartbeats": "600",
          "heartbeat.interval.ms": "1000",
          "gcp.spanner.credentials.json": key_string,
      },
  }

  out_dir = os.path.dirname(output)
  if out_dir:
    os.makedirs(out_dir, exist_ok=True)

  with open(output, "w") as f:
    json.dump(config, f, indent=2)
  print(f"Successfully generated {output}")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Generate Debezium Spanner Connector JSON configuration."
  )
  parser.add_argument("--project", default=DEFAULT_PROJECT)
  parser.add_argument("--instance", default=DEFAULT_INSTANCE)
  parser.add_argument("--database", default=DEFAULT_DATABASE)
  parser.add_argument("--change-stream", default=DEFAULT_CHANGE_STREAM)
  parser.add_argument("--key-file", default=DEFAULT_KEY_FILE)
  parser.add_argument("--output", default=DEFAULT_OUTPUT)

  args = parser.parse_args()
  generate_source_json(
      project=args.project,
      instance=args.instance,
      database=args.database,
      change_stream=args.change_stream,
      key_file=args.key_file,
      output=args.output,
  )
