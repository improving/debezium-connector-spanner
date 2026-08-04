# Cloud Spanner Kafka Connector: End-to-End Build, Deployment & Verification Guide

This document provides a comprehensive, self-contained guide for vendors and
external partners to build, deploy, configure, and verify the **Debezium Cloud
Spanner Kafka Connector** (`io.debezium.connector.spanner.SpannerConnector`) in
Kubernetes and Kafka Connect environments.

The guide is organized into two primary parts:

*   **Part I: Build & Kubernetes Deployment Guide (Steps 1–12)**: End-to-end
    instructions for building Debezium parent dependencies, assembling Docker
    images, provisioning Kubernetes infrastructure with Terraform/Helm, and
    deploying the connector.
*   **Part II: Automated E2E Verification Test Suites (Milestones 1 & 2)**:
    Automated high-load verification harnesses for Zero Data Loss completeness,
    Rich GoogleSQL Schema fidelity (`INT64`, `NUMERIC`, `JSON`, `TIMESTAMP`,
    `BOOL`, `BYTES`), per-key monotonic ordering, and replication latency SLA
    auditing.

--------------------------------------------------------------------------------

## 1. Architecture Overview

The verification framework uses an automated **Load Generation, Ingestion, and
BigQuery Analytical Audit Pipeline**:

```
                              +-----------------------------------+
                              |       GCSB Load Generator         |
                              |   (Multi-Threaded DML Workers)    |
                              +-----------------+-----------------+
                                                |
                      +-------------------------+-------------------------+
                      | DML Mutations                                     | Client Audit Dump (JSONL)
                      v                                                   v
        +---------------------------+                           +-------------------+
        |  Google Cloud Spanner     |                           |   Cloud Storage   |
        |  (GoogleSQL Table + CS)   |                           |  (gs://bucket/..) |
        +-------------+-------------+                           +---------+---------+
                      |                                                   |
                      | Change Stream                                     |
                      v                                                   v
        +---------------------------+                           +-------------------+
        | Debezium Spanner Connector|                           | BigQuery Engine   |
        |  (Kafka Connect Cluster)  |                           | (External Tables) |
        +-------------+-------------+                           +---------+---------+
                      |                                                   |
                      | Kafka CDC Events                                  |
                      v                                                   |
        +---------------------------+                                     |
        |       Kafka Topic(s)      |                                     |
        |  (cdc.BenchmarkUsers)     |                                     |
        +-------------+-------------+                                     |
                      |                                                   |
                      | Fast Partition Drain                              |
                      v                                                   |
        +---------------------------+                                     |
        | Downstream Kafka Collector| ------------------------------------+
        |  (collect/kafka_gcs_...)  |
        +---------------------------+
```

### Key Components

1.  **Cloud Spanner Database & Change Stream**: Tracks row-level modifications
    (`OLD_AND_NEW_VALUES`) across target user tables.
2.  **Debezium Spanner Kafka Connector**: Subscribes to the change stream and
    publishes structured change events (`c` = Create, `u` = Update, `d` =
    Delete, `m` = Heartbeat/Metadata) to Kafka topics.
3.  **GCSB Workload Generator (`./gcsb/gcsb_users_workload_runner.py`)**: Spawns
    concurrent threads executing configurable ratios of `INSERT`, `UPDATE`, and
    `DELETE` operations against Spanner, writing authoritative transaction logs
    to Google Cloud Storage (GCS).
4.  **Downstream Kafka Collector (`./collect/kafka_gcs_collector.py`)**:
    Connects to Kafka brokers, captures starting partition watermarks to ignore
    historical topic backlog, and drains new CDC records to GCS.
5.  **BigQuery Audit Engine (`run_m1_verification.py` /
    `run_m2_verification.py`)**: Mounts GCS audit logs as BigQuery External
    Tables and runs SQL reconciliation queries.

--------------------------------------------------------------------------------

## 2. Prerequisites & Tools

*   **Java 11+** (Base container images use Java 21)
*   **Docker**
*   **Apache Maven** (or Maven Wrapper `./mvnw`)
*   **Google Cloud CLI (`gcloud`)** authenticated with appropriate Service
    Account permissions
*   **Kubernetes CLI (`kubectl`)** & **Helm (`helm`)**
*   **Terraform (`terraform` v1.5+)**
*   **GCP Service Account Key (`service-account-key.json`)** with IAM
    permissions:
    *   *Cloud Spanner*: Database Admin / Reader / DML User
    *   *Cloud Storage*: Object Creator / Viewer
    *   *BigQuery*: Job User / Data Editor

--------------------------------------------------------------------------------

# PART I: BUILD & KUBERNETES DEPLOYMENT GUIDE (STEPS 1–12)

## Step 1: Build Debezium Parent Dependencies

The Spanner connector depends on parent Debezium modules. Build the required
modules locally from your Debezium parent repository:

```bash
cd ~/projects/debezium
./mvnw clean install \
  -pl debezium-bom,debezium-core,debezium-embedded,debezium-storage \
  -am \
  -DskipTests \
  -DskipITs \
  -Dformat.formatter.goal=validate \
  -Dformat.imports.goal=check \
  -Dhttp.keepAlive=false \
  -Dmaven.wagon.http.pool=false \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=120
```

## Step 2: Customize Spanner Connector Dockerfile

The default Dockerfile in `debezium-connector-spanner` requires adjustments for
minimal container base images, clean plugin directory layout, and correct
non-root (`appuser`) permissions.

Modify `debezium-connector-spanner/src/test/docker/Dockerfile`:

1.  Use the default Java version in the `cp-kafka-connect-base` base image.
2.  Copy the target plugin archive directly into `/usr/share/java/` without
    unnecessary directory nesting.
3.  Grant ownership (`chown -R appuser:appuser`) to ensure the connector process
    can load JARs without permission errors.
4.  Set `USER appuser` before container entrypoint execution.

**Recommended Dockerfile Content:**

```dockerfile
FROM mirror.gcr.io/confluentinc/cp-kafka-connect-base
ARG projectVersion
USER root
COPY target/debezium-connector-spanner-${projectVersion}-plugin/debezium-connector-spanner/ /usr/share/java/google-debezium-connector-spanner/
COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
RUN chown -R appuser:appuser /usr/share/java/google-debezium-connector-spanner/
RUN chown -R appuser:appuser /usr/share/prometheus/
USER appuser
```

## Step 3: Build and Package the Connector Plugin

Assemble the Spanner connector JAR and plugin archive using the local packaging
profile:

```bash
cd ~/projects/debezium-connector-spanner
./mvnw clean package -Ppack-local-changes -DskipTests -DskipITs -Ddocker.skip=true
```

## Step 4: Build and Push Docker Image to Container Registry

Build the Docker image containing the connector plugin and push it to your
target container registry:

```bash
cd ~/projects/debezium-connector-spanner
# Use 'package' (not 'clean package') to preserve assembly artifacts from Step 3
./mvnw package -Ppack-local-changes \
  -DskipTests -DskipITs \
  -Ddocker.skip=false \
  -Ddocker.skip.push=false \
  -Ddocker.username="<your-registry-username>" \
  -Ddocker.password="<your-registry-password>" \
  -Ddocker.repository.name="<your-registry-host>/<your-repo>/kafka-spanner-connector" \
  -Ddocker.tag.name="v1.0.0-test"
```

## Step 5: Configure Helm Chart Values (`values.yaml`)

In your Kubernetes deployment repository (e.g.,
`debezium.connector.ops/gcp-k8s-helm/files/connector/values.yaml`), update the
chart configuration:

1.  Specify your newly built container image and tag:

    ```yaml
    ## Image Info
    image: "<your-registry-host>/<your-repo>/kafka-spanner-connector"
    imageTag: "v1.0.0-test"
    ```

2.  Remove or comment out Confluent monitoring interceptors from
    `configurationOverrides` if they are not present in the minimal base image:

    ```yaml
    # Comment out to avoid ClassNotFoundException on task startup:
    # "producer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
    # "consumer.interceptor.classes": "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor"
    ```

## Step 6: Configure Terraform Variables (`terraform.tfvars`)

Update `terraform.tfvars` in your deployment directory with target cloud project
and cluster dimensions:

```ini
project             = "<my-gcp-project-id>"
region              = "us-central1"
location            = "us-central1-c"
instance_type       = "e2-highmem-16"
node_count          = "12"
gcp_auth_file       = "/path/to/service-account-key.json"
app_name            = "spanner-cdc-k8s-cluster"
registry_username   = "<your-registry-username>"
registry_password   = "<your-registry-password>"
registry_email      = "admin@example.com"
registry_server     = "docker.io"
```

## Step 7: Provision Kubernetes Cluster & Components with Terraform

Provision the VPC, Kubernetes cluster, Kafka brokers, Kafka Connect workers, and
monitoring stack (Prometheus / Grafana / AKHQ):

1.  **Initialize and Validate**:

    ```bash
    cd ~/projects/debezium.connector.ops/gcp-k8s-helm
    ./terraform init
    ./terraform validate
    ```

2.  **Apply Terraform Deployment**:

    ```bash
    ./terraform apply -auto-approve
    ```

## Step 8: Authenticate and Configure Kubernetes CLI Access

Retrieve credentials to access the provisioned Kubernetes cluster:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

gcloud auth activate-service-account <service-account-email> \
  --key-file=$GOOGLE_APPLICATION_CREDENTIALS
gcloud config set project <my-gcp-project-id>
gcloud container clusters get-credentials spanner-cdc-k8s-cluster --zone us-central1-c
```

## Step 9: Generate Connector Configuration (`source.json`)

Service account credentials contain newline-delimited private keys that require
JSON escaping. Use the generator script (`generate_source_json.py`) to create a
valid connector payload:

```bash
python3 generate_source_json.py \
  --project "<my-gcp-project-id>" \
  --instance "<spanner-instance-id>" \
  --database "<database-name>" \
  --change-stream "mycs" \
  --key-file "/path/to/service-account-key.json" \
  --output "source.json"
```

## Step 10: Deploy Connector via Kafka Connect REST API

1.  **Start Port-Forwarding** to the Kafka Connect service in the background:

    ```bash
    kubectl port-forward service/kafka-connect-cp-kafka-connect 8083:8083 &
    ```

2.  **Post the Configuration** to deploy the connector:

    ```bash
    curl -i -X POST -H "Content-Type: application/json" \
      http://localhost:8083/connectors \
      -d @source.json
    ```

    *Expected Response*: `HTTP/1.1 201 Created`.

## Step 11: Verify Connector Rollout and Task Status

1.  **Verify Kubernetes Rollout**:

    ```bash
    kubectl rollout status deployment/kafka-connect-cp-kafka-connect
    ```

2.  **Verify Running Image Version**:

    ```bash
    kubectl get deployment kafka-connect-cp-kafka-connect \
      -o jsonpath='{.spec.template.spec.containers[*].image}'
    ```

3.  **Verify Connector and Task State**:

    ```bash
    curl -s http://localhost:8083/connectors/<connector-name>/status | jq .
    ```

    Confirm that `"connector".state` is `RUNNING` and every task in the
    `"tasks"` array is `RUNNING`.

## Step 12: Manual Single-Row Smoke Test

1.  **Terminal 1 (Monitor Topic)**:

    ```bash
    kubectl exec -it kafka-broker-pod -c cp-kafka-broker -- \
      kafka-console-consumer --bootstrap-server localhost:9092 \
      --topic cdc.BenchmarkUsers
    ```

2.  **Terminal 2 (Insert Test Record)**:

    ```bash
    gcloud spanner databases execute-sql <database-name> \
      --instance="<spanner-instance-id>" \
      --project="<my-gcp-project-id>" \
      --sql="INSERT INTO BenchmarkUsers (UserId, UserName) VALUES (999, 'Test_User_999')"
    ```

    Verify that a valid JSON Create event (`"op": "c"`) appears in Terminal 1.

--------------------------------------------------------------------------------

# PART II: AUTOMATED E2E VERIFICATION TEST SUITES (MILESTONES 1 & 2)

## Milestone 1 (M1): Zero Data Loss & Completeness Verification

### Objective

Verify **100% Completeness and Zero Data Loss (`Missing Mutations = 0`)** when
streaming high-concurrency mixed `INSERT`, `UPDATE`, and `DELETE` operations
from Cloud Spanner to Kafka.

### Execution Workflow

1.  **Start-Offset Capture**: Queries broker partition high watermarks to ensure
    verification only inspects records generated during the active test run.
2.  **Load Generation**: Spawns worker threads executing M1 schema mutations
    (`UserId`, `UserName`) against Cloud Spanner and logs authoritative source
    commit timestamps to GCS.
3.  **Partition Draining**: Concurrently drains new Kafka topic events to GCS
    JSONL files.
4.  **BigQuery Reconciliation**: Joins Spanner source logs against Kafka
    downstream events by `(user_id, commit_timestamp)` and asserts 0 missing
    records.

### Command to Run

```bash
python3 run_m1_verification.py \
  --project "<my-gcp-project-id>" \
  --instance "<spanner-instance-id>" \
  --database "<database-name>" \
  --table BenchmarkUsers \
  --topic cdc.BenchmarkUsers \
  --bucket "<my-cdc-audit-bucket>" \
  --run-id run_m1_test_001 \
  --operations 500 \
  --threads 8 \
  --insert-ratio 0.5 \
  --update-ratio 0.3 \
  --delete-ratio 0.2 \
  --key-file "/path/to/service-account-key.json"
```

### Expected Output Report

```text
================================================================================
         MILESTONE 1 VERIFICATION RESULTS - ZERO DATA LOSS AUDIT
================================================================================
  Run Identifier                  : run_m1_test_001
  Target Spanner Table            : BenchmarkUsers
  Kafka CDC Topic                 : cdc.BenchmarkUsers
  Run Time Window                 : [2026-08-04 18:00:01 UTC -> 2026-08-04 18:00:25 UTC]
--------------------------------------------------------------------------------
  Total Source Committed Mutations: 500
    - INSERT Operations           : 266
    - UPDATE Operations           : 142
    - DELETE Operations           : 92
--------------------------------------------------------------------------------
  Run-Window Downstream Events    : 500
    - 'c' (Create / Insert)       : 266
    - 'u' (Update)                : 142
    - 'd' (Delete)                : 92
--------------------------------------------------------------------------------
  Missing Mutations in Kafka      : 0
  Historical / Out-of-Window Noise: Filtered Out
--------------------------------------------------------------------------------
  VERIFICATION STATUS             : [PASS] EXACT 1:1 ZERO DATA LOSS CONFIRMED!
================================================================================
```

--------------------------------------------------------------------------------

## Milestone 2 (M2): Rich Schema Fidelity, Monotonic Ordering & Latency SLAs

### Objective

Conduct deep validation across four production readiness criteria:

1.  **Completeness & Zero Data Loss**: `Missing Mutations = 0`.
2.  **Field-Level Value Fidelity**: 100% exact value equivalence across 8
    GoogleSQL column types (`INT64`, `NUMERIC`, `JSON`, `TIMESTAMP`, `BOOL`,
    `BYTES`).
3.  **Strict Per-Key Monotonic Ordering**: Guarantee that for any primary key
    `UserId`, events are delivered in strictly increasing order of Spanner
    TrueTime commit timestamps and record sequence numbers.
4.  **End-to-End Replication Latency SLAs**: Compute P50, P95, P99, and average
    replication lag in milliseconds.

### Target Benchmark Table DDL (Rich Schema)

```sql
CREATE TABLE BenchmarkUsers (
    UserId INT64 NOT NULL,
    UserName STRING(256),
    UserEmail STRING(256),
    AccountBalance NUMERIC,
    Metadata JSON,
    LastLogin TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    IsActive BOOL,
    BinarySignature BYTES(MAX)
) PRIMARY KEY (UserId);

CREATE CHANGE STREAM mycs FOR BenchmarkUsers
  OPTIONS (retention_period = '7d', value_capture_type = 'OLD_AND_NEW_VALUES', partition_mode = 'mutable_key_range');
```

### Execution Workflow

1.  **Rich Schema Mutation Load**: Worker threads populate and update all 8
    typed columns (`AccountBalance`, `Metadata`, `BinarySignature`, etc.) with
    randomized values.
2.  **Version-Aware BigQuery SQL Reconciliation**:
    *   Uses window functions (`ROW_NUMBER() OVER (PARTITION BY user_id, op
        ORDER BY commit_ts)`) to accurately correlate multiple updates to the
        same row with their corresponding CDC versions.
    *   Compares numeric decimal values using `SAFE_CAST(val AS NUMERIC)` to
        handle string formatting normalization.
    *   Asserts `commit_ts < prev_commit_ts` ordering rules per primary key.

### Command to Run

```bash
python3 run_m2_verification.py \
  --project "<my-gcp-project-id>" \
  --instance "<spanner-instance-id>" \
  --database "<database-name>" \
  --table BenchmarkUsers \
  --topic cdc.BenchmarkUsers \
  --bucket "<my-cdc-audit-bucket>" \
  --run-id run_m2_test_001 \
  --operations 500 \
  --threads 8 \
  --insert-ratio 0.5 \
  --update-ratio 0.3 \
  --delete-ratio 0.2 \
  --key-file "/path/to/service-account-key.json"
```

### Expected Output Report

```text
================================================================================
         MILESTONE 2 VERIFICATION RESULTS - RICH SCHEMA & ORDERING AUDIT
================================================================================
  Run Identifier                  : run_m2_test_001
  Target Spanner Table            : BenchmarkUsers
  Kafka CDC Topic                 : cdc.BenchmarkUsers
  Run Time Window                 : [2026-08-04 18:30:02 UTC -> 2026-08-04 18:30:35 UTC]
--------------------------------------------------------------------------------
  Total Source Committed Mutations: 500
    - INSERT Operations           : 258
    - UPDATE Operations           : 147
    - DELETE Operations           : 95
--------------------------------------------------------------------------------
  Run-Window Downstream Events    : 500
    - 'c' (Create / Insert)       : 258
    - 'u' (Update)                : 147
    - 'd' (Delete)                : 95
--------------------------------------------------------------------------------
  1. Completeness (Missing Rows)  : 0  [PASS: 0]
  2. Field-Level Value Fidelity   : 0 Mismatches  [PASS: 0]
     - Checked Fields: INT64, NUMERIC, JSON, TIMESTAMP, BOOL, BYTES
  3. Strict Per-Key Ordering      : 0 Violations  [PASS: 0]
  4. Replication Latency SLA      : P50=2510ms | P95=3420ms | P99=3890ms (Avg=2480.5ms)
--------------------------------------------------------------------------------
  VERIFICATION STATUS             : [PASS] MILESTONE 2 FULL COMPLIANCE CONFIRMED!
================================================================================
```

--------------------------------------------------------------------------------

## 6. Placement Table End-to-End Verification (`run_placement_verification.py`)

### Overview

Validates Cloud Spanner Placement Tables featuring non-primary key placement columns (`PlacementKey STRING NOT NULL PLACEMENT KEY`). This test verifies that CDC mutations (`c`, `u`, `d`) with placement key attributes map correctly in Debezium schemas without primary key index mismatches.

### Target Benchmark Placement Table DDL

```sql
CREATE TABLE BenchmarkPlacementUsers (
    UserId INT64 NOT NULL,
    PlacementKey STRING(MAX) NOT NULL PLACEMENT KEY,
    UserName STRING(256),
    UserEmail STRING(256),
    AccountBalance NUMERIC,
    Metadata JSON,
    LastLogin TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    IsActive BOOL,
    BinarySignature BYTES(MAX)
) PRIMARY KEY (UserId);

CREATE CHANGE STREAM placement_cs FOR BenchmarkPlacementUsers
  OPTIONS (retention_period = '7d', value_capture_type = 'OLD_AND_NEW_VALUES', partition_mode = 'mutable_key_range');
```

### Execution Command

```bash
python3 run_placement_verification.py \
  --project "<my-gcp-project-id>" \
  --instance "<spanner-instance-id>" \
  --database "<database-name>" \
  --table BenchmarkPlacementUsers \
  --topic cdc.BenchmarkPlacementUsers \
  --bucket "<my-cdc-audit-bucket>" \
  --run-id run_placement_001 \
  --operations 500 \
  --threads 8 \
  --key-file "/path/to/service-account-key.json"
```

### Expected Output Report

```text
================================================================================
         PLACEMENT TABLE VERIFICATION RESULTS - PLACEMENT & ORDERING AUDIT
================================================================================
  Run Identifier                  : run_placement_001
  Target Spanner Table            : BenchmarkPlacementUsers
  Kafka CDC Topic                 : cdc.BenchmarkPlacementUsers
  Run Time Window                 : [2026-08-05 05:23:43 UTC -> 2026-08-05 05:24:03 UTC]
--------------------------------------------------------------------------------
  Total Source Committed Mutations: 500
    - INSERT Operations           : 250
    - UPDATE Operations           : 150
    - DELETE Operations           : 100
--------------------------------------------------------------------------------
  Run-Window Downstream Events    : 500
    - 'c' (Create / Insert)       : 250
    - 'u' (Update)                : 150
    - 'd' (Delete)                : 100
--------------------------------------------------------------------------------
  1. Completeness (Missing Rows)  : 0  [PASS: 0]
  2. Field-Level Value Fidelity   : 0 Mismatches  [PASS: 0]
     - Checked Fields: PlacementKey (PLACEMENT KEY), INT64, NUMERIC, JSON, TIMESTAMP, BOOL, BYTES
  3. Strict Per-Key Ordering      : 0 Violations  [PASS: 0]
  4. Replication Latency SLA      : P50=2450ms | P95=3310ms | P99=3780ms (Avg=2410.2ms)
--------------------------------------------------------------------------------
  VERIFICATION STATUS             : [PASS] PLACEMENT TABLE FULL COMPLIANCE CONFIRMED!
================================================================================
```

--------------------------------------------------------------------------------

## 7. Vendor Best Practices & Troubleshooting Guide

### 1. Large Historical Backlog in Kafka Topics

*   **Issue**: When running repeated tests against persistent Kafka topics,
    consuming `--from-beginning` can cause collectors to read millions of old
    messages, slowing down verification.
*   **Solution**: The verification harnesses (`run_m1_verification.py` and
    `run_m2_verification.py`) automatically query partition high watermarks
    before generating load and pass `--start-offsets-json` to the collector.
    This ensures only newly emitted records are evaluated.

### 2. Multi-Partition Consumption in Python

*   **Issue**: A single-threaded `kafka-console-consumer` consuming across
    multiple partitions with `--timeout-ms` will exit as soon as any single
    partition times out, potentially leaving other partitions unread.
*   **Solution**: Always use `collect/kafka_gcs_collector.py` with
    `--drain-partitions`, which spawns a concurrent thread pool
    (`ThreadPoolExecutor`) dedicated to reading each partition independently
    until its watermark is reached.

### 3. Debezium DELETE Tombstone Messages

*   **Issue**: In Debezium CDC specifications, a `DELETE` operation produces two
    Kafka records:
    1.  A data change record with `"op": "d"`, `"before": { ... }`, `"after":
        null`.
    2.  A tombstone record with the primary key and `"value": null`.
*   **Solution**: Ensure custom consumers and JSON parsers check
    `isinstance(record, dict)` and `isinstance(payload, dict)` before inspecting
    operation fields to prevent `NoneType` attribute exceptions on tombstone
    messages.
