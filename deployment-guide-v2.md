# Deploying the Debezium Spanner Connector on GKE

## Overview

This guide walks through deploying the Debezium Spanner Connector in a Kafka Connect cluster running on Google Kubernetes Engine (GKE). It covers provisioning a Cloud Spanner instance, building and publishing a connector Docker image, deploying the GKE cluster with Terraform, and registering a connector to begin capturing change data.

By the end, you will have:

- A Cloud Spanner instance with Change Streams enabled
- A containerized Kafka Connect cluster running on GKE
- A registered Debezium Spanner connector publishing CDC events to Kafka topics
- Grafana and Prometheus configured for observability

> **Estimated total time:** 60–90 minutes, including approximately 30 minutes of unattended Terraform provisioning.

---

## Who This Guide Is For

This guide assumes familiarity with Google Cloud Platform, Kubernetes, and Kafka. It is intended for platform engineers or data engineers deploying Debezium in a production or staging environment.

---

## Prerequisites

Ensure the following tools are installed and authenticated before proceeding:

| Tool | Purpose | Install Link |
|------|---------|--------------|
| `gcloud` CLI | Interact with GCP resources | [Install](https://cloud.google.com/sdk/docs/install) |
| Docker (with `buildx`) | Build and push connector images | [Install](https://docs.docker.com/get-docker/) |
| Terraform | Provision GKE infrastructure | [Install](https://developer.hashicorp.com/terraform/install) |
| `kubectl` | Manage the Kubernetes cluster | [Install](https://kubernetes.io/docs/tasks/tools/) |
| Maven | Build the connector JAR | [Install](https://maven.apache.org/install.html) |

You will also need:

- A GCP project with billing enabled
- A GCP Service Account with a downloaded JSON key file
- A [Docker Hub](https://hub.docker.com/) account with push access
- The `debezium-connector-spanner` source repository cloned locally
- The `debezium.connector.ops` operations repository cloned locally

> **Note:** The Service Account used for GKE provisioning requires the following IAM roles at minimum: `Kubernetes Engine Admin`, `Service Account User`, and `Storage Admin`. The Service Account used for Spanner requires `Cloud Spanner Admin`.

---

## Part 1: Google Cloud Spanner Setup

### 1.1 Create a Spanner Instance

1. In the [Google Cloud Console](https://console.cloud.google.com), search for **Spanner** and open the service.
2. Click **Create Instance** and fill in the following:

   | Field   | Value                                               |
   |---------|-----------------------------------------------------|
   | Edition | `Enterprise-Plus` *(required for geo-partitioning)* |
   | Name    | `<INSTANCE_NAME>`                                   |
   | ID      | `<INSTANCE_ID>`                                     |
   | Region  | `Multi-Region nam10` *(or your preferred region)*   |
   | Nodes   | `1` *(scale up for production workloads)*           |

3. Click **Create**.

> **Scaling note:** A single node is sufficient for testing. For production workloads or high-throughput CDC, increase the node count based on your expected transaction volume.

### 1.2 Grant Service Account Permissions

1. In the Spanner instance list, check the checkbox next to your instance.
2. Click **Permissions** in the top action bar.
3. Add your GCP Service Account with the role **Cloud Spanner Admin**.

### Checkpoint 1

Verify the instance appears as **Ready** in the Spanner instance list before continuing.

---

## Part 2: Build and Publish the Docker Image

This section packages the connector and publishes it to Docker Hub so that GKE can pull it during deployment.

### 2.1 Set Up Docker Hub Authentication

1. Create a [Docker Hub](https://hub.docker.com/) account if you do not already have one.
2. Log in from your terminal:
   ```bash
   docker login
   ```
3. Generate a Docker Hub **Access Token** with Read & Write permissions:
   - Navigate to **Account Settings → Security → Access Tokens → New Access Token**
   - Save the token; you will need it when configuring Terraform in Part 3.

### 2.2 Build the Connector JAR

From the root of the `debezium-connector-spanner` repository:

```bash
mvn clean package \
  -Dmaven.test.skip=true \
  -Ppack-local-changes \
  -Ddocker.skip=true
```

> **Note:** `-Dmaven.test.skip=true` skips the test suite to speed up the build. Remove this flag if you want to run tests locally before packaging.

### 2.3 Build and Push the Docker Image

```bash
docker buildx build \
  --platform linux/amd64 \
  --build-arg projectVersion=3.6.0.Final \
  -f ./src/test/docker/Dockerfile \
  -t <DOCKER_HUB_USERNAME>/kafka-spanner-connector:<DOCKER_TAG> \
  --push .
```

Replace `<DOCKER_HUB_USERNAME>` and `<DOCKER_TAG>` with your Docker Hub username and a version tag (e.g., `1.0.0`).

> **Platform note:** The `--platform linux/amd64` flag is required if you are building on Apple Silicon (arm64) or any non-amd64 host. GKE nodes run on amd64 by default.

### Checkpoint 2

Verify the image appears in your Docker Hub repository at `https://hub.docker.com/r/<DOCKER_HUB_USERNAME>/kafka-spanner-connector` before continuing.

---

## Part 3: GKE Cluster Setup with Terraform

### 3.1 Configure Terraform Variables

Open `terraform.tfvars` in the root of the `debezium.connector.ops` repository and configure the following:

```hcl
project           = "<GCP_PROJECT_ID>"
region            = "us-central1"
location          = "us-central1-a"
instance_type     = "e2-highmem-16"
node_count        = "18"
gcp_auth_file     = "<SERVICE_ACCOUNT_KEY_RELATIVE_PATH>"
app_name          = "spanner-connector"
registry_username = "<DOCKER_HUB_USERNAME>"
registry_password = "<DOCKER_HUB_TOKEN>"
registry_email    = "<DOCKER_HUB_EMAIL>"
registry_server   = "docker.io"
```

> **Sizing guidance:** The default configuration provisions 18 nodes and 36 Kafka Connect replicas — 2 replicas per node. If you need to scale the deployment up or down, keep `replicaCount` in `values.yaml` (Part 3.2) at approximately 2× the `node_count` here to maintain even pod distribution.

### 3.2 Configure the Kafka Connect Helm Values

Open `connector/values.yaml` and update the image fields:

```yaml
image: <DOCKER_HUB_USERNAME>/kafka-spanner-connector
imageTag: "<DOCKER_TAG>"
```

The full `values.yaml` reference is provided below. The fields most likely to need adjustment for your environment are:

- **`replicaCount`** — number of Kafka Connect pods (keep at 2× `node_count`)
- **`bootstrap.servers`** — Kafka broker address inside the cluster
- **`configurationOverrides`** — converter, replication factor, and topic names
- **`resources`** — CPU and memory limits per pod
- **`heapOptions`** — JVM heap sizing (set to ~50% of pod memory limit)

<details>
<summary>Full <code>connector/values.yaml</code> reference</summary>

```yaml
replicaCount: 36

image: <DOCKER_HUB_USERNAME>/kafka-spanner-connector
imageTag: "<DOCKER_TAG>"

imagePullPolicy: Always

imagePullSecrets:
  - name: kafka-connect

servicePort: 8083

configurationOverrides:
  "plugin.path": "/usr/share/java,/usr/share/confluent-hub-components/,/opt/kafka/spanner/"
  "key.converter": "org.apache.kafka.connect.json.JsonConverter"
  "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  "config.storage.replication.factor": "3"
  "offset.storage.replication.factor": "3"
  "status.storage.replication.factor": "3"
  "config.storage.topic": "_kafka-connect-configs"
  "offset.storage.topic": "_kafka-connect-offsets"
  "status.storage.topic": "_kafka-connect-status"
  "offset.flush.interval.ms": "200"
  "task.shutdown.graceful.timeout.ms": "60000"
  "bootstrap.servers": kafka-cp-kafka-headless:9092
  "rest.advertised.port": "8083"
  "group.id": "kafka-new-group-reader"
  "key.converter.schema.registry.url": "http://kafka-cp-schema-registry:8081"
  "value.converter.schema.registry.url": "http://kafka-cp-schema-registry:8081"
  "log4j.rootLogger": "INFO"
  "log4j.logger.org.apache.kafka.connect.runtime.res": "WARN"
  "log4j.logger.org.reflections": "ERROR"

heapOptions: '-Xms10g -Xmx10g -XX:+UseG1GC -Xlog:gc'

customEnv:
  JAVA_OPTS: >-
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Dcom.sun.management.jmxremote.local.only=false
    -Dcom.sun.management.jmxremote.port=5555
    -Dcom.sun.management.jmxremote.rmi.port=5555
    -Djava.rmi.server.hostname=$(POD_IP)

resources:
  limits:
    cpu: 6000m
    memory: 20Gi
  requests:
    cpu: 4000m
    memory: 20Gi

jmx:
  port: 5555

prometheus:
  jmx:
    enabled: true
    image: solsson/kafka-prometheus-jmx-exporter@sha256
    imageTag: 6f82e2b0464f50da8104acd7363fb9b995001ddff77d248379f8788e78946143
    imagePullPolicy: IfNotPresent
    port: 5556
    resources:
      limits:
        cpu: 500m
        memory: 512Mi
      requests:
        cpu: 100m
        memory: 256Mi

livenessProbe:
  httpGet:
    path: /connectors
    port: 8083
  initialDelaySeconds: 90
  periodSeconds: 15
  failureThreshold: 100

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/path: /metrics
  prometheus.io/port: "5556"
```

</details>

### 3.3 Deploy the Infrastructure

From the root of the `debezium.connector.ops` repository:

```bash
terraform init
terraform apply
```

Review the plan output and type `yes` when prompted.

> **Expected duration:** Approximately 30 minutes. If the apply runs significantly longer, check whether your GCP project has sufficient quota for the selected `instance_type` and `node_count`. Reducing either value and re-running `terraform apply` will update the cluster in place.

### Checkpoint 3

Once `terraform apply` completes successfully, verify the cluster is visible in the [GCP Console → Kubernetes Engine → Clusters](https://console.cloud.google.com/kubernetes) before continuing.

---

## Part 4: Expose Services in GKE

After the cluster is provisioned, four workloads need to be exposed as external LoadBalancer services so they are reachable from outside the cluster.

1. In the [Google Cloud Console](https://console.cloud.google.com), navigate to **Kubernetes Engine → Workloads**.
2. Filter workloads by the label tag `spanner-connector`.
3. For each workload listed below, click its name → **Actions → Expose**, enter the port in both the **Port** and **Target port** fields, leave the type as **Load Balancer**, and confirm.

   | Workload                         | Port |
   |----------------------------------|------|
   | `akhq`                           | 8080 |
   | `grafana`                        | 3000 |
   | `kafka-connect-cp-kafka-connect` | 8083 |
   | `prometheus-server`              | 9000 |

> **Production note:** Using a LoadBalancer service exposes each endpoint to the public internet. For production environments, consider restricting access with firewall rules, or use an Ingress controller with TLS termination and authentication instead of directly exposing these services.

### Checkpoint 4

Confirm that each workload shows a green status and an external IP is assigned in the GKE Workloads view before continuing.

---

## Part 5: Connect to the Cluster Locally

> **Local development only:** The `kubectl port-forward` approach in this section is intended for local testing and debugging. For persistent access in a production environment, use the LoadBalancer external IPs provisioned in Part 4, or deploy an Ingress with appropriate authentication.

### 5.1 Fetch Cluster Credentials

```bash
gcloud container clusters get-credentials spanner-connector --zone us-central1-a
```

### 5.2 Start Port Forwards

Run the following to forward cluster services to your local machine:

```bash
kubectl port-forward service/kafka-connect-cp-kafka-connect -n default 8083:8083 &
kubectl port-forward service/grafana -n default 3000:80 &
kubectl port-forward service/akhq -n default 8080:80 &
kubectl port-forward service/prometheus-server -n default 9000:80 &
```

Services will be accessible at:

| Service       | Local URL              |
|---------------|------------------------|
| Kafka Connect | http://localhost:8083  |
| Grafana       | http://localhost:3000  |
| AKHQ          | http://localhost:8080  |
| Prometheus    | http://localhost:9000  |

### Checkpoint 5

Verify Kafka Connect is running:

```bash
curl http://localhost:8083/connectors
```

A healthy response returns an empty JSON array `[]` or a list of registered connector names.

---

## Part 6: Create the Spanner Database and Change Stream

In a new terminal, create the Spanner database and enable Change Streams on your target table:

```bash
gcloud spanner databases create load-test \
  --instance=spanner-kafka-connector

gcloud spanner databases ddl update load-test \
  --instance=spanner-kafka-connector \
  --ddl="CREATE TABLE BenchmarkUsers (
    UserId INT64 NOT NULL,
    UserName STRING(MAX)
  ) PRIMARY KEY (UserId);
  CREATE CHANGE STREAM mycs FOR BenchmarkUsers;"
```

This creates:
- A database named `load-test`
- A table `BenchmarkUsers` with a primary key on `UserId`
- A Change Stream `mycs` that captures all changes to `BenchmarkUsers`

> **Customizing for your schema:** Replace `BenchmarkUsers` and `mycs` with your own table and stream names. The `FOR BenchmarkUsers` clause restricts the stream to a single table; use `FOR ALL` to capture changes across the entire database.

### Checkpoint 6

Verify the database was created:

```bash
gcloud spanner databases list --instance=spanner-kafka-connector
```

---

## Part 7: Register the Connector

With Kafka Connect running and the Spanner database ready, register the Debezium Spanner Connector via the Kafka Connect REST API.

Create a file named `connector-config.json`:

```json
{
  "name": "spanner-connector",
  "config": {
    "connector.class": "io.debezium.connector.spanner.SpannerConnector",
    "gcp.spanner.project.id": "<GCP_PROJECT_ID>",
    "gcp.spanner.instance.id": "<INSTANCE_ID>",
    "gcp.spanner.database.id": "load-test",
    "gcp.spanner.change.stream": "mycs",
    "gcp.spanner.credentials.json": "<BASE64_ENCODED_SERVICE_ACCOUNT_KEY>",
    "kafka.topic.prefix": "spanner",
    "tasks.max": "1"
  }
}
```

> **Credentials:** The `gcp.spanner.credentials.json` field should contain the contents of your service account JSON key file, base64-encoded. Alternatively, if your GKE nodes run under a service account with Spanner access, you can omit this field and rely on Workload Identity.

Submit the configuration to Kafka Connect:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

### Checkpoint 7

Verify the connector is running:

```bash
curl http://localhost:8083/connectors/spanner-connector/status
```

The `state` field should read `RUNNING`. You can monitor connector activity and topic throughput in AKHQ at http://localhost:8080.

---

## Appendix A: Placeholder Reference

| Placeholder                           | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `<INSTANCE_NAME>`                     | Display name for your Spanner instance              |
| `<INSTANCE_ID>`                       | Unique ID for your Spanner instance                 |
| `<GCP_PROJECT_ID>`                    | Your GCP project ID                                 |
| `<SERVICE_ACCOUNT_KEY_RELATIVE_PATH>` | Relative path to your GCP service account JSON key  |
| `<DOCKER_HUB_USERNAME>`               | Your Docker Hub username                            |
| `<DOCKER_HUB_TOKEN>`                  | Your Docker Hub access token                        |
| `<DOCKER_HUB_EMAIL>`                  | Your Docker Hub account email                       |
| `<DOCKER_TAG>`                        | Tag for your connector Docker image (e.g., `1.0.0`) |
| `<BASE64_ENCODED_SERVICE_ACCOUNT_KEY>`| Base64-encoded contents of your service account key |

---

## Appendix B: Troubleshooting

### Terraform apply times out or hangs

- **Check GCP quotas:** Navigate to **IAM & Admin → Quotas** in the GCP Console and verify your project has sufficient CPU and memory quota in the target region for the requested `instance_type` and `node_count`.
- **Reduce cluster size:** Lower `node_count` in `terraform.tfvars` and `replicaCount` in `values.yaml` (keeping them at a 1:2 ratio) and re-run `terraform apply`.

### Pods fail to start with `ImagePullBackOff`

- Verify the image name and tag in `values.yaml` exactly match what was pushed to Docker Hub.
- Confirm the `registry_username`, `registry_password`, and `registry_email` values in `terraform.tfvars` are correct and that the token has Read & Write permissions.
- Check that the `kafka-connect` image pull secret was created in the cluster: `kubectl get secret kafka-connect -n default`.

### Pods crash with `OOMKilled`

- The pod's memory limit has been exceeded. Reduce `heapOptions` in `values.yaml` (e.g., from `-Xmx10g` to `-Xmx8g`) so the JVM heap stays below the container memory limit, or increase the `resources.limits.memory` value.

### Kafka Connect REST API returns connection refused

- Confirm port forwarding is active: `kubectl get pods -n default` should show all `kafka-connect-cp-kafka-connect` pods as `Running`.
- Restart the port forward: `kubectl port-forward service/kafka-connect-cp-kafka-connect -n default 8083:8083`.

### Connector status shows `FAILED`

- Retrieve the error: `curl http://localhost:8083/connectors/spanner-connector/status`
- Common causes:
  - **Invalid credentials:** Verify the service account key is correct and the account has the `Cloud Spanner Admin` role.
  - **Wrong instance or database ID:** Double-check the `gcp.spanner.instance.id` and `gcp.spanner.database.id` values in `connector-config.json`.
  - **Change stream not found:** Confirm the Change Stream was created successfully in Part 6.
