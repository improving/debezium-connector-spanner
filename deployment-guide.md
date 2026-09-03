# Deploying the Debezium Spanner Connector on Kubernetes

## Overview

This guide explains how to redeploy the Debezium Spanner Connector to a Kafka Connect cluster running on Kubernetes. You will build and publish a new connector image, then update the existing Kafka Connect deployment to use it.

Build the connector image from the [`delivery-aug-28`](https://github.com/improving/debezium-connector-spanner/tree/delivery-aug-28) branch.

## Build and Publish the Docker Image

### 1. Update the Dockerfile

Replace the contents of `src/test/docker/Dockerfile` with the following:

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

### 2. Build the Connector JAR
Ensure your local branch is tracking the correct branch you want to build against.  E.g. `git branch` should point to your target branch like `delivery-aug-28`.
From the root of the `debezium-connector-spanner` repository, run:

```bash
mvn clean generate-sources compile
```

Then package the connector:

```bash
mvn package \
  -Dmaven.test.skip=true \
  -Ppack-local-changes \
  -Ddocker.skip=true
```

### 3. Build and Push the Docker Image

N.B. Replace `<REGISTRY_USERNAME>` with your Docker registry username and `<DOCKER_TAG>` with the desired image tag (for example, `1.0.0`).
```bash
docker buildx build \
  --platform linux/amd64 \
  --build-arg projectVersion=3.6.0.Final \
  -f ./src/test/docker/Dockerfile \
  -t <REGISTRY_USERNAME>/kafka-spanner-connector:<DOCKER_TAG> \
  --push .
```

### 4. Update the Kubernetes Deployment

Use created image for Kubernetes deployment or re-deployment
Update Kubernetes deployment file to include image created on Step 3.
```yaml
image: <REGISTRY_USERNAME>/kafka-spanner-connector:<DOCKER_TAG>
```

Use the same registry username and image tag that you specified when building the image.
Then re-deploy your Kubernetes environment.

## Post-deployment notes
Once Debezium has released official release 3.7.0, update deployment with Debezium version of image.