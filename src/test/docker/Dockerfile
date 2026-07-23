FROM mirror.gcr.io/confluentinc/cp-kafka-connect-base:8.2.2
ARG projectVersion
ADD target/debezium-connector-spanner-${projectVersion}-plugin.tar.gz /usr/share/java/
COPY src/test/docker/jmx_prometheus_javaagent-0.16.1.jar /usr/share/prometheus/jmx_prometheus_javaagent.jar
COPY src/test/docker/metrics-config.yml /usr/share/prometheus/metrics-config.yml
