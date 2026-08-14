/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.util.Testing;

public class KafkaEnvironment {

    private static final String KAFKA_BROKER_SERVICE_NAME = "broker";
    private static final int KAFKA_BROKER_SERVICE_API_PORT = 9092;
    private static final int KAFKA_JMX_API_PORT = 9101;

    public static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(200L);
    public static final Duration STARTUP_CONNECTOR_TIMEOUT = Duration.ofSeconds(600L);
    public static final Duration CONFIGURE_CONNECTOR_TIMEOUT = Duration.ofSeconds(200L);

    public static final String DOCKER_COMPOSE_FILE = "src/test/java/io/debezium/connector/spanner/util/docker-compose.yml";

    private boolean isStarted = false;

    private ComposeContainer composeContainer;

    private KafkaBrokerApi<ObjectNode, ObjectNode> brokerApiOn;

    private final int brokerHostPort;

    public KafkaEnvironment(String dockerComposeFilePath) {
        Testing.Print.enable();
        Testing.print("Initializing kafka environment for IT test...");
        this.brokerHostPort = perForkPort(KAFKA_BROKER_SERVICE_API_PORT);
        int jmxHostPort = perForkPort(KAFKA_JMX_API_PORT);
        this.composeContainer = new ComposeContainer(new File(dockerComposeFilePath))
                .withEnv("KAFKA_HOST_PORT", String.valueOf(brokerHostPort))
                .withEnv("KAFKA_JMX_HOST_PORT", String.valueOf(jmxHostPort))
                .withExposedService(KAFKA_BROKER_SERVICE_NAME, KAFKA_BROKER_SERVICE_API_PORT,
                        Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        Testing.print("Finished initializing kafka environment.");
    }

    /**
     * Offsets {@code basePort} by the {@code test.forkNumber} system property, so that a build
     * running multiple forks with {@code reuseForks=false} gives each one its own Kafka broker
     * port instead of racing to bind the same fixed port. {@code test.forkNumber} isn't set
     * automatically - Surefire/Failsafe's {@code ${surefire.forkNumber}} is only a text-
     * substitution token usable inside plugin config elements like {@code argLine}, not a real
     * JVM system property, so the execution that wants per-fork ports must pass it through
     * explicitly, e.g. {@code <argLine>-Dtest.forkNumber=${surefire.forkNumber}</argLine>}. Falls
     * back to {@code basePort} unchanged when the property is absent (the common case: a single,
     * non-forked test run that never sets it).
     */
    private static int perForkPort(int basePort) {
        String forkNumberProperty = System.getProperty("test.forkNumber");
        if (forkNumberProperty == null) {
            return basePort;
        }
        try {
            return basePort + Integer.parseInt(forkNumberProperty);
        }
        catch (NumberFormatException e) {
            return basePort;
        }
    }

    public void start() {

        Testing.print("Starting Kafka environment");
        this.composeContainer.start();
        ContainerState brokerState = (ContainerState) composeContainer
                .getContainerByServiceName(KAFKA_BROKER_SERVICE_NAME)
                .orElseThrow();

        this.brokerApiOn = KafkaBrokerApi.createKafkaBrokerApiObjectNode(brokerState, brokerHostPort);
    }

    public KafkaBrokerApi<ObjectNode, ObjectNode> kafkaBrokerApiOn() {
        return brokerApiOn;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void setStarted() {
        isStarted = true;
    }

    public void clearTopics() {
        try (AdminClient adminClient = kafkaBrokerApiOn().createAdminClient()) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            Arrays.asList("_kafka-connect-configs",
                    "_kafka-connect-offsets",
                    "_kafka-connect-status",
                    "_kafka-connect-status",
                    "_schemas",
                    "_confluent-command",
                    "_confluent_balancer_api_state",
                    "_confluent-metrics",
                    "__consumer_offsets",
                    "_confluent-telemetry-metrics",
                    "_rebalancing_topic_spanner_connector_testing-connector").forEach(
                            topics::remove);
            adminClient.deleteTopics(topics);
        }
        catch (Exception e) {
            Testing.print("Error clearing Kafka topics: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void stop() {
        if (composeContainer != null) {
            composeContainer.stop();
        }
    }
}
