/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import io.debezium.connector.spanner.util.KafkaConnectRestClient;

/**
 * Standalone Kafka Connect setup/teardown for the MUTABLE_KEY_RANGE demo: deploys or removes
 * the Spanner source connector against a real, REST-reachable Kafka Connect worker via
 * {@code --action=deploy}/{@code --action=delete}. Running {@code delete} then {@code deploy}
 * again is how to restart/recreate the connector.
 */
public final class DemoKafkaConnectSetup {

    private static final String CONNECTOR_CLASS = "io.debezium.connector.spanner.SpannerConnector";
    private static final Duration CONNECTOR_RUNNING_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration CONNECTOR_ABSENT_TIMEOUT = Duration.ofMinutes(3);

    private DemoKafkaConnectSetup() {
    }

    public static void main(String[] args) {
        String action = argValue(args, "--action");
        if (action == null) {
            throw new IllegalArgumentException("Usage: DemoKafkaConnectSetup --action=deploy|delete");
        }

        KafkaConnectRestClient client = new KafkaConnectRestClient(connectRestUrl());
        switch (action) {
            case "deploy" -> deploy(client);
            case "delete" -> delete(client);
            default -> throw new IllegalArgumentException("Unknown action: " + action);
        }
    }

    private static void deploy(KafkaConnectRestClient client) {
        String connectorName = MutableKeyRangeDemoScenario.CONNECTOR_NAME;

        client.waitForWorkerReady(Duration.ofMinutes(2));

        Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("name", connectorName);
        config.put("tasks.max", "1");
        config.put("gcp.spanner.project.id", projectId());
        config.put("gcp.spanner.instance.id", instanceId());
        config.put("gcp.spanner.database.id", MutableKeyRangeDemoScenario.databaseId());
        config.put("gcp.spanner.change.stream", MutableKeyRangeDemoScenario.CHANGE_STREAM_NAME);
        config.put("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        config.put("gcp.spanner.mutable.window.minutes", "1");
        config.put("gcp.spanner.low-watermark.enabled", "false");
        config.put("heartbeat.interval.ms", "300000");
        config.put("connector.spanner.sync.kafka.bootstrap.servers", kafkaBootstrapServersContainer());
        config.put("internal.schema.history.kafka.bootstrap.servers", kafkaBootstrapServersContainer());
        config.put("bootstrap.servers", kafkaBootstrapServersContainer());
        config.put("gcp.spanner.credentials.json", readApplicationDefaultCredentialsJson());

        client.deployConnector(connectorName, config);
        client.waitForConnectorRunning(connectorName, CONNECTOR_RUNNING_TIMEOUT);
        System.out.println("Connector " + connectorName + " deployed and RUNNING");

        // This check is also repeated independently by DemoDataGenerator right before it issues
        // any DML.
        ConnectorTaskStabilityWaiter.waitForStable(connectorName);
    }

    private static void delete(KafkaConnectRestClient client) {
        String connectorName = MutableKeyRangeDemoScenario.CONNECTOR_NAME;
        client.deleteConnector(connectorName);
        client.waitForConnectorAbsent(connectorName, CONNECTOR_ABSENT_TIMEOUT);
        System.out.println("Connector " + connectorName + " deleted");
    }

    private static String connectRestUrl() {
        return System.getProperty("demo.kafka.connect.rest.url", "http://localhost:8083");
    }

    private static String kafkaBootstrapServersContainer() {
        return System.getProperty("demo.kafka.bootstrap.servers.container", "broker:29092");
    }

    private static String projectId() {
        return System.getProperty("gcp.spanner.project.id", "improvingvancouver");
    }

    private static String instanceId() {
        return System.getProperty("gcp.spanner.instance.id", "spanner-kafka-connector");
    }

    /**
     * Resolves Application Default Credentials to forward to the containerized Kafka Connect
     * worker, which has no access to the host's own ADC. Mirrors
     * {@code AbstractSpannerConnectorIT#readApplicationDefaultCredentialsJson}, duplicated here
     * since that method is private on a JUnit-coupled test base class this plain demo shouldn't
     * otherwise depend on.
     */
    private static String readApplicationDefaultCredentialsJson() {
        String credentialsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credentialsPath == null || credentialsPath.isEmpty()) {
            credentialsPath = System.getProperty("user.home") + "/.config/gcloud/application_default_credentials.json";
        }
        if (!Files.exists(Paths.get(credentialsPath))) {
            throw new IllegalStateException(
                    "No Application Default Credentials found to forward to the real Kafka Connect worker. "
                            + "Set GOOGLE_APPLICATION_CREDENTIALS or run "
                            + "'gcloud auth application-default login --impersonate-service-account=<sa-email>'");
        }
        try {
            return new String(Files.readAllBytes(Paths.get(credentialsPath)));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read Application Default Credentials file at " + credentialsPath, e);
        }
    }

    private static String argValue(String[] args, String name) {
        String prefix = name + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }
        return null;
    }
}
