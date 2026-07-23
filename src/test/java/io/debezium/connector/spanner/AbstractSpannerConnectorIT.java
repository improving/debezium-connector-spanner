/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Pattern;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaConnectRestClient;
import io.debezium.connector.spanner.util.KafkaEnvironment;
import io.debezium.connector.spanner.util.PartitionMode;
import io.debezium.connector.spanner.util.RealModeRecordPoller;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.function.BooleanConsumer;
import io.debezium.util.Testing;

public class AbstractSpannerConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String KAFKA_CONNECT_MODE_PROPERTY = "debezium.test.kafka-connect.mode";
    private static final String REAL_MODE = "real";

    private static final String REAL_MODE_CONNECT_REST_URL = "http://localhost:8083";

    private static KafkaConnectRestClient connectRestClient;
    private static RealModeRecordPoller realModePoller;
    private static String activeRealModeConnectorName;

    private static final KafkaEnvironment KAFKA_ENVIRONMENT = new KafkaEnvironment(
            KafkaEnvironment.DOCKER_COMPOSE_FILE);
    protected static final Database database = Database.TEST_DATABASE;
    protected static final Connection databaseConnection = database.getConnection();
    protected static final Database pgDatabase = Database.TEST_PG_DATABASE;
    protected static final Connection pgDatabaseConnection = pgDatabase.getConnection();
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    static {
        if (!KAFKA_ENVIRONMENT.isStarted()) {
            Testing.Print.enable();
            KAFKA_ENVIRONMENT.start();
            KAFKA_ENVIRONMENT.setStarted();
        }
    }

    protected static final Configuration baseConfig;
    static {
        Configuration.Builder builder = Configuration.create()
                .with("gcp.spanner.instance.id", database.getInstanceId())
                .with("gcp.spanner.project.id", database.getProjectId())
                .with("gcp.spanner.database.id", database.getDatabaseId())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("connector.spanner.sync.kafka.bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("internal.schema.history.kafka.bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("heartbeat.interval.ms", "300000")
                .with("gcp.spanner.low-watermark.enabled", false)
                .with("tasks.max", 3); // see DBZ-8428
        if (!Database.isRealSpannerMode()) {
            // Real mode: leave the emulator host unset so the connector falls back to the real
            // Spanner endpoint and resolves Application Default Credentials, matching the harness.
            builder.with("gcp.spanner.emulator.host", emulatorHostForConnector());
        }
        if (System.getProperty(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME) != null) {
            builder.with(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME, System.getProperty(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME));
        }
        if (System.getProperty("gcp.spanner.host") != null) {
            builder.with("gcp.spanner.host", System.getProperty("gcp.spanner.host"));
        }
        if (System.getProperty("spanner.omni.use.plaintext") != null) {
            builder.with("spanner.omni.use.plaintext", System.getProperty("spanner.omni.use.plaintext"));
        }
        if (System.getProperty("spanner.omni.client.key.path") != null && System.getProperty("spanner.omni.client.cert.path") != null) {
            builder.with("spanner.omni.client.key.path", System.getProperty("spanner.omni.client.key.path"));
            builder.with("spanner.omni.client.cert.path", System.getProperty("spanner.omni.client.cert.path"));
        }
        baseConfig = builder.build();
    }

    protected static final Configuration basePgConfig = Configuration.copy(baseConfig)
            .with("gcp.spanner.instance.id", pgDatabase.getInstanceId())
            .with("gcp.spanner.project.id", pgDatabase.getProjectId())
            .with("gcp.spanner.database.id", pgDatabase.getDatabaseId())
            .build();

    @BeforeAll
    public static void before() throws InterruptedException {
        Testing.Print.enable();
    }

    @AfterAll
    public static void after() throws InterruptedException {
        Testing.print("Cleaning up kafka...");
        KAFKA_ENVIRONMENT.clearTopics();
        Testing.print("Cleaning complete!");
    }

    protected static void clearKafkaTopics() {
        KAFKA_ENVIRONMENT.clearTopics();
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "30"));
    }

    protected static boolean hasNonEmulatorBackend() {
        return Database.isSpannerOmniEndpoint() || Database.isRealSpannerMode();
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = isRealConnectMode()
                ? config.getString(BaseSpannerConnectorConfig.CONNECTOR_NAME_PROPERTY_NAME)
                : "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }

    private static boolean isRealConnectMode() {
        return REAL_MODE.equalsIgnoreCase(System.getProperty(KAFKA_CONNECT_MODE_PROPERTY, "embedded"));
    }

    private static String kafkaBootstrapServersForConnector() {
        return isRealConnectMode()
                ? KAFKA_ENVIRONMENT.kafkaBrokerContainerNetworkAddress()
                : KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress();
    }

    private static String emulatorHostForConnector() {
        return isRealConnectMode() ? Connection.containerNetworkEmulatorHost : Connection.emulatorHost;
    }

    private static String readApplicationDefaultCredentialsJson() {
        String credentialsPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credentialsPath == null || credentialsPath.isEmpty()) {
            credentialsPath = System.getProperty("user.home") + "/.config/gcloud/application_default_credentials.json";
        }
        if (!java.nio.file.Files.exists(Paths.get(credentialsPath))) {
            throw new IllegalStateException(
                    "No Application Default Credentials found to forward to the real Kafka Connect worker "
                            + "when both real-connect and real-Spanner modes are active. Set GOOGLE_APPLICATION_CREDENTIALS "
                            + "or run 'gcloud auth application-default login --impersonate-service-account=<sa-email>'");
        }
        try {
            return new String(java.nio.file.Files.readAllBytes(Paths.get(credentialsPath)));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read Application Default Credentials file at " + credentialsPath, e);
        }
    }

    private static KafkaConnectRestClient connectRestClient() {
        if (connectRestClient == null) {
            connectRestClient = new KafkaConnectRestClient(REAL_MODE_CONNECT_REST_URL);
        }
        return connectRestClient;
    }

    @Override
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig) {
        if (!isRealConnectMode()) {
            super.start(connectorClass, connectorConfig);
            return;
        }

        KafkaConnectRestClient client = connectRestClient();
        client.waitForWorkerReady(KafkaEnvironment.STARTUP_TIMEOUT);

        Map<String, String> configMap = new HashMap<>(connectorConfig.asMap());
        configMap.put("connector.class", connectorClass.getName());
        String connectorName = connectorConfig.getString(BaseSpannerConnectorConfig.CONNECTOR_NAME_PROPERTY_NAME);

        if (Database.isRealSpannerMode()) {
            configMap.put("gcp.spanner.credentials.json", readApplicationDefaultCredentialsJson());
        }

        client.deployConnector(connectorName, configMap);
        client.waitForConnectorRunning(connectorName, KafkaEnvironment.STARTUP_CONNECTOR_TIMEOUT);
        activeRealModeConnectorName = connectorName;

        if (consumedLines == null) {
            consumedLines = new ArrayBlockingQueue<>(getMaximumEnqueuedRecordCount());
        }
        Pattern topicPattern = Pattern.compile("^" + Pattern.quote(connectorName) + "\\..*$");
        realModePoller = new RealModeRecordPoller(KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress(), topicPattern, consumedLines);
        realModePoller.start();
    }

    @Override
    public void stopConnector(BooleanConsumer callback) {
        if (!isRealConnectMode() || activeRealModeConnectorName == null) {
            super.stopConnector(callback);
            return;
        }

        boolean stoppedSuccessfully = true;
        try {
            if (realModePoller != null) {
                realModePoller.stop();
                realModePoller = null;
            }
            connectRestClient().deleteConnector(activeRealModeConnectorName);
            connectRestClient().waitForConnectorAbsent(activeRealModeConnectorName, KafkaEnvironment.CONFIGURE_CONNECTOR_TIMEOUT);
        }
        catch (RuntimeException e) {
            stoppedSuccessfully = false;
            logger.warn("Failed to stop connector '{}' in real mode", activeRealModeConnectorName, e);
        }
        finally {
            activeRealModeConnectorName = null;
        }
        if (callback != null) {
            callback.accept(!stoppedSuccessfully);
        }
    }

    @Override
    protected void assertConnectorIsRunning() {
        if (!isRealConnectMode()) {
            super.assertConnectorIsRunning();
            return;
        }
        assertThat(activeRealModeConnectorName != null && connectRestClient().isConnectorRunning(activeRealModeConnectorName)).isTrue();
    }

    @Override
    protected void assertConnectorNotRunning() {
        if (!isRealConnectMode()) {
            super.assertConnectorNotRunning();
            return;
        }
        boolean running = activeRealModeConnectorName != null && connectRestClient().isConnectorRunning(activeRealModeConnectorName);
        assertThat(running).isFalse();
    }

    protected void assumeSupportedPartitionMode(PartitionMode partitionMode) {
        boolean unsupported = partitionMode == PartitionMode.MUTABLE_KEY_RANGE && !hasNonEmulatorBackend();
        Assumptions.assumeFalse(unsupported,
                () -> partitionMode + " is not yet supported here: the local Docker Spanner emulator's DDL parser "
                        + "rejects partition_mode outright, so a MUTABLE_KEY_RANGE stream can't be created against "
                        + "it at all - re-run with -Dspanner.type=OMNI -Dgcp.spanner.host=<host:port> "
                        + "-Dspanner.omni.use.plaintext=true against a running Spanner Omni instance, or with "
                        + "-Preal-spanner against a real Cloud Spanner instance, instead");
    }
}
