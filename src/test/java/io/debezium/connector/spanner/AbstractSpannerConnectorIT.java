/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Pattern;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaConnectRestClient;
import io.debezium.connector.spanner.util.KafkaEnvironment;
import io.debezium.connector.spanner.util.RealModeRecordPoller;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.function.BooleanConsumer;
import io.debezium.util.Testing;

public class AbstractSpannerConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String KAFKA_CONNECT_MODE_PROPERTY = "debezium.test.kafka-connect.mode";
    private static final String REAL_MODE = "real";

    private static final String REAL_MODE_CONNECT_REST_URL = "http://localhost:8083";
    private static final String DEFAULT_REAL_MODE_CONNECTOR_NAME = "testing-connector";
    private static final Pattern REAL_MODE_TOPIC_PATTERN = Pattern.compile("^testing-connector\\..*$");

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
                .with("gcp.spanner.emulator.host", emulatorHostForConnector())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("connector.spanner.sync.kafka.bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("internal.schema.history.kafka.bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("bootstrap.servers", kafkaBootstrapServersForConnector())
                .with("heartbeat.interval.ms", "300000")
                .with("gcp.spanner.low-watermark.enabled", false)
                .with("tasks.max", 3); // see DBZ-8428
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

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
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
        configMap.put(BaseSpannerConnectorConfig.CONNECTOR_NAME_PROPERTY_NAME, DEFAULT_REAL_MODE_CONNECTOR_NAME);

        client.deployConnector(DEFAULT_REAL_MODE_CONNECTOR_NAME, configMap);
        client.waitForConnectorRunning(DEFAULT_REAL_MODE_CONNECTOR_NAME, KafkaEnvironment.STARTUP_CONNECTOR_TIMEOUT);
        activeRealModeConnectorName = DEFAULT_REAL_MODE_CONNECTOR_NAME;

        if (consumedLines == null) {
            consumedLines = new ArrayBlockingQueue<>(getMaximumEnqueuedRecordCount());
        }
        realModePoller = new RealModeRecordPoller(KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress(), REAL_MODE_TOPIC_PATTERN, consumedLines);
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
}
