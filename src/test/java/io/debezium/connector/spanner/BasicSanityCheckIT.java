/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.cloud.spanner.Dialect;
import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RealSpannerCompatible
public class BasicSanityCheckIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicSanityCheckIT.class);

    private static final String tablePrefix = "embedded_sanity_tests_table";
    private static final String changeStreamPrefix = "embeddedSanityTestChangeStream";

    @Test
    public void shouldNotStartConnectorWithoutRequireConfigs() throws InterruptedException {
        // Config with only instance id provided.
        Configuration config = Configuration.create()
                .with("gcp.spanner.instance.id", database.getInstanceId())
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Connector configuration is not valid"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithoutNonExistentChangeStreams() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", "fooBar")
                .with("name", tablePrefix + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("ChangeStream 'fooBar' doesn't exist or you don't have sufficient permissions"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithOutOfRangeHeartbeatMillis() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamPrefix)
                .with("heartbeat.interval.ms", "1")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now().plus(2, ChronoUnit.DAYS)))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Heartbeat interval must be between 100 and 300000"));
        });
        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void shouldStreamUpdatesToKafka(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tablePrefix, null, dialect);
        String stream = streamFor(changeStreamPrefix, null, dialect);

        createTableAndStream(connection, PartitionMode.IMMUTABLE_KEY_RANGE, table, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, PartitionMode.IMMUTABLE_KEY_RANGE);
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("insert into " + table + "(id, value) values (1, 'some value')");
            connection.executeUpdate("update " + table + " set value = 'test' where id = 1");
            connection.executeUpdate("delete from " + table + " where id = 1");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(4);

            // Verify that mod types are create + update + delete + TOMBSTONE in order.
            assertThat((String) ((Struct) (records.get(0).value())).get("op")).isEqualTo("c");
            assertThat((String) ((Struct) (records.get(1).value())).get("op")).isEqualTo("u");
            assertThat((String) ((Struct) (records.get(2).value())).get("op")).isEqualTo("d");
            assertThat(records.get(3).value()).isEqualTo(null);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
