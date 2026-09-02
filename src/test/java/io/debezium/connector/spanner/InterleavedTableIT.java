/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed tables/change stream per invocation.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class InterleavedTableIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(InterleavedTableIT.class);

    private static final String parentTableNamePrefix = "embedded_interleaved_parent_table";
    private static final String childTableNamePrefix = "embedded_interleaved_child_table";
    private static final String changeStreamNamePrefix = "embeddedInterleavedChangeStream";

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldCaptureCascadingDeleteOfInterleavedChildRows(PartitionMode partitionMode, Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String parentTable = tableFor(parentTableNamePrefix, partitionMode, dialect);
        String childTable = tableFor(childTableNamePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNamePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(parentTable, tableParams);

        tableParams = "(id INT64, child_id INT64, value STRING(100)) PRIMARY KEY (id, child_id), "
                + "INTERLEAVE IN PARENT " + parentTable + " ON DELETE CASCADE";
        connection.createTable(childTable, tableParams);
        connection.createChangeStream(stream, partitionMode, parentTable, childTable);
        try {
            final Configuration config = buildTestConfig(base, stream, parentTable, partitionMode);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // One atomic transaction inserting the parent and its interleaved child.
            connection.executeUpdate(List.of(
                    "INSERT INTO " + parentTable + "(id, name) VALUES (1, 'Alice')",
                    "INSERT INTO " + childTable + "(id, child_id, value) VALUES (1, 100, 'Item1')"));

            // Only the parent is deleted explicitly - the child row is removed purely by
            // the ON DELETE CASCADE relationship, with no DML statement of its own.
            connection.executeUpdate(
                    "DELETE FROM " + parentTable + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

            List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTable));
            List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTable));
            // insert + delete + tombstone, for both parent and child.
            assertThat(parentRecords).hasSize(3);
            assertThat(childRecords).hasSize(3);

            Struct parentInsert = (Struct) parentRecords.get(0).value();
            assertThat(parentInsert.get("op")).isEqualTo("c");
            Struct childInsert = (Struct) childRecords.get(0).value();
            assertThat(childInsert.get("op")).isEqualTo("c");
            // The insert was one atomic transaction across parent and child.
            assertThat(childInsert.getStruct("source").getString("server_transaction_id"))
                    .isEqualTo(parentInsert.getStruct("source").getString("server_transaction_id"));

            Struct parentDelete = (Struct) parentRecords.get(1).value();
            assertThat(parentDelete.get("op")).isEqualTo("d");
            assertThat(parentDelete.getStruct("before").getString("name")).isEqualTo("Alice");

            // The cascaded child delete must show up even though no DML ever targeted
            // the child table directly, and it must be part of the same transaction as
            // the parent's explicit delete.
            Struct childDelete = (Struct) childRecords.get(1).value();
            assertThat(childDelete.get("op")).isEqualTo("d");
            assertThat(childDelete.getStruct("before").getString("value")).isEqualTo("Item1");
            assertThat(childDelete.getStruct("source").getString("server_transaction_id"))
                    .isEqualTo(parentDelete.getStruct("source").getString("server_transaction_id"));

            // Each key gets its own tombstone.
            assertThat(parentRecords.get(2).value()).isNull();
            assertThat(childRecords.get(2).value()).isNull();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(childTable);
            connection.dropTable(parentTable);
        }
    }
}
