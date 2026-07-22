/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class InterleavedTableIT extends AbstractSpannerConnectorIT {

    private static final String parentTableName = "embedded_interleaved_parent_table";
    private static final String childTableName = "embedded_interleaved_child_table";
    private static final String changeStreamName = "embeddedInterleavedChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(parentTableName + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createTable(childTableName
                + "(id INT64, child_id INT64, value STRING(100)) PRIMARY KEY (id, child_id), "
                + "INTERLEAVE IN PARENT " + parentTableName + " ON DELETE CASCADE");
        databaseConnection.createChangeStream(changeStreamName, parentTableName, childTableName);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(childTableName);
        databaseConnection.dropTable(parentTableName);
    }

    @Test
    public void shouldCaptureCascadingDeleteOfInterleavedChildRows() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", parentTableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        // One atomic transaction inserting the parent and its interleaved child.
        databaseConnection.executeUpdate(List.of(
                "INSERT INTO " + parentTableName + "(id, name) VALUES (1, 'Alice')",
                "INSERT INTO " + childTableName + "(id, child_id, value) VALUES (1, 100, 'Item1')"));

        // Only the parent is deleted explicitly - the child row is removed purely by
        // the ON DELETE CASCADE relationship, with no DML statement of its own.
        databaseConnection.executeUpdate(
                "DELETE FROM " + parentTableName + " WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

        List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTableName));
        List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTableName));
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
}
