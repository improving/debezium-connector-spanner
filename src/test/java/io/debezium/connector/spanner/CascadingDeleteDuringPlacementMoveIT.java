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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * The most complex of the three placement scenarios: combines {@link InterleavedTableIT}'s
 * proven cascade-delete mechanism with an in-flight placement move, to check whether the
 * connector's partition-move bookkeeping and its cascade-delete handling agree on which
 * partition owns the key at the moment of deletion.
 */
@Disabled("Can't be validated against either available test backend: the local Docker emulator's DDL "
        + "parser rejects the partition_mode option outright, and Spanner Omni supports MUTABLE_KEY_RANGE "
        + "but not geo-partitioning/CREATE PLACEMENT. Also needs a createPlacement() helper added to the "
        + "test Connection utility. Disabled until a real, multi-region Spanner instance is available to "
        + "test against")
public class CascadingDeleteDuringPlacementMoveIT extends AbstractSpannerConnectorIT {

    private static final String parentTableName = "placement_parent_cascade_table";
    private static final String childTableName = "placement_child_cascade_table";
    private static final String changeStreamName = "placementCascadeDuringMoveStream";

    @BeforeAll
    static void setup() throws Exception {
        // TODO(blocked): Connection has no createPlacement(...) helper yet
        // databaseConnection.createPlacement("PlacementEast", "east-partition");
        // databaseConnection.createPlacement("PlacementWest", "west-partition");

        databaseConnection.createTable(parentTableName
                + "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, name STRING(MAX)) "
                + "PRIMARY KEY (id)");
        databaseConnection.createTable(childTableName
                + "(id INT64 NOT NULL, child_id INT64 NOT NULL, value STRING(MAX)) "
                + "PRIMARY KEY (id, child_id), INTERLEAVE IN PARENT " + parentTableName + " ON DELETE CASCADE");

        databaseConnection.createChangeStream(changeStreamName, PartitionMode.MUTABLE_KEY_RANGE,
                parentTableName, childTableName);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(childTableName);
        databaseConnection.dropTable(parentTableName);
    }

    @Test
    public void shouldOrderCascadingDeleteCorrectlyRelativeToInFlightPlacementMove() throws Exception {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", parentTableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(List.of(
                "INSERT INTO " + parentTableName + "(id, region, name) VALUES (1, 'PlacementEast', 'Alice')",
                "INSERT INTO " + childTableName + "(id, child_id, value) VALUES (1, 100, 'Item1')"));

        databaseConnection.executeUpdate(
                "UPDATE " + parentTableName + " SET region = 'PlacementWest' WHERE id = 1");

        // Deleting the parent immediately after the move cascades to the child, exactly
        // as InterleavedTableIT proved for a non-placement parent - but now the delete
        // has to be correctly attributed to whichever partition the row had just moved
        // to. If the connector's partition-move bookkeeping and its cascade-delete
        // handling don't agree on which partition currently owns this key, this is
        // exactly the kind of place a duplicate or dropped delete could hide.
        databaseConnection.executeUpdate(
                "DELETE FROM " + parentTableName + " WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

        List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTableName));
        List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTableName));
        // insert + move + delete + tombstone, for both parent and child.
        assertThat(parentRecords).hasSize(4);
        assertThat(childRecords).hasSize(3); // insert + cascaded delete + tombstone (no move-triggered record on the child)

        Struct parentDelete = (Struct) parentRecords.get(2).value();
        Struct childDelete = (Struct) childRecords.get(1).value();
        assertThat(parentDelete.get("op")).isEqualTo("d");
        assertThat(childDelete.get("op")).isEqualTo("d");

        // Same correlation check InterleavedTableIT already established for the
        // non-placement case - both deletes must share one transaction identity, even
        // though the parent had just moved placements.
        assertThat(childDelete.getStruct("source").getString("server_transaction_id"))
                .isEqualTo(parentDelete.getStruct("source").getString("server_transaction_id"));

        // Exactly one delete + one tombstone per key - no duplicate delivery caused by
        // the move and the cascade both trying to "explain" the same row disappearing.
        assertThat(parentRecords.get(3).value()).isNull(); // tombstone
        assertThat(childRecords.get(2).value()).isNull(); // tombstone

        stopConnector();
        assertConnectorNotRunning();
    }
}
