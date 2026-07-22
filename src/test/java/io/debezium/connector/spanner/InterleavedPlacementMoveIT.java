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
 * Interleaved-in-placement scenario: a child table has no placement key of its own - it
 * is always physically co-located with its parent row, so it must move with the parent
 * whenever the parent's placement key changes.
 *
 * <p>Blocked because the public Docker Spanner emulator's DDL parser rejects the
 * {@code partition_mode} option outright, so a {@code MUTABLE_KEY_RANGE} stream can't be
 * created against it at all; Spanner Omni does accept {@code MUTABLE_KEY_RANGE} streams but
 * explicitly rejects geo-partitioning as a deployment/edition restriction
 * ({@code CreateInstancePartition} and {@code CREATE PLACEMENT} both return
 * {@code UNIMPLEMENTED}, confirmed directly); and {@code Connection} has no
 * {@code createPlacement(...)} helper yet.
 *
 * <p><b>Open design question this test exists to answer</b> - not yet resolved by
 * reading the Spanner docs alone: does the child table produce its own correlated
 * signal when the parent moves (even though no DML touched the child), the way
 * {@link InterleavedTableIT} proved a cascading {@code DELETE} does? Or does the
 * child's data move silently, observable only indirectly via subsequent child reads
 * being correctly ordered relative to the parent's move? The Spanner docs describe
 * {@code PartitionEventRecord} at the key-range level, not in per-table terms, so this
 * needs resolving during design review once {@code MUTABLE_KEY_RANGE} semantics are
 * understood in more depth against a real project.
 *
 * <p>The connector's current implementation is suggestive but not conclusive: its
 * {@code PartitionEventEvent} dispatch operates purely at the key-range/partition level
 * (internal move-out and processed-timestamp bookkeeping via {@code PartitionManager}),
 * with no new per-table Kafka record or {@code SourceInfo} field introduced. That's
 * consistent with "the child moves silently," but isn't the same as confirming it against
 * a real move - still needs settling once this scenario can actually run.
 */
@Disabled("Can't be validated against either available test backend: the local Docker emulator's DDL "
        + "parser rejects the partition_mode option outright, and Spanner Omni supports MUTABLE_KEY_RANGE "
        + "but not geo-partitioning/CREATE PLACEMENT. Disabled until a real, multi-region Spanner instance "
        + "is available to test against")
public class InterleavedPlacementMoveIT extends AbstractSpannerConnectorIT {

    private static final String parentTableName = "placement_parent_move_table";
    private static final String childTableName = "placement_child_move_table";
    private static final String changeStreamName = "placementParentChildMoveStream";

    @BeforeAll
    static void setup() throws Exception {
        // TODO(blocked): Connection has no createPlacement(...) helper yet
        // databaseConnection.createPlacement("PlacementEast", "east-partition");
        // databaseConnection.createPlacement("PlacementWest", "west-partition");

        databaseConnection.createTable(parentTableName
                + "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, name STRING(MAX)) "
                + "PRIMARY KEY (id)");
        // Interleaved children of a placement table have no placement key of their own -
        // they are physically co-located with the parent row and move with it.
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
    public void shouldMoveInterleavedChildRowsWithParentPlacementChange() throws Exception {
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

        // Moving the parent's placement must move the child's physical storage too,
        // since interleaved rows are always co-located with their parent.
        databaseConnection.executeUpdate(
                "UPDATE " + parentTableName + " SET region = 'PlacementWest' WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

        List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTableName));
        Struct parentMoveRecord = (Struct) parentRecords.get(1).value(); // after the insert
        assertThat(parentMoveRecord.getStruct("after").getString("region")).isEqualTo("PlacementWest");

        // See the class-level javadoc for the open design question this test exists to
        // answer regarding whether the child produces its own signal on a parent move.

        // Regardless of which answer that resolves to, a subsequent child mutation must
        // be correctly ordered after the parent's move.
        databaseConnection.executeUpdate(
                "UPDATE " + childTableName + " SET value = 'Item1-updated' WHERE id = 1 AND child_id = 100");

        List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTableName));
        Struct childUpdateRecord = (Struct) childRecords.get(childRecords.size() - 1).value();
        long parentMoveTimestamp = parentMoveRecord.getStruct("source").getInt64("ts_ms");
        long childUpdateTimestamp = childUpdateRecord.getStruct("source").getInt64("ts_ms");
        assertThat(childUpdateTimestamp).isGreaterThan(parentMoveTimestamp);

        stopConnector();
        assertConnectorNotRunning();
    }
}
