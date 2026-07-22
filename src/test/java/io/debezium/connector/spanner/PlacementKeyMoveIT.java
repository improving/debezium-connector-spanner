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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Core placement-table scenario: a row physically moves between placements, and a
 * follow-up write to the same row must not be delivered out of order relative to that
 * move. This is the property that actually depends on correct
 * {@code PartitionEventRecord} move-in/move-out handling.
 *
 * <p>Expects two pre-provisioned instance partitions, {@code east-partition} and
 * {@code west-partition}, to already exist - see {@code doc/real-spanner-testing.md} for
 * provisioning steps. {@code Connection.createPlacement(...)} maps a {@code PLACEMENT} onto
 * an existing instance partition by name; it does not provision the instance partition
 * itself.
 */
@EnabledIf("hasNonEmulatorBackend")
public class PlacementKeyMoveIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "placement_key_move_table";
    private static final String changeStreamName = "placementKeyMoveStream";
    private static final String placementEast = "PlacementKeyMoveEast";
    private static final String placementWest = "PlacementKeyMoveWest";

    @BeforeAll
    static void setup() throws Exception {
        databaseConnection.createPlacement(placementEast, "east-partition");
        databaseConnection.createPlacement(placementWest, "west-partition");

        databaseConnection.createTable(tableName
                + "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, value STRING(MAX)) "
                + "PRIMARY KEY (id)");

        // This overload already exists (Connection.createChangeStream(String, PartitionMode, String...));
        // Connection.createMutableKeyRangeChangeStream(String, String...) is an equivalent,
        // MUTABLE_KEY_RANGE-only alternative. Either way, the emulator rejects this DDL
        // outright.
        databaseConnection.createChangeStream(changeStreamName, PartitionMode.MUTABLE_KEY_RANGE, tableName);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
        databaseConnection.dropPlacement(placementEast);
        databaseConnection.dropPlacement(placementWest);
    }

    @Test
    public void shouldOrderRecordsCorrectlyWhenRowMovesBetweenPlacements() throws Exception {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        // Row starts in the "east" placement.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableName + "(id, region, value) VALUES (1, '" + placementEast + "', 'v1')");

        // Changing the placement key physically moves the row's data to the other
        // instance partition. This is the operation that triggers PartitionEventRecord
        // move-out (on the source partition) and move-in (on the destination partition).
        databaseConnection.executeUpdate(
                "UPDATE " + tableName + " SET region = '" + placementWest + "' WHERE id = 1");

        // A follow-up write to the SAME row, issued immediately after the move, so we
        // can check it isn't processed out of order relative to the move itself.
        databaseConnection.executeUpdate(
                "UPDATE " + tableName + " SET value = 'v2' WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(3); // insert + move + follow-up

        Struct moveRecord = (Struct) records.get(1).value();
        assertThat(moveRecord.get("op")).isEqualTo("u");
        assertThat(moveRecord.getStruct("before").getString("region")).isEqualTo(placementEast);
        assertThat(moveRecord.getStruct("after").getString("region")).isEqualTo(placementWest);

        // The follow-up write to the new placement must be strictly ordered after the
        // move, with a later commit timestamp - if the connector read the destination
        // partition before the source partition had caught up to the move's
        // commit_timestamp, this write could be delivered out of order or duplicated.
        Struct followUpRecord = (Struct) records.get(2).value();
        assertThat(followUpRecord.getStruct("after").getString("value")).isEqualTo("v2");
        long moveTimestamp = moveRecord.getStruct("source").getInt64("ts_ms");
        long followUpTimestamp = followUpRecord.getStruct("source").getInt64("ts_ms");
        assertThat(followUpTimestamp).isGreaterThan(moveTimestamp);

        // TODO(blocked): PartitionEventEvent now has a real dispatch case in
        // SpannerStreamingChangeEventSource (it drives internal move-out/processed-timestamp
        // bookkeeping via PartitionManager), so it's no longer silently dropped at the
        // dispatch loop. It still isn't surfaced as an inspectable Kafka record or SourceInfo
        // field, though, so asserting directly on move_in/move_out event content (source and
        // destination partition tokens) still isn't possible through the normal SourceRecord
        // API - only its downstream ordering effect, as asserted above, is currently
        // test-observable.

        stopConnector();
        assertConnectorNotRunning();
    }
}
