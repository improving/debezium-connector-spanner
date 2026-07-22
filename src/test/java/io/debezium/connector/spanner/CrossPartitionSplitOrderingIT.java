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

/**
 * The emulator splits recursively on its own, roughly every 15-20 seconds,
 * purely on a fixed schedule rather than in response to load - each split doubling the
 * number of active leaf partitions. This is unlike real Spanner, where splitting is driven
 * by data size/load, but it gives us a live, constantly churning partition topology for
 * free, without needing to control or trigger it directly.
 *
 * <p>Rather than asserting on the split topology itself (partition tokens/counts), which is
 * an emulator implementation detail that could change, this asserts on the documented
 * guarantee it exists to protect: records returned from child partitions must be processed
 * only after records from all parent partitions have been processed. We wait long enough to
 * guarantee several splits have happened in the background, make a follow-up write to the
 * same row, and check it still arrives exactly once, with the correct content, strictly
 * ordered after the first write - i.e. the recursive splitting happening underneath does not
 * cause reordering, drops, or duplicate delivery.
 */
public class CrossPartitionSplitOrderingIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "cross_partition_split_ordering_table";
    private static final String changeStreamName = "crossPartitionSplitOrderingStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldDeliverFollowUpWriteExactlyOnceAndInOrderAcrossBackgroundPartitionSplits() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableName + "(id, value) VALUES (1, 'v1')");

        // Long enough to guarantee multiple recursive splits have already happened
        // underneath by the time the follow-up write below lands.
        Thread.sleep(TimeUnit.SECONDS.toMillis(45));

        databaseConnection.executeUpdate(
                "UPDATE " + tableName + " SET value = 'v2' WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));

        // Exactly insert + update - no duplicate delivery caused by the row's key range
        // having moved across several generations of child partitions in the background.
        assertThat(records).hasSize(2);

        Struct insertRecord = (Struct) records.get(0).value();
        assertThat(insertRecord.get("op")).isEqualTo("c");
        assertThat(insertRecord.getStruct("after").getString("value")).isEqualTo("v1");

        Struct updateRecord = (Struct) records.get(1).value();
        assertThat(updateRecord.get("op")).isEqualTo("u");
        assertThat(updateRecord.getStruct("before").getString("value")).isEqualTo("v1");
        assertThat(updateRecord.getStruct("after").getString("value")).isEqualTo("v2");

        // Strict commit-order: the update must be attributed a later commit timestamp than
        // the insert, even though it was very likely read back from a different (many
        // generations removed) child partition than the one the insert came from.
        long insertTimestamp = insertRecord.getStruct("source").getInt64("ts_ms");
        long updateTimestamp = updateRecord.getStruct("source").getInt64("ts_ms");
        assertThat(updateTimestamp).isGreaterThan(insertTimestamp);

        stopConnector();
        assertConnectorNotRunning();
    }
}
