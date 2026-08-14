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

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * The emulator splits recursively on its own, roughly every 15-20 seconds,
 * purely on a fixed schedule rather than in response to load - each split doubling the
 * number of active leaf partitions. This is unlike real Spanner, where splitting is driven
 * by data size/load, but it gives us a live, constantly churning partition topology for
 * free, without needing to control or trigger it directly.
 *
 * <p>Rather than asserting on the split topology itself (partition tokens/counts), this
 * asserts on the documented guarantee it exists to protect: records returned from child
 * partitions must be processed only after records from all parent partitions have been
 * processed. We wait long enough to guarantee several splits have happened in the
 * background, make a follow-up write to the same row, and check it still arrives exactly
 * once, with the correct content, strictly ordered after the first write - i.e. the
 * recursive splitting happening underneath does not cause reordering, drops, or duplicate
 * delivery.
 *
 * <p>A new child partition's dispatch can fall behind the change stream's live low-watermark
 * (which advances with wall-clock progress, not a retention window) by more than its ~15-20
 * second margin, in which case Spanner rejects the query with {@code OUT_OF_RANGE: Specified
 * start_timestamp is too far in the past}. This isn't specific to either partition mode,
 * but whichever mode runs second in a shared JVM is more exposed to it, for reasons not fully
 * diagnosed (no specific shared JVM state has been identified as the cause). That's why this
 * lives as two top-level test classes ({@link CrossPartitionSplitOrderingImmutableKeyRangeIT}
 * and {@link CrossPartitionSplitOrderingMutableKeyRangeIT}), each with its own {@code @Test}
 * method calling {@link #runTest}, run through the {@code integration-test-isolated-jvm}
 * Failsafe execution in {@code pom.xml} that gives each class its own {@code reuseForks=false}
 * JVM fork. {@code KafkaEnvironment} assigns each fork its own Kafka broker port
 * ({@code KafkaEnvironment.perForkPort}) so the two forks' Docker containers don't collide.
 */
public abstract class CrossPartitionSplitOrderingTestBase extends AbstractSpannerConnectorIT {

    private static final String tableNamePrefix = "cross_partition_split_ordering_table";
    private static final String changeStreamNamePrefix = "crossPartitionSplitOrderingStream";

    protected void runTest(PartitionMode partitionMode)
            throws InterruptedException, ExecutionException {
        String tableName = tableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNamePrefix + partitionMode.name();

        // A fresh emulator instance and database per invocation: the two invocations run
        // in separate, often concurrent, JVM forks (see pom.xml), so sharing the default
        // instance/database would risk concurrent DDL/instance-creation conflicts between them.
        Database testDatabase = Database.builder().generateInstanceId().generateDatabaseId().build();
        Connection connection = testDatabase.getConnection();

        connection.createTable(tableName + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        connection.createChangeStream(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(createBaseConfigBuilder(testDatabase, false).build(),
                    changeStreamName, tableName, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, value) VALUES (1, 'v1')");

            // Long enough to guarantee multiple recursive splits have already happened
            // underneath by the time the follow-up write below lands.
            Thread.sleep(TimeUnit.SECONDS.toMillis(45));

            connection.executeUpdate(
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
        finally {
            stopConnector();
            connection.dropDatabase(testDatabase.getDatabaseId());
            connection.dropInstance(testDatabase.getInstanceId());
            connection.close();
        }
    }
}
