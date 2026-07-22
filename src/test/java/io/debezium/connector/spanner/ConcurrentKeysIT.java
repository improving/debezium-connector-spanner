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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class ConcurrentKeysIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_concurrent_keys_table";
    private static final String changeStreamName = "embeddedConcurrentKeysStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName
                + "(id INT64, name STRING(100), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldNotCrossContaminateStateBetweenInterleavedKeys() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        // Inserts and updates for four different keys are deliberately interleaved,
        // rather than done one key at a time, to stress any per-row state cache that
        // might be keyed or indexed incorrectly.
        databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (1, 'Alice', 10)");
        databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (2, 'Bob', 20)");
        databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 100 WHERE id = 1");
        databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (3, 'Carol', 30)");
        databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 200 WHERE id = 2");
        databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 300 WHERE id = 3");
        databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (4, 'Dave', 40)");
        databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 400 WHERE id = 4");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(30, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(8);

        Map<Long, List<SourceRecord>> byKey = new LinkedHashMap<>();
        for (SourceRecord record : records) {
            long id = ((Struct) record.key()).getInt64("id");
            byKey.computeIfAbsent(id, k -> new ArrayList<>()).add(record);
        }
        assertThat(byKey.keySet()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);

        String[] names = { "Alice", "Bob", "Carol", "Dave" };
        long[] initialScores = { 10, 20, 30, 40 };
        long[] updatedScores = { 100, 200, 300, 400 };

        for (int i = 0; i < 4; i++) {
            long id = i + 1L;
            List<SourceRecord> keyRecords = byKey.get(id);
            assertThat(keyRecords).hasSize(2);

            Struct insert = (Struct) keyRecords.get(0).value();
            assertThat(insert.get("op")).isEqualTo("c");
            Struct insertAfter = insert.getStruct("after");
            assertThat(insertAfter.getString("name")).isEqualTo(names[i]);
            assertThat(insertAfter.getInt64("score")).isEqualTo(initialScores[i]);

            Struct update = (Struct) keyRecords.get(1).value();
            assertThat(update.get("op")).isEqualTo("u");
            // Each key's update must reflect exactly its own prior and new state -
            // not another key's name or score that happened to be processed nearby.
            Struct updateBefore = update.getStruct("before");
            assertThat(updateBefore.getString("name")).isEqualTo(names[i]);
            assertThat(updateBefore.getInt64("score")).isEqualTo(initialScores[i]);

            Struct updateAfter = update.getStruct("after");
            assertThat(updateAfter.getString("name")).isEqualTo(names[i]);
            assertThat(updateAfter.getInt64("score")).isEqualTo(updatedScores[i]);
        }

        stopConnector();
        assertConnectorNotRunning();
    }
}
