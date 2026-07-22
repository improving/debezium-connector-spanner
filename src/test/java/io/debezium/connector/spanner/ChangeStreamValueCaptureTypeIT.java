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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class ChangeStreamValueCaptureTypeIT extends AbstractSpannerConnectorIT {

    private static final String tableNameNewValues = "embedded_new_values_capture_table";
    private static final String changeStreamNameNewValues = "embeddedNewValuesCaptureStream";

    private static final String tableNameNewRow = "embedded_new_row_capture_table";
    private static final String changeStreamNameNewRow = "embeddedNewRowCaptureStream";

    private static final String tableNameNewRowAndOldValues = "embedded_new_row_old_values_capture_table";
    private static final String changeStreamNameNewRowAndOldValues = "embeddedNewRowAndOldValuesCaptureStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableNameNewValues
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewValue(changeStreamNameNewValues, tableNameNewValues);

        databaseConnection.createTable(tableNameNewRow
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewRow(changeStreamNameNewRow, tableNameNewRow);

        databaseConnection.createTable(tableNameNewRowAndOldValues
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewRowAndOldValues(changeStreamNameNewRowAndOldValues, tableNameNewRowAndOldValues);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamNameNewValues);
        databaseConnection.dropTable(tableNameNewValues);

        databaseConnection.dropChangeStream(changeStreamNameNewRow);
        databaseConnection.dropTable(tableNameNewRow);

        databaseConnection.dropChangeStream(changeStreamNameNewRowAndOldValues);
        databaseConnection.dropTable(tableNameNewRowAndOldValues);
    }

    @BeforeEach
    void initFramework() {
        clearKafkaTopics();
        initializeConnectorTestFramework();
    }

    @AfterEach
    void ensureConnectorStopped() throws InterruptedException {
        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldCaptureFullNewRowWithNoNonKeyOldValues() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameNewValues)
                .with("name", tableNameNewValues + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameNewValues + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
        // Only 'score' is touched here — 'name' and 'status' are left alone.
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameNewValues + " SET score = 20 WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameNewValues));
        assertThat(records).hasSize(2);

        Struct updateRecord = (Struct) records.get(1).value();
        assertThat(updateRecord.get("op")).isEqualTo("u");

        // NEW_VALUES captures no non-key old values; the primary key still identifies
        // the row, but none of the other columns' prior values are included.
        Struct before = updateRecord.getStruct("before");
        assertThat(before).isNotNull();
        assertThat(before.getInt64("id")).isEqualTo(1L);
        assertThat(before.getString("name")).isNull();
        assertThat(before.getString("status")).isNull();
        assertThat(before.getInt64("score")).isNull();

        // The connector returns the full non-key row in
        // "after" regardless of which columns actually changed - matching the same
        // full-row behavior observed for OLD_AND_NEW_VALUES.
        Struct after = updateRecord.getStruct("after");
        assertThat(after.getInt64("score")).isEqualTo(20);
        assertThat(after.getString("name")).isEqualTo("Alice");
        assertThat(after.getString("status")).isEqualTo("active");
    }

    @Test
    public void shouldCaptureFullNewRowWithNoOldValues() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameNewRow)
                .with("name", tableNameNewRow + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameNewRow + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
        // Only 'score' is touched here — 'name' and 'status' are left alone.
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameNewRow + " SET score = 20 WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameNewRow));
        assertThat(records).hasSize(2);

        Struct updateRecord = (Struct) records.get(1).value();
        assertThat(updateRecord.get("op")).isEqualTo("u");

        // NEW_ROW captures no old values.
        Struct before = updateRecord.getStruct("before");
        assertThat(before).isNotNull();
        assertThat(before.getInt64("id")).isEqualTo(1L);
        assertThat(before.getString("name")).isNull();
        assertThat(before.getString("status")).isNull();
        assertThat(before.getInt64("score")).isNull();

        // NEW_ROW captures the full row - both modified and unmodified columns.
        Struct after = updateRecord.getStruct("after");
        assertThat(after.getInt64("score")).isEqualTo(20);
        assertThat(after.getString("name")).isEqualTo("Alice");
        assertThat(after.getString("status")).isEqualTo("active");
    }

    @Test
    public void shouldCaptureFullRowOnBothSides() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameNewRowAndOldValues)
                .with("name", tableNameNewRowAndOldValues + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameNewRowAndOldValues + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
        // Only 'score' is touched here — 'name' and 'status' are left alone.
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameNewRowAndOldValues + " SET score = 20 WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameNewRowAndOldValues));
        assertThat(records).hasSize(2);

        Struct updateRecord = (Struct) records.get(1).value();
        assertThat(updateRecord.get("op")).isEqualTo("u");

        Struct before = updateRecord.getStruct("before");
        assertThat(before.getInt64("score")).isEqualTo(10);
        assertThat(before.getString("name")).isEqualTo("Alice");
        assertThat(before.getString("status")).isEqualTo("active");

        Struct after = updateRecord.getStruct("after");
        assertThat(after.getInt64("score")).isEqualTo(20);
        assertThat(after.getString("name")).isEqualTo("Alice");
        assertThat(after.getString("status")).isEqualTo("active");
    }
}
