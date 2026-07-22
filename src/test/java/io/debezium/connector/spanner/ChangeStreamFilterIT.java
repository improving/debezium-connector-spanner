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

import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Statement;

import io.debezium.config.Configuration;

public class ChangeStreamFilterIT extends AbstractSpannerConnectorIT {

    private static final String tableNameExcludeDelete = "embedded_exclude_delete_table";
    private static final String changeStreamNameExcludeDelete = "embeddedExcludeDeleteStream";

    private static final String tableNameExcludeInsert = "embedded_exclude_insert_table";
    private static final String changeStreamNameExcludeInsert = "embeddedExcludeInsertStream";

    private static final String tableNameExcludeUpdate = "embedded_exclude_update_table";
    private static final String changeStreamNameExcludeUpdate = "embeddedExcludeUpdateStream";

    private static final String tableNameTxnExclusion = "embedded_txn_exclusion_table";
    private static final String changeStreamNameTxnExclusion = "embeddedTxnExclusionStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableNameExcludeDelete + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeDelete(changeStreamNameExcludeDelete, tableNameExcludeDelete);

        databaseConnection.createTable(tableNameExcludeInsert + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeInsert(changeStreamNameExcludeInsert, tableNameExcludeInsert);

        databaseConnection.createTable(tableNameExcludeUpdate + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeUpdate(changeStreamNameExcludeUpdate, tableNameExcludeUpdate);

        databaseConnection.createTable(tableNameTxnExclusion + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamAllowTxnExclusion(changeStreamNameTxnExclusion, tableNameTxnExclusion);
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamNameExcludeDelete);
        databaseConnection.dropTable(tableNameExcludeDelete);

        databaseConnection.dropChangeStream(changeStreamNameExcludeInsert);
        databaseConnection.dropTable(tableNameExcludeInsert);

        databaseConnection.dropChangeStream(changeStreamNameExcludeUpdate);
        databaseConnection.dropTable(tableNameExcludeUpdate);

        databaseConnection.dropChangeStream(changeStreamNameTxnExclusion);
        databaseConnection.dropTable(tableNameTxnExclusion);
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
    public void shouldExcludeDeleteEventsAndTheirTombstones() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameExcludeDelete)
                .with("name", tableNameExcludeDelete + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeDelete + "(id, name) VALUES (1, 'Alice')");
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameExcludeDelete + " SET name = 'Bob' WHERE id = 1");
        databaseConnection.executeUpdate(
                "DELETE FROM " + tableNameExcludeDelete + " WHERE id = 1");

        // A second row, inserted and never touched again, gives us a clear signal
        // that the connector is still alive and delivering records after the
        // excluded delete - not just coincidentally quiet.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeDelete + "(id, name) VALUES (2, 'Carol')");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeDelete));

        // insert(1) + update(1) + insert(2) - no delete, and therefore no tombstone.
        assertThat(records).hasSize(3);

        assertThat(((Struct) records.get(0).value()).get("op")).isEqualTo("c");
        assertThat(((Struct) records.get(1).value()).get("op")).isEqualTo("u");

        Struct secondInsert = (Struct) records.get(2).value();
        assertThat(secondInsert.get("op")).isEqualTo("c");
        assertThat(secondInsert.getStruct("after").getString("name")).isEqualTo("Carol");
    }

    @Test
    public void shouldReflectRealPriorStateOnUpdateAfterAnExcludedInsert() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameExcludeInsert)
                .with("name", tableNameExcludeInsert + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        // This insert is invisible to the stream, but the row genuinely exists
        // afterward with these values.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeInsert + "(id, name) VALUES (1, 'Alice')");
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameExcludeInsert + " SET name = 'Bob' WHERE id = 1");
        databaseConnection.executeUpdate(
                "DELETE FROM " + tableNameExcludeInsert + " WHERE id = 1");

        // A row that's only ever inserted, never revisited - it must produce zero
        // records at all, not just a suppressed-but-otherwise-present one.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeInsert + "(id, name) VALUES (2, 'Carol')");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeInsert));

        // update(1) + delete(1) + tombstone(1) - no insert for either row.
        assertThat(records).hasSize(3);

        Struct updateRecord = (Struct) records.get(0).value();
        assertThat(updateRecord.get("op")).isEqualTo("u");
        // The row's real prior value was 'Alice', even though the stream never
        // reported the insert that created it.
        assertThat(updateRecord.getStruct("before").getString("name")).isEqualTo("Alice");
        assertThat(updateRecord.getStruct("after").getString("name")).isEqualTo("Bob");

        Struct deleteRecord = (Struct) records.get(1).value();
        assertThat(deleteRecord.get("op")).isEqualTo("d");
        assertThat(deleteRecord.getStruct("before").getString("name")).isEqualTo("Bob");

        // Tombstone for row 1's delete.
        assertThat(records.get(2).value()).isNull();
    }

    @Test
    public void shouldExcludeUpdateEventsButReflectRealStateOnSubsequentDelete() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameExcludeUpdate)
                .with("name", tableNameExcludeUpdate + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeUpdate + "(id, name) VALUES (1, 'Alice')");
        // This update really executes against the row - it's excluded from the
        // change stream, not from the database itself.
        databaseConnection.executeUpdate(
                "UPDATE " + tableNameExcludeUpdate + " SET name = 'Bob' WHERE id = 1");
        databaseConnection.executeUpdate(
                "DELETE FROM " + tableNameExcludeUpdate + " WHERE id = 1");

        // A second row, inserted and never touched again, gives us a clear signal
        // that the connector is still alive and delivering records after the
        // excluded update - not just coincidentally quiet.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameExcludeUpdate + "(id, name) VALUES (2, 'Carol')");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeUpdate));

        // insert(1) + delete(1) + tombstone(1) + insert(2) - no update event at all.
        assertThat(records).hasSize(4);

        Struct insertRecord = (Struct) records.get(0).value();
        assertThat(insertRecord.get("op")).isEqualTo("c");
        assertThat(insertRecord.getStruct("after").getString("name")).isEqualTo("Alice");

        Struct deleteRecord = (Struct) records.get(1).value();
        assertThat(deleteRecord.get("op")).isEqualTo("d");
        // The row's real data did change to 'Bob' before being deleted - the stream
        // just never reported the update itself.
        assertThat(deleteRecord.getStruct("before").getString("name")).isEqualTo("Bob");

        // Tombstone for row 1's delete.
        assertThat(records.get(2).value()).isNull();

        Struct secondInsert = (Struct) records.get(3).value();
        assertThat(secondInsert.get("op")).isEqualTo("c");
        assertThat(secondInsert.getStruct("after").getString("name")).isEqualTo("Carol");
    }

    @Test
    public void shouldNotRecordTransactionExplicitlyExcludedFromChangeStreams() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamNameTxnExclusion)
                .with("name", tableNameTxnExclusion + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate(
                "INSERT INTO " + tableNameTxnExclusion + "(id, name) VALUES (1, 'Alice')");

        // This transaction really executes against the row - it's excluded from the
        // change stream, not from the database itself.
        databaseConnection.databaseClient.readWriteTransaction(Options.excludeTxnFromChangeStreams())
                .run(transaction -> transaction.executeUpdate(
                        Statement.of("UPDATE " + tableNameTxnExclusion + " SET name = 'Excluded' WHERE id = 1")));

        databaseConnection.executeUpdate(
                "UPDATE " + tableNameTxnExclusion + " SET name = 'Bob' WHERE id = 1");

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameTxnExclusion));

        // insert + the final visible update - the excluded transaction produces
        // no record of its own at all.
        assertThat(records).hasSize(2);

        Struct insertRecord = (Struct) records.get(0).value();
        assertThat(insertRecord.get("op")).isEqualTo("c");
        assertThat(insertRecord.getStruct("after").getString("name")).isEqualTo("Alice");

        Struct visibleUpdate = (Struct) records.get(1).value();
        assertThat(visibleUpdate.get("op")).isEqualTo("u");
        // The row's real data did change to 'Excluded' - the stream just never
        // reported it, so this visible update's "before" reflects that real prior
        // state, not the last value the stream actually showed.
        assertThat(visibleUpdate.getStruct("before").getString("name")).isEqualTo("Excluded");
        assertThat(visibleUpdate.getStruct("after").getString("name")).isEqualTo("Bob");
    }
}
