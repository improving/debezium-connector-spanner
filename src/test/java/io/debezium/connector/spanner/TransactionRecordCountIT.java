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

public class TransactionRecordCountIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_txn_record_count_table";
    private static final String changeStreamName = "embeddedTxnRecordCountStream";

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
    public void shouldReportRecordAndPartitionCountsForTransaction() throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        // Seed row 1 in its own single-statement transaction.
        databaseConnection.executeUpdate(
                "INSERT INTO " + tableName + "(id, value) VALUES (1, 'A-initial')");

        // One atomic transaction touching two rows - the transaction-wide record count
        // must reflect both changes, not just what one row's own record shows in
        // isolation.
        databaseConnection.executeUpdate(List.of(
                "UPDATE " + tableName + " SET value = 'A-updated' WHERE id = 1",
                "INSERT INTO " + tableName + "(id, value) VALUES (2, 'B-initial')"));

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(3);

        Struct seedInsertSource = ((Struct) records.get(0).value()).getStruct("source");
        // A single-statement transaction touching one row: the transaction only ever
        // produced one record, entirely within one partition for this tiny table.
        assertThat(seedInsertSource.getInt64("number_records_in_transaction")).isEqualTo(1);
        assertThat(seedInsertSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);

        Struct sharedUpdateSource = ((Struct) records.get(1).value()).getStruct("source");
        Struct sharedInsertSource = ((Struct) records.get(2).value()).getStruct("source");

        // Both records came from the same two-statement transaction: each one must
        // report the transaction-wide total (2), not a count scoped to itself or to
        // whichever partition happened to carry it.
        assertThat(sharedUpdateSource.getInt64("number_records_in_transaction")).isEqualTo(2);
        assertThat(sharedInsertSource.getInt64("number_records_in_transaction")).isEqualTo(2);

        // Both changes land in the same (only) partition for a table this small.
        assertThat(sharedUpdateSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);
        assertThat(sharedInsertSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);

        stopConnector();
        assertConnectorNotRunning();
    }
}
