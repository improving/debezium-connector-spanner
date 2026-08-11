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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * There are exactly four sources a change stream watches:
 * DML statements, mutations, cascading deletes on interleaved child tables (covered by
 * {@link InterleavedTableIT}), and deletes resulting from TTL rules - not yet covered by
 * anything. This would also be the only way to exercise the positive case of
 * {@code system_transaction: true} on the emitted record's source block, which nothing
 * currently tests (every other scenario is a normal user-issued DML statement, so it's
 * always {@code false}).
 *
 * <p>Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed table/change stream per invocation.
 */
@Disabled("Blocked on TTL eviction never running in the Spanner emulator within any "
        + "test-practical window")
public class TtlDeleteEventIT extends AbstractSpannerConnectorIT {

    private static final String tableNamePrefix = "ttl_delete_event_table";
    private static final String changeStreamNamePrefix = "ttlDeleteEventStream";

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldEmitDeleteAsSystemTransactionWhenRowExpiresViaTtl(PartitionMode partitionMode) throws Exception {
        String tableName = tableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNamePrefix + partitionMode.name();
        databaseConnection.createTable(tableName
                + "(id INT64, value STRING(100), expire_at TIMESTAMP NOT NULL) PRIMARY KEY (id), "
                + "ROW DELETION POLICY (OLDER_THAN(expire_at, INTERVAL 1 DAY))");
        databaseConnection.createChangeStream(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = Configuration.copy(baseConfig)
                    .with("gcp.spanner.change.stream", changeStreamName)
                    .with("name", tableName + "_test")
                    .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                    .build();

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // expire_at is already two days in the past, so the row is immediately eligible
            // for TTL garbage collection - on real Spanner, GC runs on its own schedule
            // (typically within ~72 hours), not on insert.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, value, expire_at) VALUES ("
                            + "1, 'expires-soon', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(3); // insert + TTL-triggered delete + tombstone

            Struct ttlDelete = (Struct) records.get(1).value();
            assertThat(ttlDelete.get("op")).isEqualTo("d");

            // The one assertion this whole test exists for: a TTL-triggered delete is the only
            // documented source of a positive system_transaction, distinguishing it from every
            // other (user-issued) DML statement in this test suite.
            assertThat(ttlDelete.getStruct("source").getBoolean("system_transaction")).isTrue();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            databaseConnection.dropChangeStream(changeStreamName);
            databaseConnection.dropTable(tableName);
        }
    }
}
