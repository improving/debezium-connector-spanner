/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assumptions;
import org.slf4j.Logger;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

public class PerPlacementTvfTestBase extends AbstractSpannerConnectorIT {

    private static final String EAST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.east.instance.partition", "east-partition");
    private static final String WEST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.west.instance.partition", "west-partition");
    private static final String EAST_PLACEMENT = System.getProperty(
            "spanner.test.east.placement", "PlacementMoveEast");
    private static final String WEST_PLACEMENT = System.getProperty(
            "spanner.test.west.placement", "PlacementMoveWest");

    public void shouldReadEastPlacementOnlyFromEastTvf(Dialect dialect, Logger logger) throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Per-placement TVF tests require real Cloud Spanner. Run with -Dspanner.test.real=true.");

        Connection connection = connectionFor(dialect, logger);
        Configuration base = baseConfigFor(dialect);
        connection.createPlacementIfMissing(EAST_PLACEMENT, EAST_INSTANCE_PARTITION);
        connection.createPlacementIfMissing(WEST_PLACEMENT, WEST_INSTANCE_PARTITION);

        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String table = tableFor("placement_tvf_smoke_" + suffix, null, dialect);
        String stream = "pt" + suffix.substring(Math.max(0, suffix.length() - 8));

        connection.createTable(table,
                "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, value STRING(MAX)) PRIMARY KEY (id)");
        boolean connectorStarted = false;
        try {
            connection.createPerPlacementTvfChangeStream(stream, table);
            List<String> tvfNames = connection.readPlacementTvfNames(stream);
            assertThat(tvfNames).hasSize(3);

            String eastTvf = tvfForPlacement(tvfNames, EAST_PLACEMENT);
            String westTvf = tvfForPlacement(tvfNames, WEST_PLACEMENT);
            assertThat(tvfNames).anyMatch(name -> name.toLowerCase(Locale.ROOT).endsWith("_placement_default"));
            Configuration config = Configuration.copy(
                    buildTestConfig(base, stream, table, PartitionMode.MUTABLE_KEY_RANGE))
                    .with("gcp.spanner.placement.tvf.names", String.join(",", tvfNames))
                    .with("tasks.max", 1)
                    .build();

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            connectorStarted = true;
            assertConnectorIsRunning();

            connection.executeUpdate("INSERT INTO " + table
                    + "(id, region, value) VALUES (1, '" + EAST_PLACEMENT + "', 'east-value')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            assertThat(records).hasSize(1);
            SourceRecord record = records.get(0);
            Struct after = ((Struct) record.value()).getStruct("after");
            assertThat(after.getInt64("id")).isEqualTo(1L);
            assertThat(after.getString("region")).isEqualTo(EAST_PLACEMENT);
            assertThat(after.getString("value")).isEqualTo("east-value");
            assertThat(SpannerPartition.extractTvfName(record.sourcePartition())).isEqualTo(eastTvf);
            assertThat(records).noneMatch(
                    r -> westTvf.equals(SpannerPartition.extractTvfName(r.sourcePartition())));

            stopConnector();
            connectorStarted = false;
            assertConnectorNotRunning();
        }
        finally {
            if (connectorStarted) {
                stopConnector();
            }
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    private static String tvfForPlacement(List<String> tvfNames, String placement) {
        return tvfNames.stream()
                .filter(name -> name.toLowerCase(Locale.ROOT)
                        .endsWith("_" + placement.toLowerCase(Locale.ROOT)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No generated TVF for placement " + placement + ": " + tvfNames));
    }
}
