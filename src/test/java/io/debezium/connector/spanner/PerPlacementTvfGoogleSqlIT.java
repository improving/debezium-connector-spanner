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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

@RealSpannerCompatible
public class PerPlacementTvfGoogleSqlIT extends AbstractSpannerConnectorIT {

    private static final String EAST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.east.instance.partition", "east-partition");
    private static final String WEST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.west.instance.partition", "west-partition");
    private static final String EAST_PLACEMENT = System.getProperty(
            "spanner.test.east.placement", "PlacementMoveEast");
    private static final String WEST_PLACEMENT = System.getProperty(
            "spanner.test.west.placement", "PlacementMoveWest");

    @BeforeAll
    static void setupPlacements() throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Per-placement TVF tests require real Cloud Spanner. Run with -Dspanner.test.real=true.");
        databaseConnection.createPlacementIfMissing(EAST_PLACEMENT, EAST_INSTANCE_PARTITION);
        databaseConnection.createPlacementIfMissing(WEST_PLACEMENT, WEST_INSTANCE_PARTITION);
    }

    @Test
    public void shouldReadEastPlacementOnlyFromEastTvf() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String table = "placement_tvf_smoke_" + suffix;
        String stream = "PlacementTvfSmoke" + suffix;

        databaseConnection.createTable(table,
                "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, value STRING(MAX)) PRIMARY KEY (id)");
        boolean connectorStarted = false;
        try {
            databaseConnection.createPerPlacementTvfChangeStream(stream, table);
            List<String> tvfNames = databaseConnection.readPlacementTvfNames(stream);
            assertThat(tvfNames).hasSize(3);

            String eastTvf = tvfForPlacement(tvfNames, EAST_PLACEMENT);
            String westTvf = tvfForPlacement(tvfNames, WEST_PLACEMENT);
            assertThat(tvfNames).anyMatch(name -> name.toLowerCase(Locale.ROOT).endsWith("_placement_default"));
            Configuration config = Configuration.copy(
                    buildTestConfig(baseConfig, stream, table, PartitionMode.MUTABLE_KEY_RANGE))
                    .with("gcp.spanner.placement.tvf.names", String.join(",", tvfNames))
                    .with("tasks.max", 1)
                    .build();

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            connectorStarted = true;
            assertConnectorIsRunning();

            databaseConnection.executeUpdate("INSERT INTO " + table
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
            databaseConnection.dropChangeStream(stream);
            databaseConnection.dropTable(table);
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
