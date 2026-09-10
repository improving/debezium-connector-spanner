/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

class LowWatermarkCalculatorTest {

    @Test
    void returnsEarliestStartTimestampAcrossPlacementInitialPartitions() {
        Timestamp earliest = Timestamp.ofTimeMicroseconds(100L);
        PartitionState east = initialPartition("tvfEast", Timestamp.ofTimeMicroseconds(200L));
        PartitionState west = initialPartition("tvfWest", earliest);
        TaskState taskState = TaskState.builder()
                .taskUid("task0")
                .partitions(List.of(east, west))
                .sharedPartitions(List.of())
                .build();
        TaskSyncContext context = mock(TaskSyncContext.class);
        when(context.isInitialized()).thenReturn(true);
        when(context.getAllTaskStates()).thenReturn(Map.of("task0", taskState));
        when(context.getTaskUid()).thenReturn("task0");
        TaskSyncContextHolder contextHolder = mock(TaskSyncContextHolder.class);
        when(contextHolder.get()).thenReturn(context);
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.getHeartbeatInterval()).thenReturn(Duration.ofSeconds(10));
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);

        Timestamp lowWatermark = new LowWatermarkCalculator(config, contextHolder, offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(earliest, lowWatermark);
        verifyNoInteractions(offsetProvider);
    }

    private static PartitionState initialPartition(String tvfName, Timestamp startTimestamp) {
        return PartitionState.builder()
                .token(InitialPartition.PARTITION_TOKEN)
                .tvfName(tvfName)
                .startTimestamp(startTimestamp)
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of())
                .build();
    }
}
