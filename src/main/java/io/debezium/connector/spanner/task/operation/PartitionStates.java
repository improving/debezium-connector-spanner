/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/** Shared lookups across every {@link PartitionState} visible from a {@link TaskSyncContext}. */
final class PartitionStates {

    private PartitionStates() {
    }

    /**
     * Every partition state visible from this context: the current task's own partitions and
     * shared partitions, plus those of every other task tracked in the sync topic.
     */
    static List<PartitionState> allPartitionStates(TaskSyncContext taskSyncContext) {
        return Stream.concat(
                Stream.concat(
                        taskSyncContext.getTaskStates().values().stream()
                                .flatMap(taskState -> taskState.getPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getPartitions().stream()),
                Stream.concat(
                        taskSyncContext.getTaskStates().values().stream()
                                .flatMap(taskState -> taskState.getSharedPartitions().stream()),
                        taskSyncContext.getCurrentTaskState().getSharedPartitions().stream()))
                .collect(Collectors.toList());
    }
}
