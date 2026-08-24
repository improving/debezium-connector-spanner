/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Clear partition from the shared section of the task state,
 * after partition was picked up by another task
 */
public class ClearSharedPartitionOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClearSharedPartitionOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    private TaskSyncContext clear(TaskSyncContext taskSyncContext) {

        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        // Retrieve the tokens that are owned by other tasks.
        Set<String> otherTokens = taskSyncContext.getAllTaskStates().values().stream().flatMap(taskState -> taskState.getPartitions().stream())
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());

        List<PartitionState> currentSharedList = currentTaskState.getSharedPartitions().stream()
                .collect(Collectors.toList());

        List<PartitionState> finalSharedList = new ArrayList<PartitionState>();

        // Filter or reassign shared partitions that are currently owned or shared to dead tasks.
        for (PartitionState sharedToken : currentSharedList) {
            // This token is owned by another task.
            if (otherTokens.contains(sharedToken.getToken())) {
                LOGGER.info("Task {}, removing token {} since it is already owned by other tasks", taskSyncContext.getTaskUid(), sharedToken);
            }

            // Mutable key range race: the same token can appear in multiple tasks' sharedPartitions
            // simultaneously when several source partitions each emit a PartitionStartRecord for
            // the same destination (empty parentTokens bypasses ConflictResolver). Break the tie
            // deterministically: the task with the lexicographically smaller UID keeps its claim;
            // the higher-UID task yields by removing its own sharedPartitions entry.
            else if (isClaimedByLowerUidTask(taskSyncContext, sharedToken.getToken())) {
                LOGGER.warn("Task {}, removing duplicate shared partition {} — another task with lower UID has already claimed it",
                        taskSyncContext.getTaskUid(), sharedToken.getToken());
            }

            else {
                // This token is not owned by other tasks, nor is it shared to a dead task.
                finalSharedList.add(sharedToken);
            }
        }

        if (finalSharedList.size() != currentSharedList.size()) {
            this.isRequiredPublishSyncEvent = true;
        }

        return taskSyncContext.toBuilder().currentTaskState(currentTaskState.toBuilder()
                .sharedPartitions(finalSharedList)
                .build()).build();
    }

    /**
     * Returns true if any other task whose UID is lexicographically smaller than the current task's
     * UID has the given token in its {@code sharedPartitions}. Used to break ties when multiple
     * tasks claim the same token simultaneously (mutable key range race condition).
     */
    private boolean isClaimedByLowerUidTask(TaskSyncContext context, String token) {
        String currentUid = context.getCurrentTaskState().getTaskUid();
        return context.getTaskStates().values().stream()
                .filter(ts -> ts.getTaskUid().compareTo(currentUid) < 0)
                .flatMap(ts -> ts.getSharedPartitions().stream())
                .anyMatch(p -> p.getToken().equals(token));
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return clear(taskSyncContext);
    }
}
