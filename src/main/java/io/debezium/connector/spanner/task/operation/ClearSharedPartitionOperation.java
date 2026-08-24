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
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
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

        Set<String> lowerUidActiveTokens = lowerUidActivePartitionTokens(taskSyncContext);

        List<PartitionState> currentPartitions = new ArrayList<>(currentTaskState.getPartitions());
        List<PartitionState> finalPartitions = new ArrayList<>(currentPartitions.size());
        boolean partitionsHealed = false;

        for (PartitionState p : currentPartitions) {
            if (!PartitionStateEnum.FINISHED.equals(p.getState())
                    && !PartitionStateEnum.REMOVED.equals(p.getState())
                    && lowerUidActiveTokens.contains(p.getToken())) {
                LOGGER.warn("Task {}, self-healing duplicate partition {} — a lower-UID task already owns it; marking REMOVED",
                        taskSyncContext.getTaskUid(), p.getToken());
                finalPartitions.add(p.toBuilder().state(PartitionStateEnum.REMOVED).build());
                partitionsHealed = true;
            }
            else {
                finalPartitions.add(p);
            }
        }

        if (finalSharedList.size() != currentSharedList.size() || partitionsHealed) {
            this.isRequiredPublishSyncEvent = true;
        }

        return taskSyncContext.toBuilder().currentTaskState(currentTaskState.toBuilder()
                .sharedPartitions(finalSharedList)
                .partitions(finalPartitions)
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

    /**
     * Returns the set of partition tokens actively owned (non-FINISHED, non-REMOVED) by tasks
     * whose UID is lexicographically smaller than the current task's UID. Used to detect
     * partitions-level duplicates created by the mutable key range race condition so the
     * higher-UID task can yield.
     */
    private Set<String> lowerUidActivePartitionTokens(TaskSyncContext context) {
        String currentUid = context.getCurrentTaskState().getTaskUid();
        return context.getTaskStates().values().stream()
                .filter(ts -> ts.getTaskUid().compareTo(currentUid) < 0)
                .flatMap(ts -> ts.getPartitions().stream())
                .filter(p -> !PartitionStateEnum.FINISHED.equals(p.getState())
                        && !PartitionStateEnum.REMOVED.equals(p.getState()))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
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
