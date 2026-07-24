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

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Checks what partitions are ready for streaming
 */
public class FindPartitionForStreamingOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionForStreamingOperation.class);

    private boolean isRequiredPublishSyncEvent = false;

    public FindPartitionForStreamingOperation() {
    }

    private TaskSyncContext takePartitionForStreaming(TaskSyncContext taskSyncContext) {
        Set<String> allPartitions = getAllPartitions(taskSyncContext);
        Set<String> finishedPartitions = getFinishedPartitions(taskSyncContext);

        TaskState taskState = taskSyncContext.getCurrentTaskState();
        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.CREATED)) {
                        boolean takePartitionForStreaming = false;
                        LOGGER.debug("Task sees partition with CREATED state, task Uid {}, partition {}", taskSyncContext.getTaskUid(), partitionState);
                        if (partitionState.getMoveInState() != null) {
                            if (canDestPartitionContinue(taskSyncContext, partitionState)) {
                                LOGGER.info("Task takes MoveIn partition for streaming, source(s) processed MoveOut, taskUid: {}, partition {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken());
                                takePartitionForStreaming = true;
                            }
                            else {
                                LOGGER.info("Task not taking MoveIn partition for streaming, waiting for source(s) MoveOut, taskUid: {}, partition {}, sources {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            }
                        }
                        else if (finishedPartitions.containsAll(partitionState.getParents())) {
                            takePartitionForStreaming = true;
                            LOGGER.info("Task takes partition for streaming, taskUid: {}, partition {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken());

                        }
                        else if (!atLeastOneParentExists(taskSyncContext, partitionState.getParents())) {
                            LOGGER.info("Task takes partition for streaming, since parents no longer exist, taskUid: {}, partition {}, parents {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            takePartitionForStreaming = true;
                        }
                        else {
                            LOGGER.info("Task not taking partition for streaming, since parents are not finished, taskUid: {}, partition {}, parents {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());

                        }

                        if (takePartitionForStreaming) {
                            this.isRequiredPublishSyncEvent = true;

                            return partitionState.toBuilder()
                                    .state(PartitionStateEnum.READY_FOR_STREAMING)
                                    .build();
                        }
                        else {
                            return partitionState;
                        }
                    }
                    return partitionState;
                }).collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private Set<String> getFinishedPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));

        return partitionStateList.stream()
                .filter(partitionState -> PartitionStateEnum.FINISHED.equals(partitionState.getState())
                        || PartitionStateEnum.REMOVED.equals(partitionState.getState()))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
    }

    private Set<String> getAllPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        // add all owned partitions
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));

        // add all shared parttions.
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getSharedPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .collect(Collectors.toList()));
        return partitionStateList.stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
    }

    /**
     * Determines whether a destination partition that is paused after processing a MoveIn
     * event can resume streaming. This requires that every source partition referenced in the
     * destination's {@link MoveInState} has published a {@link MoveOutState} that is at or past
     * the MoveIn commit timestamp, and, if exactly at that timestamp, includes this destination
     * partition among its recorded destinations.
     */
    private boolean canDestPartitionContinue(TaskSyncContext taskSyncContext, PartitionState destPartition) {
        MoveInState moveInState = destPartition.getMoveInState();
        Timestamp moveInTimestamp = moveInState.getTimestamp();
        String destToken = destPartition.getToken();

        for (String sourceToken : moveInState.getSourcePartitionTokens()) {
            MoveOutState sourceMoveOutState = findMoveOutState(taskSyncContext, sourceToken);
            if (sourceMoveOutState == null) {
                return false;
            }
            int cmp = sourceMoveOutState.getTimestamp().compareTo(moveInTimestamp);
            if (cmp < 0) {
                return false;
            }
            else if (cmp == 0 && !sourceMoveOutState.getDestPartitionTokens().contains(destToken)) {
                return false;
            }
        }
        return true;
    }

    private MoveOutState findMoveOutState(TaskSyncContext taskSyncContext, String token) {
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState.getMoveOutState();
            }
        }
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getSharedPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState.getMoveOutState();
            }
        }
        for (TaskState taskState : taskSyncContext.getTaskStates().values()) {
            for (PartitionState partitionState : taskState.getPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState.getMoveOutState();
                }
            }
            for (PartitionState partitionState : taskState.getSharedPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState.getMoveOutState();
                }
            }
        }
        return null;
    }

    private boolean atLeastOneParentExists(TaskSyncContext taskSyncContext, Set<String> parents) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getSharedPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .collect(Collectors.toList()));

        Set<String> allPartitions = partitionStateList.stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
        for (String parent : parents) {
            if (allPartitions.contains(parent)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return takePartitionForStreaming(taskSyncContext);
    }
}
