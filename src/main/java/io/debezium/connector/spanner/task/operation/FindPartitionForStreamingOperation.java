/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import io.debezium.connector.spanner.task.PartitionOffsetProvider;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Checks what partitions are ready for streaming
 */
public class FindPartitionForStreamingOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionForStreamingOperation.class);

    private boolean isRequiredPublishSyncEvent = false;
    private final boolean isMutableKeyRange;
    private final PartitionOffsetProvider partitionOffsetProvider;
    private Map<String, Timestamp> committedOffsets = Map.of();

    public FindPartitionForStreamingOperation() {
        this(false, null);
    }

    public FindPartitionForStreamingOperation(boolean isMutableKeyRange) {
        this(isMutableKeyRange, null);
    }

    public FindPartitionForStreamingOperation(boolean isMutableKeyRange, PartitionOffsetProvider partitionOffsetProvider) {
        this.isMutableKeyRange = isMutableKeyRange;
        this.partitionOffsetProvider = partitionOffsetProvider;
    }

    private TaskSyncContext takePartitionForStreaming(TaskSyncContext taskSyncContext) {
        Set<String> finishedPartitions = getFinishedPartitions(taskSyncContext);

        TaskState taskState = taskSyncContext.getCurrentTaskState();
        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.CREATED)) {
                        boolean takePartitionForStreaming = false;
                        LOGGER.debug("Task sees partition with CREATED state, task Uid {}, partition {}", taskSyncContext.getTaskUid(), partitionState);
                        if (partitionState.getMoveInState() != null) {
                            if (canDestPartitionContinue(taskSyncContext, partitionState, finishedPartitions)) {
                                LOGGER.info("Task takes MoveIn partition for streaming, source(s) committed past MoveIn, taskUid: {}, partition {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken());
                                takePartitionForStreaming = true;
                            }
                            else {
                                LOGGER.info(
                                        "Task not taking MoveIn partition for streaming, waiting for source(s) to commit past MoveIn, taskUid: {}, partition {}, sources {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            }
                        }
                        else if (finishedPartitions.containsAll(partitionState.getParents()) || isMutableKeyRange) {
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

    /**
     * Determines whether a destination partition that is paused after processing a MoveIn
     * event can resume streaming. This requires that every source partition referenced in the
     * destination's {@link MoveInState} has committed all data up to the MoveIn commit timestamp.
     *
     * <p>A source can prove this in two ways:
     * <ol>
     *   <li>It has published a {@link MoveOutState} at or past the MoveIn timestamp that includes
     *       this destination, and its Kafka-committed offset is strictly past the MoveIn timestamp.
     *   <li>The source partition has reached {@code FINISHED}/{@code REMOVED}, which means it has
     *       already streamed past every boundary in its key range.
     *   <li>As a crash-recovery fallback, if no {@link MoveOutState} is present, the destination can
     *       also resume once the source's Kafka-committed offset is strictly past the MoveIn
     *       timestamp. This covers the case where a task crashed after committing past a boundary but
     *       before the corresponding {@code MoveOutStateUpdateOperation} was persisted to the sync
     *       topic.
     * </ol>
     *
     * <p>Strictly greater-than is required because Kafka offsets for this connector are Spanner
     * commit timestamps. At the exact MoveIn timestamp there may be multiple records (for example a
     * heartbeat and a data change, or several data changes in the same transaction); a committed
     * offset equal to the MoveIn timestamp does not guarantee that every record at that timestamp
     * has been durably committed.
     */
    private boolean canDestPartitionContinue(TaskSyncContext taskSyncContext, PartitionState destPartition, Set<String> finishedPartitions) {
        MoveInState moveInState = destPartition.getMoveInState();
        Timestamp moveInTimestamp = moveInState.getTimestamp();
        String destToken = destPartition.getToken();

        for (String sourceToken : moveInState.getSourcePartitionTokens()) {
            if (!sourceHasResumedThisMove(taskSyncContext, sourceToken, moveInTimestamp, destToken, finishedPartitions)) {
                return false;
            }
        }
        return true;
    }

    private boolean sourceHasResumedThisMove(TaskSyncContext taskSyncContext, String sourceToken, Timestamp moveInTimestamp,
                                             String destToken, Set<String> finishedPartitions) {
        boolean satisfiedByMoveOutState = findMoveOutStates(taskSyncContext, sourceToken).stream()
                .anyMatch(moveOutState -> {
                    int cmp = moveOutState.getTimestamp().compareTo(moveInTimestamp);
                    return cmp > 0 || (cmp == 0 && moveOutState.getDestPartitionTokens().contains(destToken));
                });
        if (satisfiedByMoveOutState) {
            if (partitionOffsetProvider == null) {
                return true;
            }
            return isSourceCommittedPast(taskSyncContext, sourceToken, moveInTimestamp, destToken);
        }
        if (finishedPartitions.contains(sourceToken)) {
            LOGGER.info("Task {}, source partition {} already finished and purged, treating MoveOut as satisfied for destination {}",
                    taskSyncContext.getTaskUid(), sourceToken, destToken);
            return true;
        }
        if (partitionOffsetProvider != null) {
            return isSourceCommittedPast(taskSyncContext, sourceToken, moveInTimestamp, destToken);
        }
        PartitionState sourceState = findPartitionState(taskSyncContext, sourceToken);
        if (sourceState != null && sourceState.getProcessedTimestamp() != null
                && sourceState.getProcessedTimestamp().compareTo(moveInTimestamp) > 0) {
            LOGGER.info(
                    "Task {}, source partition {} already streamed past MoveIn timestamp {} (processedTimestamp={}) despite missing a matching MoveOutState "
                            + "(likely lost in a crash before it was persisted), treating MoveOut as satisfied for destination {}",
                    taskSyncContext.getTaskUid(), sourceToken, moveInTimestamp, sourceState.getProcessedTimestamp(), destToken);
            return true;
        }
        return false;
    }

    private boolean isSourceCommittedPast(TaskSyncContext taskSyncContext, String sourceToken, Timestamp moveInTimestamp, String destToken) {
        Timestamp committedOffset = committedOffsets.get(sourceToken);
        if (committedOffset == null) {
            LOGGER.info("Task {}, source partition {} has no committed offset yet, not resuming destination {} for MoveIn at {}",
                    taskSyncContext.getTaskUid(), sourceToken, destToken, moveInTimestamp);
            return false;
        }
        if (committedOffset.compareTo(moveInTimestamp) > 0) {
            LOGGER.info("Task {}, source partition {} committed offset {} is past MoveIn timestamp {}, resuming destination {}",
                    taskSyncContext.getTaskUid(), sourceToken, committedOffset, moveInTimestamp, destToken);
            return true;
        }
        LOGGER.info("Task {}, source partition {} committed offset {} is not past MoveIn timestamp {}, waiting",
                taskSyncContext.getTaskUid(), sourceToken, committedOffset, moveInTimestamp);
        return false;
    }

    private Map<String, Timestamp> loadCommittedOffsets(TaskSyncContext taskSyncContext) {
        if (partitionOffsetProvider == null) {
            return Map.of();
        }
        Set<String> sourceTokens = taskSyncContext.getCurrentTaskState().getPartitions().stream()
                .filter(partitionState -> PartitionStateEnum.CREATED.equals(partitionState.getState()))
                .filter(partitionState -> partitionState.getMoveInState() != null)
                .flatMap(partitionState -> partitionState.getMoveInState().getSourcePartitionTokens().stream())
                .collect(Collectors.toSet());
        if (sourceTokens.isEmpty()) {
            return Map.of();
        }
        Map<String, Timestamp> offsets = partitionOffsetProvider.getOffsets(sourceTokens);
        return offsets == null ? Map.of() : offsets;
    }

    private List<MoveOutState> findMoveOutStates(TaskSyncContext taskSyncContext, String token) {
        PartitionState partitionState = findPartitionState(taskSyncContext, token);
        return partitionState == null ? List.of() : partitionState.getMoveOutStates();
    }

    private PartitionState findPartitionState(TaskSyncContext taskSyncContext, String token) {
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState;
            }
        }
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getSharedPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState;
            }
        }
        for (TaskState taskState : taskSyncContext.getTaskStates().values()) {
            for (PartitionState partitionState : taskState.getPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState;
                }
            }
            for (PartitionState partitionState : taskState.getSharedPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState;
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
        this.committedOffsets = loadCommittedOffsets(taskSyncContext);
        return takePartitionForStreaming(taskSyncContext);
    }
}
