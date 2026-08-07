/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Records the {@link MoveOutState} for a source partition that has processed a MoveOut event.
 * If the new MoveOut shares the same commit timestamp as the partition's last-recorded
 * {@code MoveOutState}, its destinations are merged into that entry. Otherwise, any existing
 * entry whose destinations have already all caught up to (or past) its own timestamp is dropped -
 * see {@link MoveOutStateResolution} - since nothing can still be waiting on it, and the new
 * MoveOut is added in its place. An entry whose destinations have not yet all caught up is kept
 * alongside the new one until it resolves, so that {@link RemoveFinishedPartitionOperation} never
 * loses track of a still-outstanding move. Triggers a sync-topic publish so that destination
 * partitions can observe the updated {@link PartitionState#getMoveOutStates()} via the sync topic.
 */
public class MoveOutStateUpdateOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MoveOutStateUpdateOperation.class);

    private final String token;
    private final Timestamp commitTimestamp;
    private final List<String> destinationTokens;

    public MoveOutStateUpdateOperation(String token, Timestamp commitTimestamp, List<String> destinationTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.destinationTokens = destinationTokens;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();
        List<PartitionState> allPartitionStates = PartitionStates.allPartitionStates(taskSyncContext);

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return partitionState.toBuilder()
                                .moveOutStates(updatedMoveOutStates(allPartitionStates, partitionState.getMoveOutStates()))
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        LOGGER.info("Task {}, MoveOut state updated: partition={}, commitTimestamp={}, destinations={}",
                taskSyncContext.getTaskUid(), token, commitTimestamp, destinationTokens);

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }

    /**
     * Folds the new MoveOut into {@code existing}. Two cases:
     *
     * <p><b>Same timestamp as the last entry:</b> this is the same commit window recording
     * another destination (e.g. a key range split across several destinations in one move), not
     * a genuinely new move - so it's merged into that entry rather than kept as a separate one.
     * Earlier entries, if any, are left untouched.
     *
     * <p><b>Different timestamp:</b> this is a new, independent move. Before adding it, every
     * existing entry is re-checked and dropped if it has already resolved (all its destinations
     * have caught up - see {@link MoveOutStateResolution}), since nothing can still be waiting on
     * it. Anything not yet resolved is kept regardless of how old it is, so {@link
     * RemoveFinishedPartitionOperation} never loses track of a still-outstanding move just
     * because this source has since moved on to a different destination.
     */
    private List<MoveOutState> updatedMoveOutStates(List<PartitionState> allPartitionStates, List<MoveOutState> existing) {
        MoveOutState last = existing.isEmpty() ? null : existing.get(existing.size() - 1);
        if (last != null && last.getTimestamp().equals(commitTimestamp)) {
            // Same commit window as the last entry - fold the new destinations into it instead
            // of appending a sibling entry for the same move.
            List<String> mergedDestinations = new ArrayList<>(last.getDestPartitionTokens());
            mergedDestinations.addAll(destinationTokens);
            List<MoveOutState> merged = new ArrayList<>(existing.subList(0, existing.size() - 1));
            merged.add(new MoveOutState(commitTimestamp, mergedDestinations));
            return merged;
        }

        // A genuinely new move. Drop any existing entry that has already resolved - it's no
        // longer needed - but keep anything still outstanding, however old, so this source
        // doesn't silently forget a destination that hasn't caught up yet.
        List<MoveOutState> stillPending = existing.stream()
                .filter(moveOutState -> !MoveOutStateResolution.allDestinationsHaveResumed(allPartitionStates, moveOutState))
                .collect(Collectors.toList());
        stillPending.add(new MoveOutState(commitTimestamp, destinationTokens));
        return stillPending;
    }
}
