/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;

/**
 * Shared logic for deciding whether every destination named in a {@link MoveOutState} entry has
 * caught up to (or past) that entry's commit timestamp in its own stream. Used both to decide
 * whether a finished source partition is safe to delete ({@link RemoveFinishedPartitionOperation})
 * and whether an older {@link MoveOutState} entry is safe to drop when a newer one is recorded
 * ({@link MoveOutStateUpdateOperation}) - an entry must never be discarded by the latter while the
 * former would still need it.
 */
final class MoveOutStateResolution {

    private MoveOutStateResolution() {
    }

    static boolean allDestinationsHaveResumed(List<PartitionState> allPartitionStates, MoveOutState moveOutState) {
        Timestamp moveOutTimestamp = moveOutState.getTimestamp();
        for (String destToken : moveOutState.getDestPartitionTokens()) {
            PartitionState dest = allPartitionStates.stream()
                    .filter(p -> destToken.equals(p.getToken()))
                    .findFirst()
                    .orElse(null);
            if (dest == null) {
                // Destination not tracked anywhere - nothing left depending on this source.
                continue;
            }
            boolean destHasReachedThisMove = dest.getProcessedTimestamp() != null
                    && dest.getProcessedTimestamp().compareTo(moveOutTimestamp) >= 0;
            if (!destHasReachedThisMove) {
                return false;
            }
        }
        return true;
    }
}
