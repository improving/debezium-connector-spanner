/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.task.MoveInGateChecker;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Per-partition buffer that accumulates {@link ChangeStreamEvent}s received after a MoveIn
 * boundary while the destination partition waits for all source partition(s) to confirm
 * their corresponding MoveOut event(s) via the sync topic.
 *
 * <p>The underlying Spanner gRPC connection <em>stays open</em> throughout; events are
 * held here rather than being forwarded to the downstream blocking
 * {@link io.debezium.connector.spanner.StreamEventQueue}. When
 * {@link #isGateOpen()} returns {@code true} the caller drains the buffer with
 * {@link #drain()} and resumes normal forwarding on the same result set — no
 * reconnection or re-query to Spanner is needed.
 *
 * <p>A single gate instance may accumulate multiple consecutive MoveIn events (at the
 * same or later commit timestamps). The source-token set grows as new MoveIn records
 * arrive; every entry must individually be satisfied before the gate opens.
 *
 * <p>When {@link #isFull()} returns {@code true} the caller must fall back to the
 * existing close/reopen path (the original MoveIn pause behaviour) and discard this
 * gate.  The {@link #getFirstMoveInEvent()} and {@link #getFirstMoveInMetadata()}
 * accessors supply the values needed for that fallback path.
 */
public class MoveInBufferGate {

    /**
     * Ordered map of moveInTimestamp → source-token set accumulated from all MoveIn
     * events seen while this gate is active.  {@code LinkedHashMap} preserves insertion
     * order so logging is deterministic.
     */
    private final Map<Timestamp, Set<String>> sourcesByTimestamp = new LinkedHashMap<>();

    private final List<ChangeStreamEvent> buffer = new ArrayList<>();

    private final String destToken;
    private final int maxBufferEvents;
    private final Supplier<TaskSyncContext> taskSyncContextSupplier;

    /** First MoveIn event seen; kept for the overflow-fallback path. */
    private PartitionEventEvent firstMoveInEvent;
    /** Metadata of the first MoveIn event; used for latency-metric logging on fallback. */
    private ChangeStreamResultSetMetadata firstMoveInMetadata;

    public MoveInBufferGate(String destToken, int maxBufferEvents,
                            Supplier<TaskSyncContext> taskSyncContextSupplier) {
        this.destToken = destToken;
        this.maxBufferEvents = maxBufferEvents;
        this.taskSyncContextSupplier = taskSyncContextSupplier;
    }

    /**
     * Records a MoveIn event. Must be called for <em>every</em> MoveIn event
     * encountered while this gate is active, including the first one that created
     * the gate.
     *
     * @param ts          commit timestamp of the MoveIn event
     * @param sourceTokens source partition tokens listed in the MoveIn record
     * @param event       the raw {@link PartitionEventEvent} (stored for fallback use)
     * @param metadata    result-set metadata at the time the event was read (stored for
     *                    latency logging on the fallback path)
     */
    public void recordMoveIn(Timestamp ts, List<String> sourceTokens,
                             PartitionEventEvent event, ChangeStreamResultSetMetadata metadata) {
        sourcesByTimestamp.computeIfAbsent(ts, k -> new LinkedHashSet<>()).addAll(sourceTokens);
        if (firstMoveInEvent == null) {
            firstMoveInEvent = event;
            firstMoveInMetadata = metadata;
        }
    }

    /**
     * Appends an event to the in-memory buffer.  Events are stored in the order
     * they arrived from Spanner and will be forwarded to the downstream queue in that
     * same order when the gate opens.
     */
    public void add(ChangeStreamEvent event) {
        buffer.add(event);
    }

    /**
     * Returns {@code true} when the buffer has reached its configured capacity limit.
     * The caller must then fall back to the existing close/reopen path.
     */
    public boolean isFull() {
        return buffer.size() >= maxBufferEvents;
    }

    /** Returns the number of events currently held in the buffer. */
    public int size() {
        return buffer.size();
    }

    /**
     * Checks — without blocking — whether all source partitions for every accumulated
     * MoveIn entry have confirmed their MoveOut event.  Reads the
     * {@link TaskSyncContext} snapshot non-blockingly from the injected supplier; the
     * {@code TaskSyncContextHolder} uses an {@link java.util.concurrent.atomic.AtomicReference}
     * so this call is wait-free.
     *
     * @return {@code true} if the destination partition may resume forwarding events
     */
    public boolean isGateOpen() {
        TaskSyncContext ctx = taskSyncContextSupplier.get();
        Set<String> finished = MoveInGateChecker.getFinishedPartitions(ctx);
        for (Map.Entry<Timestamp, Set<String>> entry : sourcesByTimestamp.entrySet()) {
            Timestamp moveInTs = entry.getKey();
            List<String> sources = new ArrayList<>(entry.getValue());
            if (!MoveInGateChecker.canContinue(ctx, destToken, moveInTs, sources, finished)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Removes and returns all buffered events in arrival order. Must only be called
     * after {@link #isGateOpen()} has returned {@code true}.
     */
    public List<ChangeStreamEvent> drain() {
        List<ChangeStreamEvent> out = new ArrayList<>(buffer);
        buffer.clear();
        return out;
    }

    /**
     * Returns an ordered snapshot of all (timestamp → sources) pairs accumulated by
     * this gate, intended for logging.
     */
    public Map<Timestamp, Set<String>> getSourcesByTimestamp() {
        return new LinkedHashMap<>(sourcesByTimestamp);
    }

    /** First MoveIn event seen; used by the overflow-fallback path. */
    public PartitionEventEvent getFirstMoveInEvent() {
        return firstMoveInEvent;
    }

    /** Metadata of the first MoveIn event; used for latency-metric logging on fallback. */
    public ChangeStreamResultSetMetadata getFirstMoveInMetadata() {
        return firstMoveInMetadata;
    }
}
