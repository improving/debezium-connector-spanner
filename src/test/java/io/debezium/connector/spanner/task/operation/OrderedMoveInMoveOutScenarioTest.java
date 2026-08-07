/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Reproduces the exact scenario from the "Partition Move-in/Move-out Ordering"
 * section of the mutable key range design doc: a destination partition P0
 * receives three sequential MoveIn boundaries - from P1, then P2, then P3 -
 * all at the same commit timestamp TS0, and must cancel/pause/resume its
 * query according to each source's independently published MoveOutState.
 *
 * <p>Preconditions (per the design doc's "Assume" list): P1 has <b>not</b> yet
 * processed any MoveOut event. P2 <b>already</b> processed its MoveOut event at
 * TS0, destined for P0, before P0 ever saw the MoveIn. P3 <b>already</b>
 * processed a MoveOut event at TS0 destined for P0, and has since processed a
 * newer one at TS1 (TS1 &gt; TS0) destined for P4.
 *
 * <pre>
 * 1) P0 hits MoveIn(P1, TS0, seq "00000")           -&gt; P0 blocked, waiting for P1
 *    P1 publishes MoveOutState{TS0, [P0]}           -&gt; P0 resumes
 * 2) P0 restarts, skips seq &lt;= "00000", hits
 *    MoveIn(P2, TS0, seq "00002")                   -&gt; P2 already published
 *    MoveOutState{TS0, [P0]}                        -&gt; P0 continues immediately
 * 3) P0 restarts, skips seq &lt;= "00002", processes
 *    a normal data record (seq "00003"), then hits
 *    MoveIn(P3, TS0, seq "00005")                   -&gt; P3 already published
 *    MoveOutState{TS1, [P4]} with TS1 &gt; TS0         -&gt; P0 continues immediately
 * </pre>
 *
 * Final state matches the design doc table exactly:
 * <pre>
 * P0: MoveInState: {TS0, "00005"} Parent: P3
 * P1: MoveOutState: {TS0, [P0]}
 * P2: MoveOutState: {TS0, [P0]}
 * P3: MoveOutState: {TS1, [P4]}
 * </pre>
 */
class OrderedMoveInMoveOutScenarioTest {

    private static final Timestamp TS0 = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    private static final Timestamp TS0_5 = Timestamp.ofTimeSecondsAndNanos(1500, 0);
    private static final Timestamp TS1 = Timestamp.ofTimeSecondsAndNanos(2000, 0);

    private TaskSyncContext context;

    private void seedPartitions(PartitionState... partitions) {
        context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(partitions))
                        .sharedPartitions(List.of())
                        .build())
                .build();
    }

    /** Mirrors {@code TaskStateChangeEventHandler.processEvent(MoveInNotificationEvent)}. */
    private void moveIn(String destToken, Timestamp ts, String recordSequence, String... sources) {
        context = new MoveInStateUpdateOperation(destToken, ts, recordSequence, List.of(sources)).doOperation(context);
        context = new FindPartitionForStreamingOperation().doOperation(context);
    }

    /** Mirrors {@code TaskStateChangeEventHandler.processEvent(MoveOutNotificationEvent)}. */
    private void moveOut(String sourceToken, Timestamp ts, String... destinations) {
        context = new MoveOutStateUpdateOperation(sourceToken, ts, List.of(destinations)).doOperation(context);
    }

    /** Mirrors the periodic {@code processSyncEvent()} re-evaluation on the destination's task. */
    private void refresh() {
        context = new FindPartitionForStreamingOperation().doOperation(context);
    }

    /**
     * Mirrors a source streaming forward on its own (e.g. via {@code onWindowAdvanced})
     * independently of anything it publishes as a MoveOut - so a test can simulate "this
     * source's own read position has passed some timestamp" without that timestamp
     * necessarily being one it also moved a key range out at.
     */
    private void advanceProcessedTimestamp(String token, Timestamp ts) {
        List<PartitionState> updated = context.getCurrentTaskState().getPartitions().stream()
                .map(p -> p.getToken().equals(token) ? p.toBuilder().processedTimestamp(ts).build() : p)
                .collect(Collectors.toList());
        context = context.toBuilder()
                .currentTaskState(context.getCurrentTaskState().toBuilder().partitions(updated).build())
                .build();
    }

    /** Marks a partition FINISHED, as {@code SourceRecordUtils}/the streaming loop would on EOF. */
    private void finish(String token, Timestamp finishedTimestamp) {
        List<PartitionState> updated = context.getCurrentTaskState().getPartitions().stream()
                .map(p -> p.getToken().equals(token)
                        ? p.toBuilder().state(PartitionStateEnum.FINISHED).finishedTimestamp(finishedTimestamp).build()
                        : p)
                .collect(Collectors.toList());
        context = context.toBuilder()
                .currentTaskState(context.getCurrentTaskState().toBuilder().partitions(updated).build())
                .build();
    }

    /** Mirrors the periodic {@link RemoveFinishedPartitionOperation} sweep. */
    private void removeFinishedPartitions() {
        SpannerEventDispatcher spannerEventDispatcher = mock(SpannerEventDispatcher.class);
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        lenient().when(connectorConfig.getFinishedPartitionDeletionDelay()).thenReturn(Duration.ZERO);
        context = new RemoveFinishedPartitionOperation(spannerEventDispatcher, connectorConfig).doOperation(context);
    }

    private PartitionState partition(String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals(token))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void sequentialMoveInsAtSameTimestampFromDifferentSources() {
        // Initial: P0 is RUNNING. P1 has not moved out yet. P2 already moved out to P0
        // at TS0. P3 already moved out to P0 at TS0, then moved out again to P4 at a
        // later timestamp TS1 - all per the design doc's stated preconditions.
        seedPartitions(
                PartitionState.builder().token("P0").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P1").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P2").state(PartitionStateEnum.RUNNING).parents(Set.of())
                        .moveOutStates(List.of(new MoveOutState(TS0, List.of("P0"))))
                        .build(),
                PartitionState.builder().token("P3").state(PartitionStateEnum.RUNNING).parents(Set.of())
                        .moveOutStates(List.of(
                                new MoveOutState(TS0, List.of("P0")),
                                new MoveOutState(TS1, List.of("P4"))))
                        .build());

        // --- 1st record: MoveIn from P1 at TS0, seq "00000" ---
        moveIn("P0", TS0, "00000", "P1");
        assertEquals(PartitionStateEnum.CREATED, partition("P0").getState(),
                "P0 must pause: P1 has not published MoveOutState yet");
        assertEquals(Set.of("P1"), partition("P0").getParents());
        assertEquals(TS0, partition("P0").getMoveInState().getTimestamp());
        assertEquals("00000", partition("P0").getMoveInState().getRecordSequence());

        // P1 processes its own MoveOut event at TS0, destined for P0.
        moveOut("P1", TS0, "P0");
        refresh();
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must resume: P1 moved out at the same TS0 and lists P0 as destination");

        // --- P0 restarts the query, skips seq <= "00000", hits the 2nd record: ---
        // MoveIn from P2 at TS0, seq "00002".
        moveIn("P0", TS0, "00002", "P2");
        assertEquals(Set.of("P2"), partition("P0").getParents());
        assertEquals("00002", partition("P0").getMoveInState().getRecordSequence());
        // P2 already published its MoveOutState{TS0, [P0]} before P0 ever saw this MoveIn
        // (a seeded precondition, not a subsequent event), so P0 continues immediately
        // without ever genuinely blocking - no separate moveOut()/refresh() needed.
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must continue immediately: P2 already moved out to P0 at the same TS0");

        // --- P0 restarts, skips seq <= "00002", processes the 3rd record (a normal data
        // record, seq "00003") normally, then hits the 4th record: MoveIn from P3 at TS0,
        // seq "00005".
        moveIn("P0", TS0, "00005", "P3");
        assertEquals(Set.of("P3"), partition("P0").getParents());
        assertEquals("00005", partition("P0").getMoveInState().getRecordSequence());
        // P3 already published a *later* MoveOut (TS1 > TS0), so P0 continues immediately
        // without waiting - matching the doc's final step.
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P0").getState(),
                "P0 must continue immediately: P3's MoveOutState timestamp TS1 is already past TS0");

        // Final state matches the design doc table exactly. P3's two MoveOutStates were
        // seeded directly as preconditions (not produced by moveOut() calls in this test),
        // so both are still present regardless of how MoveOutStateUpdateOperation itself
        // decides whether an older entry can be dropped.
        assertEquals(1, partition("P1").getMoveOutStates().size());
        assertEquals(TS0, partition("P1").getMoveOutStates().get(0).getTimestamp());
        assertEquals(List.of("P0"), partition("P1").getMoveOutStates().get(0).getDestPartitionTokens());
        assertEquals(1, partition("P2").getMoveOutStates().size());
        assertEquals(TS0, partition("P2").getMoveOutStates().get(0).getTimestamp());
        assertEquals(List.of("P0"), partition("P2").getMoveOutStates().get(0).getDestPartitionTokens());
        assertEquals(2, partition("P3").getMoveOutStates().size());
        assertEquals(TS1, partition("P3").getMoveOutStates().get(1).getTimestamp());
        assertEquals(List.of("P4"), partition("P3").getMoveOutStates().get(1).getDestPartitionTokens());
    }

    /**
     * Verifies a subtle edge case in {@code FindPartitionForStreamingOperation}: given a
     * MoveOut from P1 to P2 at T0, and a second, later MoveOut from P1 to P2 at T2 (T2 &gt;
     * T0), the older T0 entry never incorrectly satisfies a later wait tied to T2.
     */
    @Test
    void laterMoveInIsNotSatisfiedByAnOlderMoveOutToTheSameDestination() {
        // P1 has already moved a key range out to P2 once before, at T0.
        seedPartitions(
                PartitionState.builder().token("P1").state(PartitionStateEnum.RUNNING).parents(Set.of())
                        .moveOutStates(List.of(new MoveOutState(TS0, List.of("P2"))))
                        .build(),
                PartitionState.builder().token("P2").state(PartitionStateEnum.RUNNING).parents(Set.of()).build());

        // P2 later receives a second, independent MoveIn from the same source P1, this
        // time boundaried at T2 (TS1) - e.g. the key range moved back out and in again.
        moveIn("P2", TS1, "00010", "P1");
        assertEquals(PartitionStateEnum.CREATED, partition("P2").getState(),
                "P2 must pause: P1's only recorded MoveOut (T0) predates this MoveIn's T2, so it cannot satisfy it");
        assertEquals(TS1, partition("P2").getMoveInState().getTimestamp());

        // P1's own read position advances to T0_5 (between T0 and T2) - independent of any
        // MoveOut it has published.
        advanceProcessedTimestamp("P1", TS0_5);

        // At this intermediate point, refreshing must NOT resolve P2 - neither the older T0
        // MoveOutState entry nor P1's advanced-but-still-short-of-T2 processedTimestamp is
        // evidence that P1 has processed the T2 move.
        refresh();
        assertEquals(PartitionStateEnum.CREATED, partition("P2").getState(),
                "P2 must remain paused: an older, same-destination MoveOutState (and P1 not yet reaching T2) must not mask the still-pending later move");

        // P1 now actually publishes its second MoveOut, destined for P2, at T2.
        moveOut("P1", TS1, "P2");
        refresh();
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P2").getState(),
                "P2 must resume now that P1 has published the matching T2 MoveOutState");

        // The T0 entry is gone: P2's own MoveIn already advanced its processedTimestamp past
        // T0 (see MoveInStateUpdateOperation), so by the time this second moveOut() runs, the
        // T0 entry has already resolved (MoveOutStateResolution) and is dropped in favor of
        // the new T2 entry.
        assertEquals(1, partition("P1").getMoveOutStates().size());
        assertEquals(TS1, partition("P1").getMoveOutStates().get(0).getTimestamp());
        assertEquals(List.of("P2"), partition("P1").getMoveOutStates().get(0).getDestPartitionTokens());
    }

    /**
     * Verifies a same-timestamp race: D1 and D2 mutate the same key, and physically move
     * from P1 to P2 at a shared commit timestamp T2 - P1 emits D1 (seq0) then its own
     * MoveOut (seq1); P2 then sees the MoveIn (seq2) before D2 (seq3). P2 cannot process D2
     * (seq3) until P1's own MoveOut (seq1) is on record, even while P1 is still working
     * through T2 - otherwise D2 could be reordered ahead of D1 downstream.
     */
    @Test
    void destinationCannotProceedUntilSourcesOwnMoveOutAtTheSharedTimestampIsProcessed() {
        // Nothing has moved yet - P1 has no MoveOutState history at all.
        seedPartitions(
                PartitionState.builder().token("P1").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P2").state(PartitionStateEnum.RUNNING).parents(Set.of()).build());

        // P2 sees the MoveIn (seq2) at T2 - this can happen before or after P1 reaches its
        // own MoveOut (seq1); either way P2 must pause until that MoveOut is on record.
        moveIn("P2", TS1, "00002", "P1");
        assertEquals(PartitionStateEnum.CREATED, partition("P2").getState(),
                "P2 must pause: P1 has not recorded any MoveOut yet, let alone one at T2");

        // P1 is still working through its T2 window - it has emitted D1 (seq0) downstream
        // already (not modeled here, since it doesn't touch PartitionState) but has not
        // yet reached/processed its own MoveOut record (seq1).
        refresh();
        assertEquals(PartitionStateEnum.CREATED, partition("P2").getState(),
                "P2 must still be paused while P1 has processed D1 but not yet its own MoveOut - "
                        + "otherwise P2 could read D2 (seq3) before D1 (seq0) is guaranteed emitted");

        // P1 now reaches seq1 and records its MoveOut for T2, destined for P2.
        moveOut("P1", TS1, "P2");
        refresh();
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P2").getState(),
                "P2 may now proceed to read D2 (seq3) - by Spanner's own commit-sequence ordering, "
                        + "P1 could not have emitted seq1 without D1 (seq0) already having been emitted");
    }

    /**
     * Verifies that an unresolved {@link MoveOutState} entry survives a later, unrelated
     * MoveOut recorded at a different timestamp - {@link MoveOutStateUpdateOperation} only
     * drops an entry once its own destinations have caught up (see {@link
     * MoveOutStateResolution}), never merely because a newer entry came in.
     */
    @Test
    void unresolvedMoveOutEntryIsKeptAcrossANewerUnrelatedMove() {
        // P2 hasn't discovered the dependency at all yet - no moveInState, no parents.
        seedPartitions(
                PartitionState.builder().token("P1").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P2").state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                PartitionState.builder().token("P3").state(PartitionStateEnum.RUNNING).parents(Set.of()).build());

        // P1 moves out to P2 at T0, then to the unrelated P3 at a later T1.
        moveOut("P1", TS0, "P2");
        moveOut("P1", TS1, "P3");

        // P3 catches up to the move it actually depends on; P2 never does anything at all.
        advanceProcessedTimestamp("P3", TS1);

        // P1 finishes streaming its own key range and becomes eligible for deletion.
        finish("P1", TS0);
        removeFinishedPartitions();

        // P2 only now gets around to processing the boundary and issuing its MoveIn from P1.
        moveIn("P2", TS0, "00000", "P1");
        refresh();

        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, partition("P2").getState(),
                "P2 must resume once P1's T0 MoveOut is on record - if this fails, the T0 entry was pruned "
                        + "before P2 ever caught up, so P1 was deleted and P2 has no source left to check");
    }
}
