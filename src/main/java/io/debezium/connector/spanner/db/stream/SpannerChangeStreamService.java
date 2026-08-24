/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEndEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.db.model.event.RecordSequenceUtils;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.DelayChangeStreamEventsMetricEvent;

/**
 * This class queries the change stream, sends child partitions to SynchronizedPartitionManager,
 * and updates the last commit timestamp for each partition.
 */
public class SpannerChangeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeStreamService.class);

    private final ChangeStreamDao changeStreamDao;
    private final ChangeStreamRecordMapper changeStreamRecordMapper;

    private final Duration heartbeatMillis;
    private final MetricsEventPublisher metricsEventPublisher;
    private final String taskUid;
    private final Duration windowDuration;
    private final boolean mutablePartitionOrderingEnabled;

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, 20);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, windowMinutes, true);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      boolean mutablePartitionOrderingEnabled) {
        this.changeStreamDao = changeStreamDao;
        this.changeStreamRecordMapper = changeStreamRecordMapper;
        this.heartbeatMillis = heartbeatMillis;
        this.metricsEventPublisher = metricsEventPublisher;
        this.taskUid = taskUid;
        this.windowDuration = Duration.ofMinutes(windowMinutes);
        this.mutablePartitionOrderingEnabled = mutablePartitionOrderingEnabled;
    }

    public boolean isMutableKeyRange() {
        return changeStreamDao.isMutableKeyRange();
    }

    public void getEvents(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                          PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        if (changeStreamDao.isMutableKeyRange()) {
            getEventsMutable(partition, changeStreamEventConsumer, partitionEventListener);
        }
        else {
            getEventsImmutable(partition, changeStreamEventConsumer, partitionEventListener);
        }
    }

    private void getEventsImmutable(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                                    PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        final String token = partition.getToken();

        partitionEventListener.onRun(partition);

        LOGGER.info("Task: {}, Streaming {} from {} to {}", taskUid, token, partition.getStartTimestamp(), partition.getEndTimestamp());
        try (ChangeStreamResultSet resultSet = changeStreamDao.streamQuery(token, partition.getStartTimestamp(),
                partition.getEndTimestamp(), heartbeatMillis.toMillis())) {

            long start = now();
            while (resultSet.next()) {
                long delay = now() - start;

                List<ChangeStreamEvent> events = changeStreamRecordMapper.toChangeStreamEvents(
                        partition,
                        resultSet, resultSet.getMetadata());
                LOGGER.debug("Task: {}, Events receive from stream: {}", taskUid, events);

                if (!events.isEmpty() && (events.get(0) instanceof HeartbeatEvent)) {
                    var heartbeatEvent = (HeartbeatEvent) events.get(0);
                    long heartbeatLag = System.currentTimeMillis() - heartbeatEvent.getRecordTimestamp().toSqlTimestamp().toInstant().toEpochMilli();
                    if (heartbeatLag > 60_000) {
                        LOGGER.warn("Task: {}, heartbeat has very old timestamp, lag: {}, token: {}, event: {}", taskUid, heartbeatLag,
                                heartbeatEvent.getMetadata().getPartitionToken(),
                                heartbeatEvent);
                    }
                }

                processEvents(partition, events, changeStreamEventConsumer);

                if (!events.isEmpty() && !(events.get(0) instanceof HeartbeatEvent)) {
                    metricsEventPublisher.publishMetricEvent(new DelayChangeStreamEventsMetricEvent((int) delay));
                }

                start = now();
            }
        }
        catch (InterruptedException ex) {
            LOGGER.info("task {}, Interrupting streaming partition task with token {}", this.taskUid, partition.getToken());
            Thread.currentThread().interrupt();
        }

        partitionEventListener.onFinish(partition);
        LOGGER.info("Task {}, Finished consuming partition {}", taskUid, partition);

        changeStreamEventConsumer.acceptChangeStreamEvent(new FinishPartitionEvent(partition));
    }

    private void getEventsMutable(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                                  PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        final String token = partition.getToken();

        partitionEventListener.onRun(partition);

        LOGGER.info("Task: {}, Streaming mutable partition {} from {} to {}", taskUid, token,
                partition.getStartTimestamp(), partition.getEndTimestamp());

        Timestamp partitionEndTimestamp = partition.getEndTimestamp();

        Timestamp processedTimestamp = partition.getStartTimestamp();
        String lastBoundaryRecordSequence = partition.getLastBoundaryRecordSequence();
        boolean isPartitionEnded = false;
        boolean isPartitionMoveInEvent = false;
        PartitionEventEvent moveInEvent = null;
        // Wall-clock time of the last onWindowAdvanced call made inside the inner event
        // loop (as opposed to the outer window-boundary call at line 256). Used to
        // throttle sync-topic writes to at most one per heartbeatMillis interval.
        long lastWindowAdvancedWallMs = 0L;

        while (!isPartitionEnded && !isPartitionMoveInEvent
                && (partitionEndTimestamp == null || isBeforeOrEqual(processedTimestamp, partitionEndTimestamp))) {
            Timestamp endTimestamp = partitionEndTimestamp == null
                    ? addMinutes(processedTimestamp, windowDuration)
                    : minTimestamp(partitionEndTimestamp, addMinutes(processedTimestamp, windowDuration));
            String newBoundaryRecordSequence = null;

            try (ChangeStreamResultSet resultSet = changeStreamDao.streamQuery(token, processedTimestamp,
                    endTimestamp, heartbeatMillis.toMillis())) {

                long start = now();
                while (resultSet.next()) {
                    long delay = now() - start;

                    List<ChangeStreamEvent> rawEvents = changeStreamRecordMapper.toChangeStreamEvents(
                            partition,
                            resultSet, resultSet.getMetadata());
                    LOGGER.debug("Task: {}, Events receive from mutable stream: {}", taskUid, rawEvents);

                    List<ChangeStreamEvent> events = filterBoundaryDuplicates(rawEvents, processedTimestamp, lastBoundaryRecordSequence);

                    if (!events.isEmpty() && (events.get(0) instanceof HeartbeatEvent)) {
                        var heartbeatEvent = (HeartbeatEvent) events.get(0);
                        long heartbeatLag = System.currentTimeMillis() - heartbeatEvent.getRecordTimestamp().toSqlTimestamp().toInstant().toEpochMilli();
                        if (heartbeatLag > 60_000) {
                            LOGGER.warn("Task: {}, heartbeat has very old timestamp, lag: {}, token: {}, event: {}", taskUid, heartbeatLag,
                                    heartbeatEvent.getMetadata().getPartitionToken(),
                                    heartbeatEvent);
                        }
                    }

                    for (ChangeStreamEvent event : events) {
                        if (endTimestamp.equals(event.getRecordTimestamp()) && event.getRecordSequence() != null) {
                            newBoundaryRecordSequence = event.getRecordSequence();
                        }
                    }

                    processEvents(partition, events, changeStreamEventConsumer);

                    // Advance processedTimestamp in the sync context on every event batch,
                    // throttled to at most one call per heartbeatMillis, so that downstream
                    // CREATED partitions can use the processedTimestamp fallback in
                    // sourceHasResumedThisMove within heartbeatMillis rather than waiting for
                    // the full window duration.
                    //
                    // The previous version of this block fired only on HeartbeatEvent. Spanner
                    // change streams only emit heartbeats during idle periods, so in
                    // high-throughput partitions (continuous data records, no idle gaps)
                    // heartbeats never arrived and processedTimestamp advanced only at the
                    // 5-minute window boundary. This produced a matching sawtooth in the
                    // low-watermark lag metric: all downstream CREATED partitions stayed
                    // blocked for a full window before the fallback could unblock them.
                    //
                    // The outer loop's local `processedTimestamp` variable is intentionally NOT
                    // modified here — window-boundary tracking is unaffected.
                    if (!events.isEmpty()) {
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastWindowAdvancedWallMs >= heartbeatMillis.toMillis()) {
                            Timestamp latestEventTs = events.get(events.size() - 1).getRecordTimestamp();
                            if (latestEventTs != null) {
                                partitionEventListener.onWindowAdvanced(
                                        partition, latestEventTs, lastBoundaryRecordSequence);
                                lastWindowAdvancedWallMs = nowMs;
                            }
                        }
                    }

                    for (ChangeStreamEvent event : events) {
                        if (event instanceof PartitionEndEvent) {
                            isPartitionEnded = true;
                        }
                        if (event instanceof PartitionEventEvent && mutablePartitionOrderingEnabled) {
                            PartitionEventEvent partitionEventEvent = (PartitionEventEvent) event;
                            if (!partitionEventEvent.getSourcePartitions().isEmpty()) {
                                isPartitionMoveInEvent = true;
                                moveInEvent = partitionEventEvent;
                            }
                        }
                    }

                    if (!events.isEmpty() && !(events.get(0) instanceof HeartbeatEvent)) {
                        metricsEventPublisher.publishMetricEvent(new DelayChangeStreamEventsMetricEvent((int) delay));
                    }

                    if (isPartitionMoveInEvent) {
                        break;
                    }

                    start = now();
                }
            }
            catch (InterruptedException ex) {
                LOGGER.info("task {}, Interrupting streaming mutable partition task with token {}", this.taskUid, partition.getToken());
                Thread.currentThread().interrupt();
                break;
            }

            if (isPartitionMoveInEvent) {
                break;
            }

            if (partitionEndTimestamp != null && processedTimestamp.equals(partitionEndTimestamp)) {
                isPartitionEnded = true;
            }
            if (InitialPartition.isInitialPartition(token)) {
                isPartitionEnded = true;
            }

            if (newBoundaryRecordSequence != null) {
                lastBoundaryRecordSequence = newBoundaryRecordSequence;
            }
            processedTimestamp = endTimestamp;
            partitionEventListener.onWindowAdvanced(partition, processedTimestamp, lastBoundaryRecordSequence);
        }

        if (isPartitionMoveInEvent && moveInEvent != null) {
            LOGGER.info("Task {}, Pausing mutable partition {} after MoveIn event at {}, seq {}, sources {}",
                    taskUid, partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(), moveInEvent.getSourcePartitions());

            // Advance processedTimestamp to the MoveIn commit timestamp before pausing.
            // When this partition's window loop exits early due to a MoveIn event, the normal
            // onWindowAdvanced call at the bottom of the outer loop is never reached, leaving
            // processedTimestamp frozen at the previous window boundary in the sync context.
            // Downstream partitions in CREATED state rely on the processedTimestamp fallback
            // in sourceHasResumedThisMove to unblock when the MoveOutState is missing (e.g.
            // after a crash). Without this call they stall for a full window duration per chain
            // link. The MoveIn commit timestamp is always >= the split timestamp that created
            // those downstream partitions, so publishing it here satisfies their wait condition
            // immediately, collapsing the N×windowDuration chain delay to near zero.
            partitionEventListener.onWindowAdvanced(partition, moveInEvent.getCommitTimestamp(), lastBoundaryRecordSequence);

            partitionEventListener.onMoveIn(partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(), moveInEvent.getSourcePartitions());
            return;
        }

        partitionEventListener.onFinish(partition);
        LOGGER.info("Task {}, Finished consuming mutable partition {}", taskUid, partition);

        changeStreamEventConsumer.acceptChangeStreamEvent(new FinishPartitionEvent(partition));
    }

    private List<ChangeStreamEvent> filterBoundaryDuplicates(
                                                             List<ChangeStreamEvent> events,
                                                             Timestamp windowStart,
                                                             String lastBoundaryRecordSequence) {
        if (lastBoundaryRecordSequence == null) {
            return events;
        }
        List<ChangeStreamEvent> filtered = new ArrayList<>();
        for (ChangeStreamEvent event : events) {
            if (isBeforeOrEqual(event.getRecordTimestamp(), windowStart)
                    && event.getRecordSequence() != null
                    && RecordSequenceUtils.compare(event.getRecordSequence(), lastBoundaryRecordSequence) <= 0) {
                LOGGER.debug("Task: {}, Skipping boundary duplicate event at {} seq {}",
                        taskUid, windowStart, event.getRecordSequence());
                continue;
            }
            filtered.add(event);
        }
        return filtered;
    }

    private long now() {
        return Instant.now().toEpochMilli();
    }

    private Timestamp addMinutes(Timestamp timestamp, Duration duration) {
        Instant result = Instant.ofEpochSecond(
                timestamp.getSeconds(),
                timestamp.getNanos()).plus(duration);

        return Timestamp.ofTimeSecondsAndNanos(result.getEpochSecond(), result.getNano());
    }

    private Timestamp minTimestamp(Timestamp a, Timestamp b) {
        int cmp = Long.compare(a.getSeconds(), b.getSeconds());
        if (cmp == 0) {
            cmp = Integer.compare(a.getNanos(), b.getNanos());
        }
        return cmp <= 0 ? a : b;
    }

    private boolean isBeforeOrEqual(Timestamp a, Timestamp b) {
        int cmp = Long.compare(a.getSeconds(), b.getSeconds());
        if (cmp == 0) {
            cmp = Integer.compare(a.getNanos(), b.getNanos());
        }
        return cmp <= 0;
    }

    private void processEvents(Partition partition, List<ChangeStreamEvent> events,
                               ChangeStreamEventConsumer changeStreamEventConsumer)
            throws InterruptedException {
        for (final ChangeStreamEvent changeStreamEvent : events) {
            if (changeStreamEvent instanceof ChildPartitionsEvent) {
                ChildPartitionsEvent childPartitionsEvent = (ChildPartitionsEvent) changeStreamEvent;
                LOGGER.info("Task: {}, Received child partition from partition {}:{}", taskUid, partition.getToken(), childPartitionsEvent);
            }
            LOGGER.debug("Task: {}, Received record from partition {}: {}", taskUid, partition.getToken(), changeStreamEvent);

            changeStreamEventConsumer.acceptChangeStreamEvent(changeStreamEvent);
        }
    }

}
