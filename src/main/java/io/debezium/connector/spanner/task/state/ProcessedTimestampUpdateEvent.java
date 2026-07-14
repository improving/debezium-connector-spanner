/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import com.google.cloud.Timestamp;

/**
 * Event fired at the end of each mutable-stream sliding window to persist the
 * window boundary timestamp in {@link io.debezium.connector.spanner.kafka.internal.model.PartitionState}.
 */
public class ProcessedTimestampUpdateEvent implements TaskStateChangeEvent {

    private final String token;
    private final Timestamp processedTimestamp;

    public ProcessedTimestampUpdateEvent(String token, Timestamp processedTimestamp) {
        this.token = token;
        this.processedTimestamp = processedTimestamp;
    }

    public String getToken() {
        return token;
    }

    public Timestamp getProcessedTimestamp() {
        return processedTimestamp;
    }
}
