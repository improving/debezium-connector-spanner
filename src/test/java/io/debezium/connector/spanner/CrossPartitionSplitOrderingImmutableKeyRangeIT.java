/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.util.PartitionMode;

/**
 * See {@link CrossPartitionSplitOrderingTestBase} for why this is a separate class rather than
 * one parameterized invocation alongside {@link CrossPartitionSplitOrderingMutableKeyRangeIT}.
 */
public class CrossPartitionSplitOrderingImmutableKeyRangeIT extends CrossPartitionSplitOrderingTestBase {

    @Test
    public void shouldDeliverFollowUpWriteExactlyOnceAndInOrderAcrossBackgroundPartitionSplits()
            throws InterruptedException, ExecutionException {
        runTest(PartitionMode.IMMUTABLE_KEY_RANGE);
    }
}
