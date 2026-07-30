/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.demo;

import java.util.List;

/**
 * Single source of truth for the MUTABLE_KEY_RANGE demo's "orders" scenario, shared by
 * {@link DemoDataGenerator}, {@link DemoKafkaConnectSetup}, and {@link DemoConsumerValidator}
 * so the three processes cannot silently drift apart on names or the expected event sequence.
 *
 * <p>Narrative: two orders come in, the first ships, a key-range split happens transparently
 * mid-stream (the actual MUTABLE_KEY_RANGE mechanic being demonstrated), the second order
 * ships too - proving the connector kept working correctly across the split - then the first
 * order is removed. Order ids are 100/200 (not 1/2) so the forced split point (150) falls
 * strictly between two real keys, matching the only pattern confirmed to work against real
 * Spanner ({@code MutableKeyRangeIT.shouldPreserveOrderAcrossForcedKeyRangeSplit}) rather than
 * splitting exactly at an existing row's key, which is untested territory.
 */
public final class MutableKeyRangeDemoScenario {

    public static final String DEFAULT_DATABASE_ID = "mutable_key_range_demo";
    public static final String TABLE_NAME = "orders";
    public static final String CHANGE_STREAM_NAME = "ordersChangeStream";
    public static final String CONNECTOR_NAME = "orders-demo-connector";

    public static final long ORDER_ID_1 = 100L;
    public static final long ORDER_ID_2 = 200L;
    public static final String SPLIT_KEY = "150";

    private MutableKeyRangeDemoScenario() {
    }

    public static String databaseId() {
        return System.getProperty("demo.database.id", DEFAULT_DATABASE_ID);
    }

    /** Matches AbstractSpannerConnectorIT#getTopicName's real-connect-mode formula. */
    public static String topicName() {
        return CONNECTOR_NAME + "." + TABLE_NAME;
    }

    public enum Op {
        INSERT("c"),
        UPDATE("u"),
        DELETE("d"),
        TOMBSTONE(null);

        public final String code;

        Op(String code) {
            this.code = code;
        }
    }

    public record ExpectedEvent(Op op, String statusAfter) {
    }

    public static final List<ExpectedEvent> EXPECTED_FOR_ORDER_1 = List.of(
            new ExpectedEvent(Op.INSERT, "pending"),
            new ExpectedEvent(Op.UPDATE, "shipped"),
            new ExpectedEvent(Op.DELETE, null),
            new ExpectedEvent(Op.TOMBSTONE, null));

    public static final List<ExpectedEvent> EXPECTED_FOR_ORDER_2 = List.of(
            new ExpectedEvent(Op.INSERT, "pending"),
            new ExpectedEvent(Op.UPDATE, "shipped"));

    public static final int TOTAL_EXPECTED_RECORDS = EXPECTED_FOR_ORDER_1.size() + EXPECTED_FOR_ORDER_2.size();
}
