/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SpannerPartitionTest {

    @Test
    void testConstructor() {
        SpannerPartition actualSpannerPartition = new SpannerPartition("test");
        assertEquals("test", actualSpannerPartition.getValue());
        assertNull(actualSpannerPartition.getTvfName());
        assertEquals("SpannerPartition[{partitionToken=test}]", actualSpannerPartition.toString());
    }

    @Test
    void testConstructorWithTvfName() {
        SpannerPartition actualSpannerPartition = new SpannerPartition("test", "tvfA");
        assertEquals("test", actualSpannerPartition.getValue());
        assertEquals("tvfA", actualSpannerPartition.getTvfName());
        assertEquals("SpannerPartition[{partitionToken=test, tvfName=tvfA}]", actualSpannerPartition.toString());
    }

    @Test
    void testGetSourcePartition() {
        Map<String, String> actualSourcePartition = SpannerPartition.getInitialSpannerPartition().getSourcePartition();
        assertEquals(1, actualSourcePartition.size());
        assertEquals("Parent0", actualSourcePartition.get("partitionToken"));
    }

    @Test
    void testGetSourcePartitionWithTvfName() {
        Map<String, String> actualSourcePartition = new SpannerPartition("token", "tvfA").getSourcePartition();
        assertEquals(2, actualSourcePartition.size());
        assertEquals("token", actualSourcePartition.get("partitionToken"));
        assertEquals("tvfA", actualSourcePartition.get("tvfName"));
    }

    @Test
    void testExtractToken() {
        assertNull(SpannerPartition.extractToken(new HashMap<>()));
    }

    @Test
    void testExtractTvfName() {
        assertNull(SpannerPartition.extractTvfName(Map.of("partitionToken", "token")));
        assertEquals("tvfA", SpannerPartition.extractTvfName(Map.of("partitionToken", "token", "tvfName", "tvfA")));
    }

    @Test
    void testGetInitialSpannerPartition() {
        assertEquals("Parent0", SpannerPartition.getInitialSpannerPartition().getValue());
    }

    @Test
    void partitionsWithSameTokenButDifferentTvfAreNotEqual() {
        SpannerPartition a = new SpannerPartition("token", "tvfA");
        SpannerPartition b = new SpannerPartition("token", "tvfB");
        SpannerPartition legacyA = new SpannerPartition("token");
        SpannerPartition legacyB = new SpannerPartition("token", null);

        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());
        assertEquals(legacyA, legacyB);
        assertEquals(legacyA.hashCode(), legacyB.hashCode());
    }
}
