/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.demo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.spanner.demo.MutableKeyRangeDemoScenario.ExpectedEvent;
import io.debezium.connector.spanner.demo.MutableKeyRangeDemoScenario.Op;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaConnectRestClient;
import io.debezium.connector.spanner.util.RealModeRecordPoller;

/**
 * Standalone consumer for the MUTABLE_KEY_RANGE demo: consumes the "orders" topic, validates
 * the received events against {@link MutableKeyRangeDemoScenario}'s expected sequence, prints
 * a PASS/FAIL summary, then cleans up the connector and every Spanner object the demo created -
 * always, regardless of validation outcome.
 */
public final class DemoConsumerValidator {

    private DemoConsumerValidator() {
    }

    public static void main(String[] args) throws Exception {
        // This demo only ever targets real Cloud Spanner - set explicitly rather than relying
        // on callers to remember -Ddebezium.test.spanner.mode=real, since cleanup() below
        // needs a real-Spanner-mode Connection to drop the demo's database correctly. Also
        // default the project/instance here (Database's own fallback is
        // test-project/test-instance, not this demo's instance) so cleanup doesn't silently
        // target the wrong database if callers forget to pass -D flags to this specific step.
        System.setProperty("debezium.test.spanner.mode", "real");
        System.setProperty("gcp.spanner.project.id", System.getProperty("gcp.spanner.project.id", "improvingvancouver"));
        System.setProperty("gcp.spanner.instance.id", System.getProperty("gcp.spanner.instance.id", "spanner-kafka-connector"));

        List<SourceRecord> records = consumeRecords();
        boolean passed = validate(records);
        cleanup();

        if (!passed) {
            throw new AssertionError("Demo validation FAILED - see output above");
        }
    }

    private static List<SourceRecord> consumeRecords() throws InterruptedException {
        String bootstrapServers = System.getProperty("demo.kafka.bootstrap.servers.host", "localhost:9092");
        Pattern topicPattern = Pattern.compile("^" + Pattern.quote(MutableKeyRangeDemoScenario.topicName()) + "$");
        BlockingQueue<SourceRecord> queue = new ArrayBlockingQueue<>(64);
        RealModeRecordPoller poller = new RealModeRecordPoller(bootstrapServers, topicPattern, queue);
        poller.start();

        int timeoutSeconds = Integer.parseInt(System.getProperty("demo.validate.timeout.seconds", "120"));
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(timeoutSeconds).toMillis();
        List<SourceRecord> records = new ArrayList<>();
        try {
            while (records.size() < MutableKeyRangeDemoScenario.TOTAL_EXPECTED_RECORDS && System.currentTimeMillis() < deadline) {
                SourceRecord record = queue.poll(1, TimeUnit.SECONDS);
                if (record != null) {
                    records.add(record);
                }
            }
        }
        finally {
            poller.stop();
        }
        System.out.println("Consumed " + records.size() + " record(s) from " + MutableKeyRangeDemoScenario.topicName());
        return records;
    }

    private static boolean validate(List<SourceRecord> records) {
        Map<Long, List<ExpectedEvent>> actualById = new HashMap<>();
        for (SourceRecord record : records) {
            long id;
            ExpectedEvent event;
            if (record.value() == null) {
                Struct key = (Struct) record.key();
                id = key.getInt64("id");
                event = new ExpectedEvent(Op.TOMBSTONE, null);
            }
            else {
                Struct value = (Struct) record.value();
                String opCode = value.getString("op");
                boolean isDelete = "d".equals(opCode);
                Struct afterOrBefore = isDelete ? value.getStruct("before") : value.getStruct("after");
                id = afterOrBefore.getInt64("id");
                String status = isDelete ? null : afterOrBefore.getString("status");
                event = new ExpectedEvent(opCodeToOp(opCode), status);
            }
            actualById.computeIfAbsent(id, k -> new ArrayList<>()).add(event);
        }

        boolean order1Ok = checkOrder(MutableKeyRangeDemoScenario.ORDER_ID_1, MutableKeyRangeDemoScenario.EXPECTED_FOR_ORDER_1,
                actualById.getOrDefault(MutableKeyRangeDemoScenario.ORDER_ID_1, List.of()));
        boolean order2Ok = checkOrder(MutableKeyRangeDemoScenario.ORDER_ID_2, MutableKeyRangeDemoScenario.EXPECTED_FOR_ORDER_2,
                actualById.getOrDefault(MutableKeyRangeDemoScenario.ORDER_ID_2, List.of()));

        boolean countOk = records.size() == MutableKeyRangeDemoScenario.TOTAL_EXPECTED_RECORDS;
        if (!countOk) {
            System.out.println("[FAIL] Expected exactly " + MutableKeyRangeDemoScenario.TOTAL_EXPECTED_RECORDS
                    + " records total, got " + records.size());
        }

        boolean passed = order1Ok && order2Ok && countOk;
        System.out.println();
        System.out.println("RESULT: " + (passed ? "PASS" : "FAIL"));
        return passed;
    }

    private static boolean checkOrder(long orderId, List<ExpectedEvent> expected, List<ExpectedEvent> actual) {
        boolean ok = expected.equals(actual);
        int lineCount = Math.max(expected.size(), actual.size());
        for (int i = 0; i < lineCount; i++) {
            ExpectedEvent expectedEvent = i < expected.size() ? expected.get(i) : null;
            ExpectedEvent actualEvent = i < actual.size() ? actual.get(i) : null;
            boolean lineOk = Objects.equals(expectedEvent, actualEvent);
            System.out.println((lineOk ? "[PASS] " : "[FAIL] ") + "order " + orderId + " event " + i
                    + ": expected=" + expectedEvent + " actual=" + actualEvent);
        }
        return ok;
    }

    private static Op opCodeToOp(String code) {
        for (Op op : Op.values()) {
            if (op.code != null && op.code.equals(code)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown op code: " + code);
    }

    private static void cleanup() {
        System.out.println();
        System.out.println("Cleaning up...");

        // Connector first: nothing should still be reading the change stream while the
        // underlying Spanner objects are being dropped.
        try {
            KafkaConnectRestClient client = new KafkaConnectRestClient(
                    System.getProperty("demo.kafka.connect.rest.url", "http://localhost:8083"));
            client.deleteConnector(MutableKeyRangeDemoScenario.CONNECTOR_NAME);
            client.waitForConnectorAbsent(MutableKeyRangeDemoScenario.CONNECTOR_NAME, Duration.ofMinutes(3));
            System.out.println("Connector " + MutableKeyRangeDemoScenario.CONNECTOR_NAME + " removed");
        }
        catch (Exception e) {
            System.out.println("Failed to remove connector (continuing cleanup anyway): " + e.getMessage());
        }

        try {
            Connection connection = Database.builder()
                    .databaseId(MutableKeyRangeDemoScenario.databaseId())
                    .build()
                    .getPersistentConnection();
            connection.dropChangeStream(MutableKeyRangeDemoScenario.CHANGE_STREAM_NAME);
            connection.dropTable(MutableKeyRangeDemoScenario.TABLE_NAME);
            connection.dropDatabase(MutableKeyRangeDemoScenario.databaseId());
            System.out.println("Dropped change stream, table, and database " + MutableKeyRangeDemoScenario.databaseId());
        }
        catch (Exception e) {
            System.out.println("Failed to clean up Spanner objects: " + e.getMessage());
        }
    }
}
