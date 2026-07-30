/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.demo;

import java.time.Duration;

import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;

/**
 * Standalone demo data generator: creates the "orders" table and its MUTABLE_KEY_RANGE change
 * stream ({@code --phase=schema}), or runs the scenario's deterministic DML sequence including
 * a forced key-range split ({@code --phase=events}). Run as a separate process from
 * {@link DemoKafkaConnectSetup} so the connector can be deployed and confirmed running between
 * the two phases - see demo/README.md for the full run order.
 */
public final class DemoDataGenerator {

    private DemoDataGenerator() {
    }

    public static void main(String[] args) throws Exception {
        String phase = argValue(args, "--phase");
        if (phase == null) {
            throw new IllegalArgumentException("Usage: DemoDataGenerator --phase=schema|events");
        }

        // This demo only ever targets real Cloud Spanner - set explicitly rather than relying
        // on callers to remember -Ddebezium.test.spanner.mode=real. Also default the
        // project/instance here (Database's own fallback is test-project/test-instance, not
        // this demo's instance) so this class behaves the same as DemoKafkaConnectSetup, whose
        // config defaults to the same two values, whether or not callers pass -D flags.
        System.setProperty("debezium.test.spanner.mode", "real");
        System.setProperty("gcp.spanner.project.id", System.getProperty("gcp.spanner.project.id", "improvingvancouver"));
        System.setProperty("gcp.spanner.instance.id", System.getProperty("gcp.spanner.instance.id", "spanner-kafka-connector"));

        Connection connection = Database.builder()
                .databaseId(MutableKeyRangeDemoScenario.databaseId())
                .build()
                .getPersistentConnection();

        switch (phase) {
            case "schema" -> runSchemaPhase(connection);
            case "events" -> runEventsPhase(connection);
            default -> throw new IllegalArgumentException("Unknown phase: " + phase);
        }
    }

    private static void runSchemaPhase(Connection connection) throws Exception {
        String table = MutableKeyRangeDemoScenario.TABLE_NAME;
        String changeStream = MutableKeyRangeDemoScenario.CHANGE_STREAM_NAME;

        // Idempotent: a re-run after an aborted prior attempt must not fail with "already
        // exists", and must not leave leftover rows from a previous run behind to corrupt this
        // run's expected event count.
        if (connection.changeStreamExists(changeStream)) {
            System.out.println("Change stream " + changeStream + " already exists, dropping it first");
            connection.dropChangeStream(changeStream);
        }
        if (connection.tableExists(table)) {
            System.out.println("Table " + table + " already exists, dropping it first");
            connection.dropTable(table);
        }

        connection.createTable(table + "(id INT64, customer STRING(100), status STRING(50)) PRIMARY KEY(id)");
        connection.createMutableKeyRangeChangeStream(changeStream, table);
        System.out.println("Created table " + table + " and change stream " + changeStream);
    }

    private static void runEventsPhase(Connection connection) throws Exception {
        String table = MutableKeyRangeDemoScenario.TABLE_NAME;
        long id1 = MutableKeyRangeDemoScenario.ORDER_ID_1;
        long id2 = MutableKeyRangeDemoScenario.ORDER_ID_2;

        // demo-connect-deploy already waits for this same signal, a fresh rebalance can occur
        // after that process already exited, right as this one starts issuing DML, silently
        // losing every event that follows. Re-checking independently here, immediately before
        // the first statement, is what actually closes that gap.
        ConnectorTaskStabilityWaiter.waitForStable(MutableKeyRangeDemoScenario.CONNECTOR_NAME);

        connection.executeUpdate(
                "INSERT INTO " + table + " (id, customer, status) VALUES (" + id1 + ", 'Alice', 'pending')");
        System.out.println("Inserted order " + id1 + " (Alice, pending)");

        connection.executeUpdate(
                "INSERT INTO " + table + " (id, customer, status) VALUES (" + id2 + ", 'Bob', 'pending')");
        System.out.println("Inserted order " + id2 + " (Bob, pending)");

        connection.executeUpdate("UPDATE " + table + " SET status = 'shipped' WHERE id = " + id1);
        System.out.println("Order " + id1 + " shipped");

        // The actual MUTABLE_KEY_RANGE mechanic being demonstrated: a live key-range split,
        // transparent to any downstream consumer, in between the two orders' keys. Short
        // expiry: split points count against a small, instance-wide quota on the shared
        // real-Spanner test instance, and this demo only needs the split for a couple minutes.
        connection.forceSplit(table, Duration.ofMinutes(10), MutableKeyRangeDemoScenario.SPLIT_KEY);
        System.out.println("Forced a key-range split at " + MutableKeyRangeDemoScenario.SPLIT_KEY);

        connection.executeUpdate("UPDATE " + table + " SET status = 'shipped' WHERE id = " + id2);
        System.out.println("Order " + id2 + " shipped (after the split)");

        connection.executeUpdate("DELETE FROM " + table + " WHERE id = " + id1);
        System.out.println("Order " + id1 + " deleted");
    }

    private static String argValue(String[] args, String name) {
        String prefix = name + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) {
                return arg.substring(prefix.length());
            }
        }
        return null;
    }
}
