/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.demo;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Waits for the connector's own internal task-rebalance/partition-assignment protocol (a
 * separate, connector-specific Kafka consumer group) to settle, by polling {@code docker logs}
 * for the most recently mentioned task UID and requiring it to stay unchanged for
 * {@link #STABLE_WINDOW}.
 *
 * <p>Kafka Connect reporting a task {@code RUNNING} only means its {@code start()} method
 * returned - it says nothing about this internal protocol, which isn't visible through the
 * Connect REST status API at all. Right after a fresh deploy, that internal protocol can cycle
 * through a brand-new task UID roughly every 15 seconds for well over two minutes before
 * settling down - and any DML issued while it's still cycling gets silently missed rather than
 * queued or replayed later.
 *
 * <p><b>Call this from every process that's about to depend on the connector actually being
 * stable, not just once after deploy.</b> A rebalance can occur after {@code deploy}'s own wait
 * already declared things settled - confirmed directly: a fresh task UID appeared and a
 * "Rebalance finished" event fired right as a separate, later process started issuing DML, and
 * every one of those events was silently lost. {@code deploy} returning successfully only means
 * things looked stable at that moment in that process; it says nothing about what happens after
 * it exits. {@link DemoKafkaConnectSetup} and {@link DemoDataGenerator} both call this
 * independently for this reason, rather than one trusting the other's earlier check.
 */
final class ConnectorTaskStabilityWaiter {

    private static final Duration STABLE_WINDOW = Duration.ofSeconds(20);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(5);
    private static final Duration MAX_WAIT = Duration.ofMinutes(10);
    // Used only if docker logs isn't available at all (e.g. a remote/GKE-hosted worker - see
    // "Pointing at a different Kafka cluster" in demo/README.md) and the real check can't run.
    private static final Duration FALLBACK_SLEEP = Duration.ofSeconds(90);

    private ConnectorTaskStabilityWaiter() {
    }

    static String containerName() {
        return System.getProperty("demo.connect.container.name", "kafka-connect-worker");
    }

    /**
     * Blocks until the connector's task assignment has looked stable for {@link #STABLE_WINDOW},
     * or throws after {@link #MAX_WAIT} if it never does. Falls back to a fixed
     * {@link #FALLBACK_SLEEP} (with a warning) if {@code docker logs} isn't usable at all - that
     * fallback carries the same unreliability this class exists to avoid; it's only there so a
     * remote/non-Docker Kafka Connect worker doesn't break outright.
     */
    static void waitForStable(String connectorName) {
        String containerName = containerName();
        Pattern taskUidPattern = Pattern.compile(Pattern.quote(connectorName) + "_task-\\d+_[0-9a-fA-F-]{36}");

        System.out.println("Waiting for the connector's internal task assignment to settle "
                + "(polling docker logs " + containerName + ")...");

        String lastSeenTaskUid;
        try {
            lastSeenTaskUid = latestTaskUid(containerName, taskUidPattern);
        }
        catch (IOException e) {
            System.out.println("Could not read docker logs for " + containerName + " (" + e.getMessage()
                    + ") - falling back to a fixed " + FALLBACK_SLEEP.toSeconds() + "s sleep. This is "
                    + "expected if the connect worker isn't a local Docker container; otherwise check that "
                    + "docker is on PATH and the container name is correct (demo.connect.container.name).");
            sleepQuietly(FALLBACK_SLEEP);
            return;
        }

        Instant lastChangeTime = Instant.now();
        Instant deadline = lastChangeTime.plus(MAX_WAIT);
        while (true) {
            Instant now = Instant.now();
            if (lastSeenTaskUid != null && now.isAfter(lastChangeTime.plus(STABLE_WINDOW))) {
                System.out.println("Task assignment settled on " + lastSeenTaskUid);
                return;
            }
            if (now.isAfter(deadline)) {
                throw new IllegalStateException("Timed out after " + MAX_WAIT.toMinutes()
                        + " minutes waiting for the connector's internal task assignment to settle "
                        + "(last task UID seen: " + lastSeenTaskUid + "). Check 'docker logs "
                        + containerName + "' directly.");
            }
            sleepQuietly(POLL_INTERVAL);
            String currentTaskUid;
            try {
                currentTaskUid = latestTaskUid(containerName, taskUidPattern);
            }
            catch (IOException e) {
                throw new RuntimeException("Lost the ability to read docker logs for " + containerName
                        + " mid-wait", e);
            }
            if (currentTaskUid != null && !currentTaskUid.equals(lastSeenTaskUid)) {
                lastSeenTaskUid = currentTaskUid;
                lastChangeTime = now;
            }
        }
    }

    /**
     * The most recent task UID (e.g. {@code orders-demo-connector_task-0_<uuid>}) mentioned in
     * the container's recent log output, or {@code null} if none has appeared yet. Only looks at
     * a bounded tail rather than the full log, since all we need is the latest mention.
     */
    private static String latestTaskUid(String containerName, Pattern taskUidPattern) throws IOException {
        Process process = new ProcessBuilder("docker", "logs", "--tail", "500", containerName)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        try {
            process.waitFor();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while reading docker logs for " + containerName, e);
        }
        if (process.exitValue() != 0) {
            throw new IOException("docker logs " + containerName + " exited " + process.exitValue() + ": " + output);
        }
        String last = null;
        Matcher matcher = taskUidPattern.matcher(output);
        while (matcher.find()) {
            last = matcher.group();
        }
        return last;
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for connector to settle", e);
        }
    }
}
