# MUTABLE_KEY_RANGE change stream demo

A fully reproducible, end-to-end demo of Cloud Spanner `MUTABLE_KEY_RANGE` change streams
flowing through a real Kafka Connect worker to a validating consumer. Two orders come in, the
first ships, a live key-range split happens transparently mid-stream (the actual
`MUTABLE_KEY_RANGE` mechanic being demonstrated), the second order ships too - proving the
connector kept working correctly across the split - then the first order is removed. The
consumer verifies all 6 resulting Kafka records (2 inserts, 2 updates, 1 delete, 1 tombstone)
arrived correctly and in order, then cleans everything up.

The whole scenario is defined once, in
[`MutableKeyRangeDemoScenario.java`](../src/test/java/io/debezium/connector/spanner/demo/MutableKeyRangeDemoScenario.java) -
the other three classes below all read from it, so they can't silently drift apart.

## Prerequisites

1. **Real Cloud Spanner access.** Follow [`doc/real-spanner-testing.md`](../doc/real-spanner-testing.md)
   to provision/confirm access to the instance and set up Application Default Credentials via
   service-account impersonation. This demo always targets real Spanner - there's no
   emulator/Omni path.
2. **Docker running locally**, with the connector plugin image built. This needs two separate
   `mvn` invocations, not one - `docker:build` runs before `assembly:single` within the same
   `package` phase as currently wired, so the tarball the Dockerfile needs doesn't exist yet if
   you try to do both in a single command:
   ```bash
   # Pass 1: produce the assembly tarball (the `assembly` profile is not active by default)
   mvn package -Passembly -DskipTests

   # Pass 2: build the image from that tarball. This will also attempt to push to a registry
   # and fail doing so (`docker.skip.push` gates both build and push) - that's expected and
   # harmless, the image is already built and available locally by that point.
   mvn package -DskipTests -Ddocker.skip.push=false
   ```
   Confirm with `docker images kafka-spanner-connector`. Re-run both passes after any connector
   source change - the demo's Kafka Connect worker runs whatever image was last built.
3. From the repo root, confirm `gcloud auth application-default print-access-token` succeeds
   before running anything below.

## One-shot run

```bash
demo/run-demo.sh
```

This starts the local Kafka broker and a real Kafka Connect worker, creates the demo schema,
deploys the connector, runs the order sequence (including the forced split), consumes and
validates the results, prints `RESULT: PASS` or `RESULT: FAIL`, and tears down both the
containers and the Spanner objects it created. A non-zero exit code means either a step failed
outright or validation failed - scroll up in the output for which.

By default it targets project `improvingvancouver`, instance `spanner-kafka-connector`.
Override with:
```bash
GCP_SPANNER_PROJECT_ID=my-project GCP_SPANNER_INSTANCE_ID=my-instance demo/run-demo.sh
```

If validation reports 0 records consumed, see "Why `demo-connect-deploy` *and* `demo-events`
both wait before proceeding" below - both steps poll for an actual readiness signal rather than
guessing at a fixed delay, so this should be rare; if it still happens, it's worth
investigating rather than just retrying.

## Running steps individually

Useful for inspecting what's happening at each stage, or re-running just one step.

```bash
# 1. Start Kafka + a real Kafka Connect worker
demo/start-kafka-connect.sh

# 2. Create the "orders" table and its MUTABLE_KEY_RANGE change stream (idempotent - safe to
#    re-run after an aborted attempt; drops and recreates if they already exist)
mvn -Pdemo exec:java@demo-schema

# 3. Deploy the connector and wait for it to report RUNNING
mvn -Pdemo exec:java@demo-connect-deploy

# 4. Run the deterministic order sequence, including the forced key-range split
mvn -Pdemo exec:java@demo-events

# 5. Consume, validate, and clean up (connector, change stream, table, database)
mvn -Pdemo exec:java@demo-validate

# 6. Stop Kafka Connect + the broker
demo/stop-kafka-connect.sh
```

All three Java steps default `gcp.spanner.project.id`/`gcp.spanner.instance.id` to
`improvingvancouver`/`spanner-kafka-connector` consistently - only pass
`-Dgcp.spanner.project.id=...`/`-Dgcp.spanner.instance.id=...` if pointing at a different
project or instance. **Pass the same values to every step in a given run**, including
`demo-validate` - its cleanup step drops the database on whichever project/instance it's told,
so mismatched values across steps will leave the real objects behind uncleaned while it
reports success against nothing.

## Restarting or recreating the connector

No need to touch Spanner or Kafka Connect's broker - just redeploy the connector itself:

```bash
mvn -Pdemo exec:java@demo-connect-delete
mvn -Pdemo exec:java@demo-connect-deploy
```

This is also the right move after changing the connector's config (edit
[`DemoKafkaConnectSetup.java`](../src/test/java/io/debezium/connector/spanner/demo/DemoKafkaConnectSetup.java)'s
`deploy()` method, rebuild, then run these two).

## Pointing at a different Kafka cluster (e.g. GKE)

Every networking value is an overridable system property, not hardcoded - swapping targets is
a matter of different `-D` flags, not code changes:

| Property | Default | Used by |
|---|---|---|
| `demo.kafka.connect.rest.url` | `http://localhost:8083` | `demo-connect-deploy`, `demo-connect-delete`, `demo-validate` |
| `demo.kafka.bootstrap.servers.host` | `localhost:9092` | `demo-validate` (the consumer, running on the host) |
| `demo.kafka.bootstrap.servers.container` | `broker:29092` | `demo-connect-deploy` (what the *connector itself* uses, from inside whatever network the Kafka Connect worker runs on) |
| `demo.connect.container.name` | `kafka-connect-worker` | `demo-connect-deploy` and `demo-events` - the local Docker container to poll `docker logs` against while waiting for task assignment to settle (see below). When running via `demo/run-demo.sh` rather than the individual steps, set the `DEMO_CONNECT_CONTAINER_NAME` environment variable instead - the script forwards it as this property, and also uses it as the container name it creates. |

### Why `demo-connect-deploy` *and* `demo-events` both wait before proceeding

Kafka Connect reporting a task `RUNNING` only means its `start()` method returned - it says
nothing about the connector's own internal task-rebalance/partition-assignment protocol (a
separate, connector-specific Kafka consumer group), which isn't visible through the Connect
REST status API at all. Observed directly against this instance: right after a fresh deploy,
that internal protocol can cycle through a brand-new task UID roughly every 15 seconds for
well over two minutes before settling down - and any DML issued while it's still cycling gets
silently missed rather than queued or replayed later.

Both steps handle this the same way, via the shared `ConnectorTaskStabilityWaiter`: polling
`docker logs <container>` for the most recently mentioned task UID and waiting until it hasn't
changed for 20 straight seconds, rather than guessing at a fixed delay. It gives up after 10
minutes with a clear error if it never stabilizes.

**Why both steps check independently, rather than `demo-events` trusting `demo-connect-deploy`'s
earlier check:** confirmed directly that `demo-connect-deploy` settling isn't sufficient on its
own - a fresh rebalance to a brand-new task UID, with its own "Rebalance finished" event, occurred
*after* that process had already exited and returned control to `demo-events`, right as the
latter started issuing DML. Every one of those events was silently lost, reproducing the exact
"Consumed 0 records" failure this mechanism exists to prevent. `demo-connect-deploy`'s own check
still exists and is still useful (no point starting `demo-events` against a connector that's
visibly still churning), but it can only speak to what was true in its own process, at the moment
it checked - it says nothing about what happens after it exits, including during the separate
`mvn` invocation's own JVM startup time before `demo-events` gets around to checking again itself.

If the target Kafka cluster isn't reachable via `start-kafka-connect.sh`'s local Docker setup
at all (e.g. a GKE-hosted cluster), skip that script and point a Kafka Connect worker you
manage separately at it, then run steps 2-5 above with the properties overridden.

## Troubleshooting

- **`RESOURCE_EXHAUSTED: Total user split point count allowed for the instance are 5`** - the
  shared test instance has a small, instance-wide cap on concurrent split points, each of
  which lives for a while after creation (`Connection.forceSplit`'s expiry). If another test
  run or demo run recently forced a split, this can still be "in use" from that. Wait a bit and
  retry `demo-events`; no action needed on Spanner's side, it clears on its own.
- **`gcloud auth` / ADC errors** - re-run
  `gcloud auth application-default login --impersonate-service-account=<sa-email>` per
  [`doc/real-spanner-testing.md`](../doc/real-spanner-testing.md). If composing with a real
  Kafka Connect worker (which this demo always does), also confirm
  `GOOGLE_APPLICATION_CREDENTIALS` is set or the default ADC file exists at
  `~/.config/gcloud/application_default_credentials.json` - `demo-connect-deploy` forwards its
  contents to the connector running inside the container.
- **Connector never reaches `RUNNING`** - check `docker logs kafka-connect-worker`. Common
  causes: the plugin image is stale (`mvn package -DskipTests` and re-run
  `demo-connect-deploy`), or ADC credentials are missing/expired.
- **Task crash-loops with `OUT_OF_RANGE: start_timestamp ... earlier than the earliest read
  timestamp`** - `gcp.spanner.start.time` is baked into the connector's persisted config, so
  every restart retries the same invalid timestamp; this doesn't self-heal. Run
  `demo-connect-delete` then `demo-connect-deploy` to redeploy with a fresh, valid start time.
- **Validation fails with more than 6 records, or records left over from a previous run** -
  `demo-validate`'s cleanup should prevent this on a normal run, but a run that crashed before
  reaching cleanup can leave a stale topic behind. `demo/stop-kafka-connect.sh` tears down the
  whole broker (and its topics) - run it, then `demo/start-kafka-connect.sh` again for a clean
  slate before retrying.

## Manual cleanup

If a run is interrupted before `demo-validate` gets to run its own cleanup:

```bash
mvn -Pdemo exec:java@demo-connect-delete   # remove the connector, if deployed
demo/stop-kafka-connect.sh                  # stop the Kafka Connect worker + broker containers
```

The demo's Spanner objects (database `mutable_key_range_demo`, or whatever
`-Ddemo.database.id` was overridden to) are left behind in that case - the next run's
`demo-schema` step will detect and clean them up defensively before recreating. To remove them
immediately instead, drop them directly via `gcloud` or the Cloud Console.
