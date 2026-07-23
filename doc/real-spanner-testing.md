# Running integration tests against real Cloud Spanner

This runbook covers the manual, outside-of-code steps for running the
`*IT.java` integration test suite against a real, persistent Cloud Spanner
instance instead of the emulator (`debezium.test.spanner.mode=real`, the
`-Preal-spanner` Maven profile). See `openspec/changes/real-cloud-spanner-testing/design.md`
for the rationale behind these choices.

## 1. Provision the instance and service account (skip if already set up)

Check with your team whether a persistent instance/service account already
exists before doing this.

1. Use the project `improvingvancouver`
2. Create a minimally-sized, persistent instance (100 processing units).
   You can do this in the [Cloud console](https://docs.cloud.google.com/spanner/docs/create-manage-instances)
   or via `gcloud`:
   ```bash
   gcloud spanner instances create <instance-id> \
     --edition=STANDARD \
     --config=<config> \
     --processing-units=100 \
     --description="real-cloud-spanner-testing" \
     --project=improvingvancouver
   ```
3. Verify it's ready:
   ```bash
   gcloud spanner instances describe <instance-id> --project=improvingvancouver
   # expect: state: READY
   ```
4. Use the existing dedicated, least-privilege service account:
   `real-cloud-spanner-testing@improvingvancouver.iam.gserviceaccount.com`.
5. Grant that service account `roles/spanner.databaseAdmin` scoped to the
   *instance*, not the project. You can do this in the
   [Cloud console](https://docs.cloud.google.com/spanner/docs/grant-permissions#instance-level_permissions)
   or via `gcloud`:
   ```bash
   gcloud spanner instances add-iam-policy-binding <instance-id> \
     --member="serviceAccount:real-cloud-spanner-testing@improvingvancouver.iam.gserviceaccount.com" \
     --role="roles/spanner.databaseAdmin" \
     --project=improvingvancouver
   ```
6. Verify the scope is instance-level only:
   ```bash
   gcloud spanner instances get-iam-policy <instance-id> --project=improvingvancouver
   ```
   Confirm no project-level `roles/spanner.databaseAdmin` or
   `roles/spanner.admin` grant exists for this service account.
7. Record the `<instance-id>` value — this becomes the `gcp.spanner.instance.id`
   system property used below. `gcp.spanner.project.id` is always `improvingvancouver`.

## 2. Authenticate locally via impersonation

No key files. Populate local Application Default Credentials (ADC) by
impersonating the service account. This requires a `roles/iam.serviceAccountTokenCreator`
IAM grant on the service account for your user account; ask the team if you
do not already have it:

```bash
gcloud auth application-default login \
  --impersonate-service-account=real-cloud-spanner-testing@improvingvancouver.iam.gserviceaccount.com
```

This writes an `impersonated_service_account` ADC file, embedding your own
authorized-user refresh token as `source_credentials`, and is picked up
automatically by the harness's real-mode `SpannerOptions` resolution (no
`GOOGLE_APPLICATION_CREDENTIALS` needed for embedded-mode runs).

If you're also composing with `-Preal-connect` (real Kafka Connect worker,
which runs in a separate container with no access to your local ADC), point
`GOOGLE_APPLICATION_CREDENTIALS` at the ADC file so the harness can forward
its contents as the connector's `gcp.spanner.credentials.json` config value:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(gcloud info --format='value(config.paths.global_config_dir)')/application_default_credentials.json"
```

## 3. Run the tests

From `debezium-connector-spanner`:

```bash
mvn verify -Preal-spanner \
  -Dgcp.spanner.project.id=improvingvancouver \
  -Dgcp.spanner.instance.id=<instance-id> \
  -DskipTests \
  -Dit.test=<TEST_CLASS_NAME>
```

Notes:
- TEST_CLASS_NAME example:MutableKeyRangeIT
- Compose with real Kafka Connect via `-Preal-connect` in the same
  invocation if you need to exercise the REST-deployed connector path.
- `debezium.test.spanner.ddl.waittime` (default 60s) can be raised via
  `-Ddebezium.test.spanner.ddl.waittime=<seconds>` if you see DDL-propagation
  timeouts against the real instance.
- Default `mvn verify` (no `-Preal-spanner`) is unaffected and still runs
  against the emulator.

## 4. Clean up leaked databases (only if a run was killed/crashed)

`Connection.connect()` normally drops its per-run database via a JVM
shutdown hook on graceful exit. A hard-killed run (e.g. `kill -9`, IDE force
stop) can leave a database behind. List and remove any stray databases named
like `int_tests_*`:

```bash
gcloud spanner databases list --instance=<instance-id> --project=improvingvancouver
gcloud spanner databases delete <leaked-database-id> --instance=<instance-id> --project=improvingvancouver
```

## 5. Tear down the instance 

The instance is persistent and shared across runs/people — to save costs, tear it down at night:

```bash
gcloud spanner instances delete <instance-id> --project=improvingvancouver
```

