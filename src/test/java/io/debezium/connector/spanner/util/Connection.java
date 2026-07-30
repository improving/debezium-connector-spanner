/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import static io.debezium.connector.spanner.util.Database.isRealSpannerMode;
import static io.debezium.connector.spanner.util.Database.isSpannerOmniEndpoint;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Strings;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.SplitPoints;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import io.debezium.connector.spanner.db.DatabaseClientFactory;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.grpc.ManagedChannelBuilder;

public class Connection {

    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    public static final String emulatorHost = "http://localhost:9010";
    public static final String containerNetworkEmulatorHost = "http://spanner-emulator:9010";

    public DatabaseClient databaseClient;
    private Spanner spanner;
    private SchemaDao schemaDao;
    private final Dialect dialect;

    protected Connection(Database database) {
        this.projectId = database.getProjectId();
        this.instanceId = database.getInstanceId();
        this.databaseId = database.getDatabaseId();
        this.dialect = database.getDialect();
    }

    public ResultSet executeSelect(String query) {
        return databaseClient.singleUse().executeQuery(Statement.of(query));
    }

    public ResultSet executeSelect(Statement statement) {
        return databaseClient.singleUse().executeQuery(statement);
    }

    public Long executeUpdate(String query) {
        final String msg = "Execution result: {}, query: {}";
        return databaseClient.readWriteTransaction()
                .run(transaction -> {
                    final var uuid = UUID.randomUUID().toString();
                    LOG.info("Begin transaction {}", uuid);
                    final var res = transaction.executeUpdate(Statement.of(query));
                    if (res > 0L) {
                        LOG.info(msg, res, query);
                    }
                    else {
                        LOG.warn(msg, res, query);
                    }
                    return res;
                });
    }

    public Long executeUpdate(List<String> queries) {
        final String msg = "Execution result: {}, query: {}";
        return databaseClient.readWriteTransaction()
                .run(transaction -> {
                    final var uuid = UUID.randomUUID().toString();
                    LOG.info("Begin transaction {}", uuid);
                    var result = 0L;
                    for (final var query : queries) {
                        final var res = transaction.executeUpdate(Statement.of(query));
                        result += res;
                        if (res > 0L) {
                            LOG.info(msg, res, query);
                        }
                        else {
                            LOG.warn(msg, res, query);
                        }
                    }
                    LOG.info("End transaction {}, result : {}", uuid, result);
                    return result;
                });
    }

    public void updateDDL(Iterable<String> updates) throws ExecutionException, InterruptedException {
        OperationFuture<Void, UpdateDatabaseDdlMetadata> future = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(instanceId, databaseId, updates, null);
        future.get();
    }

    public void createTable(String tableDefinition) throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create table " + tableDefinition));
    }

    private static long ddlWaitTimeSeconds() {
        return Long.parseLong(System.getProperty("debezium.test.spanner.ddl.waittime", "60"));
    }

    public void createChangeStream(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables))));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> streamExists(changeStreamName));
    }

    public void createMutableKeyRangeChangeStream(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')"));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> streamExists(changeStreamName));
    }

    private static final Duration DEFAULT_SPLIT_EXPIRY = Duration.ofMinutes(30);

    /**
     * Forces Spanner to split the key range of {@code tableName} at the given key value(s),
     * triggering a mutable key range move (MoveOut/MoveIn) for change streams tracking the table.
     * Confirmed working against both Spanner Omni and real Cloud Spanner via the
     * {@code AddSplitPoints} admin API. Split points expire after {@link #DEFAULT_SPLIT_EXPIRY} -
     * use {@link #forceSplit(String, Duration, String...)} to override.
     */
    public void forceSplit(String tableName, String... keyParts) {
        forceSplit(tableName, DEFAULT_SPLIT_EXPIRY, keyParts);
    }

    /**
     * Same as {@link #forceSplit(String, String...)}, but with an explicit split-point expiry.
     * Split points count against a small, instance-wide quota (5 concurrent on the shared
     * real-Spanner test instance) until they expire, so keep this as short as safely possible for
     * whatever the calling test actually needs rather than reaching for a long, "safe" duration.
     */
    public void forceSplit(String tableName, Duration expiryDuration, String... keyParts) {
        try (com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient adminClient = spanner.createDatabaseAdminClient()) {
            Instant expiry = Instant.now().plus(expiryDuration);
            Timestamp expireTime = Timestamp.newBuilder()
                    .setSeconds(expiry.getEpochSecond())
                    .setNanos(expiry.getNano())
                    .build();

            ListValue.Builder keyValues = ListValue.newBuilder();
            for (String keyPart : keyParts) {
                keyValues.addValues(Value.newBuilder().setStringValue(keyPart).build());
            }

            SplitPoints splitPoint = SplitPoints.newBuilder()
                    .setTable(tableName)
                    .setExpireTime(expireTime)
                    .addKeys(SplitPoints.Key.newBuilder().setKeyParts(keyValues))
                    .build();

            adminClient.addSplitPoints(DatabaseName.of(projectId, instanceId, databaseId), List.of(splitPoint));
            LOG.info("Forced split on table {} at key {}", tableName, List.of(keyParts));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to force split for table " + tableName, e);
        }
    }

    public void createChangeStreamNewValue(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = 'NEW_VALUES'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamNewRow(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = 'NEW_ROW'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamNewRowAndOldValues(String changeStreamName, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = 'NEW_ROW_AND_OLD_VALUES'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamExcludeDelete(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            exclude_delete = true\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamExcludeInsert(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            exclude_insert = true\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamExcludeUpdate(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            exclude_update = true\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamAllowTxnExclusion(String changeStreamName, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            allow_txn_exclusion = true\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStreamExcludeTtlDeletes(String changeStreamName, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            exclude_ttl_deletes = true\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    public void createChangeStream(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        if (partitionMode == PartitionMode.IMMUTABLE_KEY_RANGE) {
            // The Spanner emulator's DDL parser rejects the partition_mode option
            // entirely ("Option: partition_mode is unknown"), even when the value
            // requested is the documented default. Since IMMUTABLE_KEY_RANGE is that
            // default, falling back to the plain DDL is equivalent and actually works
            // against the emulator.
            this.createChangeStream(changeStreamName, tables);
            return;
        }
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS ( partition_mode = '" + partitionMode.name() + "' )"));
        await().atMost(Duration.ofSeconds(60)).until(() -> streamExists(changeStreamName));
    }

    /**
     * Creates a {@code PLACEMENT} mapped to an existing, pre-provisioned instance partition -
     * see {@code doc/real-spanner-testing.md} for how {@code instancePartitionId} values are
     * provisioned.
     */
    public void createPlacement(String placementName, String instancePartitionId) throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create placement " + placementName +
                " OPTIONS ( instance_partition = '" + instancePartitionId + "' )"));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> placementExists(placementName));
    }

    public boolean dropPlacement(String placementName) throws InterruptedException {
        try {
            if (!placementExists(placementName)) {
                return false;
            }
            this.updateDDL(List.of("drop placement " + placementName));
        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t drop placement", ex);
            return false;
        }
        return true;
    }

    private boolean placementExists(String placementName) {
        Statement statement = Statement.newBuilder("select placement_name " +
                "from information_schema.placements " +
                "where placement_name = @placementName")
                .bind("placementName").to(placementName).build();
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    private String createInstance() {
        if (isSpannerOmniEndpoint()) {
            return DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID;
        }
        if (isRealSpannerMode()) {
            // real mode targets a pre-provisioned, persistent instance; never create/modify it here
            return instanceId;
        }
        for (Instance value : this.spanner.getInstanceAdminClient().listInstances().iterateAll()) {
            if (value.getId().getInstance().equals(instanceId)) {
                return instanceId;
            }
        }
        String configId = "regional-us-central1";
        String displayName = "For IT";
        int nodeCount = 1;
        InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
                .setInstanceConfigId(InstanceConfigId.of(projectId, configId))
                .setNodeCount(nodeCount)
                .setDisplayName(displayName)
                .build();

        OperationFuture<Instance, CreateInstanceMetadata> instance = this.spanner.getInstanceAdminClient()
                .createInstance(instanceInfo);
        try {
            instance.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return instanceId;
    }

    private boolean streamExists(String streamName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement.newBuilder("select change_stream_name " +
                    "from information_schema.change_streams cs " +
                    "where cs.change_stream_name = $1")
                    .bind("p1")
                    .to(streamName.toLowerCase())
                    .build();
        }
        else {
            statement = Statement.newBuilder("select change_stream_name " +
                    "from information_schema.change_streams cs " +
                    "where cs.change_stream_name = @streamname")
                    .bind("streamName")
                    .to(streamName).build();
        }
        return databaseClient.singleUse().executeQuery(statement).next();
    }

    public boolean dropTable(String tableName) throws InterruptedException {
        try {
            if (!tableExists(tableName)) {
                return false;
            }
            this.updateDDL(List.of("drop table " + tableName));
        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t drop table", ex);
            return false;
        }
        return true;
    }

    public boolean dropChangeStream(String changeStreamName) throws InterruptedException {
        try {
            if (!this.changeStreamExists(changeStreamName)) {
                return false;
            }
            this.updateDDL(List.of("drop change stream " + changeStreamName));

        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t delete change stream", ex);
            return false;
        }
        return true;
    }

    public boolean changeStreamExists(String changeStreamName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement.newBuilder("select * from information_schema.change_streams " +
                    "where change_stream_name = $1")
                    .bind("p1").to(changeStreamName).build();
        }
        else {
            statement = Statement.newBuilder("select * from information_schema.change_streams " +
                    "where change_stream_name = @streamName")
                    .bind("streamName").to(changeStreamName).build();
        }
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    public boolean tableExists(String tableName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement
                    .newBuilder(
                            "select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
                                    "and table_name = $1")
                    .bind("p1").to(tableName).build();
        }
        else {
            statement = Statement
                    .newBuilder(
                            "select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
                                    "and table_name = @tableName")
                    .bind("tableName").to(tableName).build();
        }
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    public boolean databaseExists(String databaseId) {
        try {
            return this.spanner.getDatabaseAdminClient().getDatabase(instanceId, databaseId) != null;
        }
        catch (Exception ex) {
            return false;
        }
    }

    public void dropDatabase(String databaseId) {
        this.spanner.getDatabaseAdminClient().dropDatabase(instanceId, databaseId);
        LOG.info("{} database has been dropped", databaseId);
    }

    public void createDatabase(String databaseId, Dialect dialect) throws InterruptedException {
        if (!isSpannerOmniEndpoint()) {
            createInstance();
        }
        DatabaseAdminClient dbAdminClient = this.spanner.getDatabaseAdminClient();
        OperationFuture<com.google.cloud.spanner.Database, CreateDatabaseMetadata> operationFuture = dbAdminClient
                .createDatabase(
                        dbAdminClient.newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId))
                                .setDialect(dialect).build(),
                        Collections.emptyList());
        try {
            operationFuture.get();
        }
        catch (ExecutionException ex) {
            throw new RuntimeException("Failed to create database", ex);
        }
        LOG.info("{} database has been created", databaseId);
    }

    public Connection connect(Dialect dialect) throws InterruptedException {
        if (this.databaseClient != null) {
            return this;
        }

        this.init();

        if (databaseExists(databaseId)) {
            this.dropDatabase(databaseId);
        }

        this.createDatabase(databaseId, dialect);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.dropDatabase(databaseId)));

        this.databaseClient = this.spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        this.schemaDao = new SchemaDao(databaseClient);

        return this;
    }

    /**
     * Connects to a database intended to outlive this JVM process: creates it only if it does
     * not already exist (idempotent across repeated runs), and does not register a drop-on-exit
     * shutdown hook the way {@link #connect(Dialect)} does. For multi-process use (e.g. a demo
     * split across separate schema/data/consumer processes that must share one database without
     * any of them tearing it down just by exiting) - callers are responsible for explicit
     * cleanup via {@link #dropChangeStream}/{@link #dropTable}/{@link #dropDatabase}.
     */
    public Connection connectPersistent(Dialect dialect) throws InterruptedException {
        if (this.databaseClient != null) {
            return this;
        }

        this.init();

        if (!databaseExists(databaseId)) {
            this.createDatabase(databaseId, dialect);
        }

        this.databaseClient = this.spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        this.schemaDao = new SchemaDao(databaseClient);

        return this;
    }

    private void init() {
        SpannerOptions.Builder builder = SpannerOptions.newBuilder();

        builder.setProjectId(projectId);
        if (isSpannerOmniEndpoint()) {
            builder.setCredentials(NoCredentials.getInstance());
            builder.setExperimentalHost(System.getProperty("gcp.spanner.host"));
            if (Boolean.parseBoolean(System.getProperty("spanner.omni.use.plaintext", "false"))) {
                builder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
            }
            else if (!Strings.isNullOrEmpty(System.getProperty("spanner.omni.client.key.path"))
                    && !Strings.isNullOrEmpty(System.getProperty("spanner.omni.client.cert.path"))) {
                builder.useClientCert(System.getProperty("spanner.omni.client.cert.path"), System.getProperty("spanner.omni.client.key.path"));
            }
            builder.setBuiltInMetricsEnabled(false);
        }
        else if (isRealSpannerMode()) {
            // Neither NoCredentials nor an emulator host is set here: SpannerOptions resolves
            // Application Default Credentials on its own, matching production behavior.
        }
        else {
            builder.setCredentials(NoCredentials.getInstance());
            builder.setEmulatorHost(emulatorHost);
        }

        SpannerOptions options = builder.build();
        try {
            this.spanner = options.getService();
        }
        catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
