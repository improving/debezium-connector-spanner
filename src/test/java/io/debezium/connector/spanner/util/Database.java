/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.util.UUID;

import com.google.cloud.spanner.Dialect;

import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.db.DatabaseClientFactory;

public class Database {

    private static final String projectId = "test-project";
    private static final String instanceId = "test-instance";

    private static final String SPANNER_MODE_PROPERTY_NAME = "debezium.test.spanner.mode";
    private static final String REAL_MODE = "real";

    private final String databaseId;

    private Connection connection;

    private final Dialect dialect;

    private Database(String databaseId, Dialect dialect) {
        this.databaseId = databaseId;
        this.dialect = dialect;
    }

    public static String getSpannerType() {
        return System.getProperty(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME);
    }

    public static boolean isSpannerOmniEndpoint() {
        return BaseSpannerConnectorConfig.SpannerType.OMNI.name().equalsIgnoreCase(getSpannerType());
    }

    public static boolean isRealSpannerMode() {
        return REAL_MODE.equalsIgnoreCase(System.getProperty(SPANNER_MODE_PROPERTY_NAME, "emulator"));
    }

    public static final Database TEST_DATABASE = Database.builder()
            .generateDatabaseId()
            .build();

    public static final Database TEST_PG_DATABASE = Database.builder()
            .generateDatabaseId()
            .dialect(Dialect.POSTGRESQL)
            .build();

    public String getProjectId() {
        if (isSpannerOmniEndpoint()) {
            return DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID;
        }
        return isRealSpannerMode() ? System.getProperty("gcp.spanner.project.id", projectId) : projectId;
    }

    public String getInstanceId() {
        if (isSpannerOmniEndpoint()) {
            return DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID;
        }
        return isRealSpannerMode() ? System.getProperty("gcp.spanner.instance.id", instanceId) : instanceId;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public Connection getConnection() {
        if (this.connection != null) {
            return this.connection;
        }
        try {
            this.connection = new Connection(this).connect(dialect);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return this.connection;
    }

    /**
     * Like {@link #getConnection()}, but via {@link Connection#connectPersistent} - for
     * multi-process use where the database must outlive any single process holding this
     * {@link Database} instance. See {@link Connection#connectPersistent} for details.
     */
    public Connection getPersistentConnection() {
        if (this.connection != null) {
            return this.connection;
        }
        try {
            this.connection = new Connection(this).connectPersistent(dialect);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return this.connection;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseId;

        private Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;

        public Builder dialect(Dialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder databaseId(String databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public Builder generateDatabaseId() {
            String uuid = UUID.randomUUID().toString().replace("-", "")
                    .substring(0, 8);
            this.databaseId = "int_tests_" + uuid;
            return this;
        }

        public Database build() {
            return new Database(databaseId, dialect);
        }
    }
}
