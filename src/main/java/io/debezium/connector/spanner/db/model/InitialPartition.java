/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;

/**
 * Utility class to determine initial partition constants and methods.
 *
 * <p>The initial partition has the artificial token defined by {@link
 * InitialPartition#PARTITION_TOKEN} and it has no parent tokens.
 */
public class InitialPartition {

    private InitialPartition() {
    }

    /**
     * The token of the initial partition. This is an artificial token for the Connector and it is not
     * recognised by Cloud Spanner.
     */
    public static final String PARTITION_TOKEN = "Parent0";

    /**
     * Delimiter used to make the artificial initial partition token unique per placement TVF, when a
     * change stream is configured with {@code gcp.spanner.placement.tvf.names}. Each configured TVF
     * has its own independent partition token space on the Spanner side, so a single root
     * {@link #PARTITION_TOKEN} would collide across TVFs; suffixing it with the TVF name keeps every
     * root partition unique while still being recognised as "the initial partition" by
     * {@link #isInitialPartition(String)}.
     */
    public static final String PARTITION_TOKEN_TVF_NAME_DELIMITER = "#";

    /**
     * Verifies if the given partition token is the initial partition (optionally suffixed with a
     * placement TVF name, see {@link #tokenFor(String)}).
     *
     * @param partitionToken the partition token to be checked
     * @return true if the given token is the initial partition, and false otherwise
     */
    public static boolean isInitialPartition(String partitionToken) {
        return Objects.equals(PARTITION_TOKEN, partitionToken)
                || (partitionToken != null && partitionToken.startsWith(PARTITION_TOKEN + PARTITION_TOKEN_TVF_NAME_DELIMITER));
    }

    /**
     * Builds the artificial initial partition token used for the given placement TVF name. When
     * {@code tvfName} is null or blank, the plain {@link #PARTITION_TOKEN} is returned; otherwise the
     * TVF name is appended so that every per-placement root partition gets a distinct token (each
     * placement TVF has its own independent partition token space on the Spanner side).
     *
     * @param tvfName the placement TVF name this initial partition is scoped to, or null/blank for the
     *     default (non per-placement-TVF) case
     * @return the initial partition token to use
     */
    public static String tokenFor(String tvfName) {
        if (tvfName == null || tvfName.isBlank()) {
            return PARTITION_TOKEN;
        }
        return PARTITION_TOKEN + PARTITION_TOKEN_TVF_NAME_DELIMITER + tvfName;
    }

}
