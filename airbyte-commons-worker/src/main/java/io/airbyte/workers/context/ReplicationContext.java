/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context;

import java.util.UUID;

/**
 * Context of a Replication.
 * <p>
 * Contains the relevant ids of the object involved in a sync. This is not the place to hold
 * configuration.
 */
public record ReplicationContext(UUID connectionId, UUID sourceId, UUID destinationId) {

}
