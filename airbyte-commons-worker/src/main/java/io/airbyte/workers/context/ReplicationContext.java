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
 *
 * @param connectionId The connection ID associated with the sync.
 * @param sourceId The source ID associated with the sync.
 * @param destinationId The destination ID associated with the sync.
 * @param jobId The job ID associated with the sync.
 * @param attempt The attempt number of the sync.
 * @param workspaceId The workspace ID associated with the sync.
 */
public record ReplicationContext(boolean isReset,
                                 UUID connectionId,
                                 UUID sourceId,
                                 UUID destinationId,
                                 Long jobId,
                                 Integer attempt,
                                 UUID workspaceId) {}
