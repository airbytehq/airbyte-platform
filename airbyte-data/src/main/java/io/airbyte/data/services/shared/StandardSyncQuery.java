/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Query object for querying connections for a workspace.
 *
 * @param workspaceId workspace to fetch connections for
 * @param sourceId fetch connections with this source id
 * @param destinationId fetch connections with this destination id
 * @param includeDeleted include tombstoned connections
 */
public record StandardSyncQuery(@Nonnull UUID workspaceId, List<UUID> sourceId, List<UUID> destinationId, boolean includeDeleted) {

}
