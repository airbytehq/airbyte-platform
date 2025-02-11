/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Query object for paginated querying of connections in multiple workspaces.
 *
 * @param workspaceIds workspaces to fetch connections for
 * @param sourceId fetch connections with this source id
 * @param destinationId fetch connections with this destination id
 * @param includeDeleted include tombstoned connections
 * @param pageSize limit
 * @param rowOffset offset
 */
public record StandardSyncsQueryPaginated(
                                          @Nonnull List<UUID> workspaceIds,
                                          List<UUID> sourceId,
                                          List<UUID> destinationId,
                                          boolean includeDeleted,
                                          int pageSize,
                                          int rowOffset) {

}
