/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Query object for paginated querying of sources/destinations in multiple workspaces.
 *
 * @param workspaceIds workspaces to fetch resources for
 * @param includeDeleted include tombstoned resources
 * @param pageSize limit
 * @param rowOffset offset
 * @param nameContains string to search name contains by
 */
public record ResourcesQueryPaginated(
                                      @Nonnull List<UUID> workspaceIds,
                                      boolean includeDeleted,
                                      int pageSize,
                                      int rowOffset,
                                      String nameContains) {

}
