/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

/**
 * Repository of all SQL queries for the Configs Db. We are moving to persistences scoped by
 * resource.
 */
@Deprecated
@SuppressWarnings("PMD.PreserveStackTrace")
public class ConfigRepository {

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

  /**
   * Query object for paginated querying of resource in an organization.
   *
   * @param organizationId organization to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record ResourcesByOrganizationQueryPaginated(
                                                      @Nonnull UUID organizationId,
                                                      boolean includeDeleted,
                                                      int pageSize,
                                                      int rowOffset) {

  }

  /**
   * Query object for paginated querying of resource for a user.
   *
   * @param userId user to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record ResourcesByUserQueryPaginated(
                                              @Nonnull UUID userId,
                                              boolean includeDeleted,
                                              int pageSize,
                                              int rowOffset) {}

  @SuppressWarnings("ParameterName")
  @VisibleForTesting
  public ConfigRepository() {}

}
