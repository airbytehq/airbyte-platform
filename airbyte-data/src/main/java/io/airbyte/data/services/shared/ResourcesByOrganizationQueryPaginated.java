/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import jakarta.annotation.Nonnull;
import java.util.UUID;

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
