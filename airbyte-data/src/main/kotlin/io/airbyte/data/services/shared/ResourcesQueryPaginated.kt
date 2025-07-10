/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import jakarta.annotation.Nonnull
import java.util.UUID

/**
 * Query object for paginated querying of sources/destinations in multiple workspaces.
 *
 * @param workspaceIds workspaces to fetch resources for
 * @param includeDeleted include tombstoned resources
 * @param pageSize limit
 * @param rowOffset offset
 * @param nameContains string to search name contains by
 */
@JvmRecord
data class ResourcesQueryPaginated(
  @field:Nonnull @param:Nonnull val workspaceIds: List<UUID>,
  val includeDeleted: Boolean = false,
  val pageSize: Int = 20,
  val rowOffset: Int = 0,
  val nameContains: String?,
)
