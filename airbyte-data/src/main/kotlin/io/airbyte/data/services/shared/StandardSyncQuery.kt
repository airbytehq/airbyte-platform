/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import jakarta.annotation.Nonnull
import java.util.UUID

/**
 * Query object for querying connections for a workspace.
 *
 * @param workspaceId workspace to fetch connections for
 * @param sourceId fetch connections with this source id
 * @param destinationId fetch connections with this destination id
 * @param includeDeleted include tombstoned connections
 */

data class StandardSyncQuery(
  @field:Nonnull @param:Nonnull val workspaceId: UUID,
  @JvmField val sourceId: List<UUID>?,
  @JvmField val destinationId: List<UUID>?,
  val includeDeleted: Boolean,
)
