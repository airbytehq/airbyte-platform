/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

/**
 * View entity representing a Group with computed member count.
 * Used for read operations that need to display the number of members in a group.
 * This is returned by JOIN queries and should not be used for persistence operations.
 */
@MappedEntity("group")
data class GroupWithMemberCount(
  var id: UUID?,
  var name: String,
  var description: String?,
  var organizationId: UUID,
  var createdAt: OffsetDateTime?,
  var updatedAt: OffsetDateTime?,
  // Computed field from JOIN query with group_member table
  var memberCount: Long?,
)
