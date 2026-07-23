/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

/**
 * View entity representing a GroupMember with user information from a JOIN.
 * Used for read operations that need to display user email and name.
 * This is returned by JOIN queries with the user table and should not be used for persistence operations.
 */
@MappedEntity("group_member")
data class GroupMemberWithUserInfo(
  var id: UUID,
  var groupId: UUID,
  var userId: UUID,
  var createdAt: OffsetDateTime,
  // Computed fields from JOIN query with user table
  var email: String?,
  var name: String?,
)
