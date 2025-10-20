/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.time.OffsetDateTime
import java.util.UUID

/**
 * Domain model representing a user's membership in a group.
 * This establishes many-to-many relationships between users and groups.
 * The combination of groupId and userId is unique - a user can only be a member of a group once.
 */
data class GroupMember(
  val id: UUID,
  val groupId: UUID,
  val userId: UUID,
  val createdAt: OffsetDateTime,
)
