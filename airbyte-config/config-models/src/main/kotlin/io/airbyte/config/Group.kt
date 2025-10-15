/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import jakarta.annotation.Nullable
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Domain model representing a user group within an organization.
 * Groups are organization-scoped collections of users that can have permissions assigned to them.
 * Group names must be unique within an organization.
 */
@JvmRecord
data class Group(
  val groupId: UUID,
  val name: String,
  @field:Nullable @param:Nullable val description: String?,
  val organizationId: UUID,
  val createdAt: OffsetDateTime,
  val updatedAt: OffsetDateTime,
) {
  init {
    require(name.isNotBlank()) { "Group name cannot be blank" }
    require(name.length <= 256) { "Group name cannot exceed 256 characters" }
    description?.let {
      require(it.length <= 1024) { "Group description cannot exceed 1024 characters" }
    }
  }

  override fun toString(): String =
    "Group(groupId=$groupId, name='$name', description=$description, " +
      "organizationId=$organizationId, createdAt=$createdAt, updatedAt=$updatedAt)"
}
