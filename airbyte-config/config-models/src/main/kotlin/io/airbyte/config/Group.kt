/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import java.time.OffsetDateTime

/**
 * Domain model representing a user group within an organization.
 * Groups are organization-scoped collections of users that can have permissions assigned to them.
 * Group names must be unique within an organization.
 */

data class Group(
  val groupId: GroupId,
  val name: String,
  val description: String?,
  val organizationId: OrganizationId,
  val memberCount: Long?,
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
}
