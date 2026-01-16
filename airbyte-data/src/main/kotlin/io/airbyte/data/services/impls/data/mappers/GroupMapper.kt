/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.GroupWithMemberCount
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.config.Group as ModelGroup
import io.airbyte.data.repositories.entities.Group as EntityGroup

/**
 * Converts a GroupWithMemberCount entity to a Group domain model.
 * Use this mapper for read operations that include member count.
 */
fun GroupWithMemberCount.toConfigModel(): ModelGroup {
  val id = requireNotNull(this.id) { "Group must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "Group must have a non-null createdAt" }
  val updatedAt = requireNotNull(this.updatedAt) { "Group must have a non-null updatedAt" }
  return ModelGroup(
    groupId = GroupId(id),
    name = this.name,
    description = this.description,
    organizationId = OrganizationId(this.organizationId),
    memberCount = this.memberCount,
    createdAt = createdAt,
    updatedAt = updatedAt,
  )
}

/**
 * Converts a basic Group entity to a Group domain model.
 * Use this mapper for write operations where member count is not needed.
 * Member count will be null in the resulting model.
 */
fun EntityGroup.toConfigModel(): ModelGroup {
  val id = requireNotNull(this.id) { "Group must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "Group must have a non-null createdAt" }
  val updatedAt = requireNotNull(this.updatedAt) { "Group must have a non-null updatedAt" }
  return ModelGroup(
    groupId = GroupId(id),
    name = this.name,
    description = this.description,
    organizationId = OrganizationId(this.organizationId),
    memberCount = null,
    createdAt = createdAt,
    updatedAt = updatedAt,
  )
}

/**
 * Converts a Group domain model to a Group entity.
 * Use this for write operations (save, update).
 */
fun ModelGroup.toEntity(): EntityGroup =
  EntityGroup(
    id = this.groupId.value,
    name = this.name,
    description = this.description,
    organizationId = this.organizationId.value,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
