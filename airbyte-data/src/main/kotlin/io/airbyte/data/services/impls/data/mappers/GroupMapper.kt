/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.Group as ModelGroup
import io.airbyte.data.repositories.entities.Group as EntityGroup

/**
 * Converts a Group entity to a Group domain model.
 */
fun EntityGroup.toConfigModel(): ModelGroup {
  val id = requireNotNull(this.id) { "GroupMember must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "GroupMember must have a non-null createdAt" }
  val updatedAt = requireNotNull(this.updatedAt) { "GroupMember must have a non-null updatedAt" }
  return ModelGroup(
    groupId = id,
    name = this.name,
    description = this.description,
    organizationId = this.organizationId,
    createdAt = createdAt,
    updatedAt = updatedAt,
  )
}

/**
 * Converts a Group domain model to a Group entity.
 */
fun ModelGroup.toEntity(): EntityGroup =
  EntityGroup(
    id = this.groupId,
    name = this.name,
    description = this.description,
    organizationId = this.organizationId,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
