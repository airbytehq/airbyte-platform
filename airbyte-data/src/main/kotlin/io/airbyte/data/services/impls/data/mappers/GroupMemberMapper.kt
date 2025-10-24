/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.GroupMember as ModelGroupMember
import io.airbyte.data.repositories.entities.GroupMember as EntityGroupMember

/**
 * Converts a GroupMember entity to a GroupMember domain model.
 */
fun EntityGroupMember.toConfigModel(): ModelGroupMember {
  val id = requireNotNull(this.id) { "GroupMember must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "GroupMember must have a non-null createdAt" }
  return ModelGroupMember(
    id = id,
    groupId = this.groupId,
    userId = this.userId,
    createdAt = createdAt,
  )
}

/**
 * Converts a GroupMember domain model to a GroupMember entity.
 */
fun ModelGroupMember.toEntity(): EntityGroupMember =
  EntityGroupMember(
    id = this.id,
    groupId = this.groupId,
    userId = this.userId,
    createdAt = this.createdAt,
  )
