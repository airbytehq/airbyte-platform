/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.GroupMemberWithUserInfo
import io.airbyte.config.GroupMember as ModelGroupMember
import io.airbyte.data.repositories.entities.GroupMember as EntityGroupMember

/**
 * Converts a GroupMemberWithUserInfo entity to a GroupMember domain model.
 * Use this mapper for read operations that include user email and name.
 */
fun GroupMemberWithUserInfo.toConfigModel(): ModelGroupMember {
  val id = requireNotNull(this.id) { "GroupMember must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "GroupMember must have a non-null createdAt" }
  val email = requireNotNull(this.email) { "GroupMember must have a non-null email (ensure query joins with user table)" }
  val name = requireNotNull(this.name) { "GroupMember must have a non-null name (ensure query joins with user table)" }
  return ModelGroupMember(
    id = id,
    groupId = this.groupId,
    userId = this.userId,
    email = email,
    name = name,
    createdAt = createdAt,
  )
}

/**
 * Converts a basic GroupMember entity to a GroupMember domain model.
 * Use this mapper for write operations where user info is not needed.
 * Email and name will be null in the resulting model.
 */
fun EntityGroupMember.toConfigModel(): ModelGroupMember {
  val id = requireNotNull(this.id) { "GroupMember must have a non-null id" }
  val createdAt = requireNotNull(this.createdAt) { "GroupMember must have a non-null createdAt" }
  return ModelGroupMember(
    id = id,
    groupId = this.groupId,
    userId = this.userId,
    email = null,
    name = null,
    createdAt = createdAt,
  )
}

/**
 * Converts a GroupMember domain model to a GroupMember entity.
 * Use this for write operations (save).
 */
fun ModelGroupMember.toEntity(): EntityGroupMember =
  EntityGroupMember(
    id = this.id,
    groupId = this.groupId,
    userId = this.userId,
    createdAt = this.createdAt,
  )
