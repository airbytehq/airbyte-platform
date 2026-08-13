/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.mappers

import io.airbyte.api.server.generated.models.GroupMemberRead
import io.airbyte.api.server.generated.models.GroupPermissionRead
import io.airbyte.api.server.generated.models.GroupRead
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.config.Permission

fun Group.toGroupRead(): GroupRead =
  GroupRead(
    groupId = groupId.value,
    name = name,
    description = description,
    organizationId = organizationId.value,
    memberCount = memberCount ?: 0,
  )

fun GroupMember.toGroupMemberRead(): GroupMemberRead =
  GroupMemberRead(
    memberId = id,
    groupId = groupId,
    userId = userId,
    userEmail = email ?: "",
    userName = name ?: "",
  )

fun Permission.toGroupPermissionRead(): GroupPermissionRead =
  GroupPermissionRead(
    permissionId = permissionId,
    groupId = groupId!!,
    permissionType = PermissionType.valueOf(permissionType.name),
    workspaceId = workspaceId,
    organizationId = organizationId,
  )
