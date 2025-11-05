/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Permission
import io.airbyte.publicApi.server.generated.models.GroupPermissionCreateRequest
import io.airbyte.publicApi.server.generated.models.GroupPermissionResponse
import io.airbyte.publicApi.server.generated.models.PermissionType
import java.util.UUID

/**
 * Converts a Permission config model to a GroupPermissionResponse for the public API.
 *
 * @return GroupPermissionResponse for the public API
 */
fun Permission.toGroupPermissionResponse(): GroupPermissionResponse =
  GroupPermissionResponse(
    permissionId = this.permissionId,
    groupId = this.groupId!!,
    permissionType = PermissionType.valueOf(this.permissionType.name),
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
  )

/**
 * Converts a list of Permission config models to a list of GroupPermissionResponses.
 *
 * @return List of GroupPermissionResponse for the public API
 */
fun List<Permission>.toGroupPermissions(): List<GroupPermissionResponse> = this.map { it.toGroupPermissionResponse() }

/**
 * Converts a GroupPermissionCreateRequest from the public API to a Permission config model.
 *
 * @param groupId The ID of the group to assign the permission to
 * @return Permission config model
 */
fun GroupPermissionCreateRequest.toPermission(groupId: UUID): Permission =
  Permission()
    .withGroupId(groupId)
    .withPermissionType(Permission.PermissionType.valueOf(this.permissionType.name))
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)
