/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.Permission

typealias EntityPermission = Permission
typealias ModelPermission = io.airbyte.config.Permission

fun EntityPermission.toConfigModel(): ModelPermission =
  ModelPermission()
    .withPermissionId(this.id)
    .withUserId(this.userId)
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)
    .withPermissionType(this.permissionType.toConfigModel())
    .withServiceAccountId(this.serviceAccountId)

fun ModelPermission.toEntity(): EntityPermission =
  EntityPermission(
    id = this.permissionId,
    userId = this.userId,
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
    permissionType = this.permissionType.toEntity(),
    serviceAccountId = this.serviceAccountId,
  )
