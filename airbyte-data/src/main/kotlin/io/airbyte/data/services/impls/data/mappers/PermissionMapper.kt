package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.Permission

typealias EntityPermission = Permission
typealias ModelPermission = io.airbyte.config.Permission

fun EntityPermission.toConfigModel(): ModelPermission {
  return ModelPermission()
    .withPermissionId(this.id)
    .withUserId(this.userId)
    .withWorkspaceId(this.workspaceId)
    .withOrganizationId(this.organizationId)
    .withPermissionType(this.permissionType.toConfigModel())
}

fun ModelPermission.toEntity(): EntityPermission {
  return EntityPermission(
    id = this.permissionId,
    userId = this.userId,
    workspaceId = this.workspaceId,
    organizationId = this.organizationId,
    permissionType = this.permissionType.toEntity(),
  )
}
