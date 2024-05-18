/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.public_api.model.generated.PermissionResponseRead
import io.airbyte.public_api.model.generated.PermissionScope

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object PermissionResponseReadMapper {
  /**
   * Converts a PermissionRead object from the config api to a PermissionResponseRead.
   *
   * @param permissionRead Output of a permission create from config api
   * @return PermissionResponseRead Response object with permission details
   */
  fun from(permissionRead: PermissionRead): PermissionResponseRead {
    val permissionResponseRead = PermissionResponseRead()
    permissionResponseRead.permissionId = permissionRead.permissionId
    permissionResponseRead.permissionType = enumValueOf(permissionRead.permissionType.name)
    permissionResponseRead.userId = permissionRead.userId
    if (permissionRead.workspaceId != null) {
      permissionResponseRead.scope = PermissionScope.WORKSPACE
      permissionResponseRead.scopeId = permissionRead.workspaceId
    } else if (permissionRead.organizationId != null) {
      permissionResponseRead.scope = PermissionScope.ORGANIZATION
      permissionResponseRead.scopeId = permissionRead.organizationId
    } else {
      permissionResponseRead.scope = PermissionScope.NONE
      permissionResponseRead.scopeId = null
    }
    return permissionResponseRead
  }
}
