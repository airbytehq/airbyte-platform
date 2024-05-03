/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.public_api.model.generated.PermissionResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object PermissionReadMapper {
  /**
   * Converts a PermissionRead object from the config api to a PermissionResponse.
   *
   * @param permissionRead Output of a permission create from config api
   * @return PermissionResponse Response object with permission details
   */
  fun from(permissionRead: PermissionRead): PermissionResponse {
    val permissionResponse = PermissionResponse()
    permissionResponse.permissionId = permissionRead.permissionId
    permissionResponse.permissionType = enumValueOf(permissionRead.permissionType.name)
    permissionResponse.userId = permissionRead.userId
    permissionResponse.workspaceId = permissionRead.workspaceId
    permissionResponse.organizationId = permissionRead.organizationId
    return permissionResponse
  }
}
