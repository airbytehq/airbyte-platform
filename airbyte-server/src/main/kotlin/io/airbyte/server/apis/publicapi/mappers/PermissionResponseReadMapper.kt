/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.publicApi.server.generated.models.PermissionResponseRead
import io.airbyte.publicApi.server.generated.models.PermissionScope
import java.util.UUID

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
  fun from(permissionRead: PermissionRead): PermissionResponseRead =
    PermissionResponseRead(
      permissionId = permissionRead.permissionId,
      permissionType = enumValueOf(permissionRead.permissionType.name),
      userId = permissionRead.userId,
      scope =
        if (permissionRead.workspaceId != null) {
          PermissionScope.WORKSPACE
        } else if (permissionRead.organizationId != null) {
          PermissionScope.ORGANIZATION
        } else {
          PermissionScope.NONE
        },
      scopeId =
        if (permissionRead.workspaceId != null) {
          permissionRead.workspaceId
        } else if (permissionRead.organizationId != null) {
          permissionRead.organizationId
        } else {
          UUID.fromString("00000000-0000-0000-0000-000000000000")
        },
    )
}
