/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.publicApi.server.generated.models.PermissionResponse

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
  fun from(permissionRead: PermissionRead): PermissionResponse =
    PermissionResponse(
      permissionId = permissionRead.permissionId,
      permissionType = enumValueOf(permissionRead.permissionType.name),
      userId = permissionRead.userId,
      workspaceId = permissionRead.workspaceId,
      organizationId = permissionRead.organizationId,
    )
}
