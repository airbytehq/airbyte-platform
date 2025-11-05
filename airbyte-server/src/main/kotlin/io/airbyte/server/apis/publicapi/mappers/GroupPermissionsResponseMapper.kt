/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.GroupPermissionResponse
import io.airbyte.api.server.generated.models.GroupPermissionsResponse

/**
 * Mappers that help convert a list of GroupPermissionResponse to GroupPermissionsResponse for the public API.
 */
object GroupPermissionsResponseMapper {
  /**
   * Converts a list of GroupPermissionResponse objects to a GroupPermissionsResponse.
   *
   * @param permissions List of GroupPermissionResponse objects
   * @return GroupPermissionsResponse containing the list of permissions
   */
  fun from(permissions: List<GroupPermissionResponse>): GroupPermissionsResponse =
    GroupPermissionsResponse(
      `data` = permissions,
    )
}
