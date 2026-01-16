/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.OrganizationUserRead
import io.airbyte.publicApi.server.generated.models.UserResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object UserReadMapper {
  /**
   * Converts an OrganizationUserRead object from the config api to a UserResponse.
   *
   * @param organizationUserRead Output of an organizationUser get from config api
   * @return UserResponse User response object
   */
  fun from(organizationUserRead: OrganizationUserRead): UserResponse =
    UserResponse(
      id = organizationUserRead.userId,
      name = organizationUserRead.name,
      email = organizationUserRead.email,
    )
}
