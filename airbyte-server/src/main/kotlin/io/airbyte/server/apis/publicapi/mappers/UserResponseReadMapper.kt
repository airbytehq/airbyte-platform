/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.OrganizationUserRead
import io.airbyte.public_api.model.generated.UserResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object UserResponseReadMapper {
  /**
   * Converts a UserRead object from the config api to a UserResponse.
   *
   * @param userRead Output of a user get from config api
   * @return UserResponse Response object with permission details
   */
  fun from(userRead: OrganizationUserRead): UserResponse {
    val userResponse = UserResponse()
    userResponse.userId = userRead.userId
    userResponse.name = userRead.name
    userResponse.email = userRead.email
    return userResponse
  }
}
