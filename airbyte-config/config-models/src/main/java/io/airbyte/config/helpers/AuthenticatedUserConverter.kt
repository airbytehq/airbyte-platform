/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.User

object AuthenticatedUserConverter {
  @JvmStatic
  fun toUser(user: AuthenticatedUser): User =
    User()
      .withUserId(user.userId)
      .withName(user.name)
      .withDefaultWorkspaceId(user.defaultWorkspaceId)
      .withStatus(user.status)
      .withCompanyName(user.companyName)
      .withEmail(user.email)
      .withNews(user.news)
      .withUiMetadata(user.uiMetadata)

  @JvmStatic
  fun toAuthenticatedUser(
    user: User,
    authUserId: String?,
    authProvider: AuthProvider?,
  ): AuthenticatedUser =
    AuthenticatedUser()
      .withUserId(user.userId)
      .withAuthUserId(authUserId)
      .withAuthProvider(authProvider)
      .withName(user.name)
      .withDefaultWorkspaceId(user.defaultWorkspaceId)
      .withStatus(user.status)
      .withCompanyName(user.companyName)
      .withEmail(user.email)
      .withNews(user.news)
      .withUiMetadata(user.uiMetadata)
}
