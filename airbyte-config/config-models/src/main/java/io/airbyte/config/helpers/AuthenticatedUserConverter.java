/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.AuthProvider;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.User;

public class AuthenticatedUserConverter {

  public static User toUser(final AuthenticatedUser user) {
    return new User()
        .withUserId(user.getUserId())
        .withName(user.getName())
        .withDefaultWorkspaceId(user.getDefaultWorkspaceId())
        .withStatus(user.getStatus())
        .withCompanyName(user.getCompanyName())
        .withEmail(user.getEmail())
        .withNews(user.getNews())
        .withUiMetadata(user.getUiMetadata());
  }

  public static AuthenticatedUser toAuthenticatedUser(final User user, final String authUserId, final AuthProvider authProvider) {
    return new AuthenticatedUser()
        .withUserId(user.getUserId())
        .withAuthUserId(authUserId)
        .withAuthProvider(authProvider)
        .withName(user.getName())
        .withDefaultWorkspaceId(user.getDefaultWorkspaceId())
        .withStatus(user.getStatus())
        .withCompanyName(user.getCompanyName())
        .withEmail(user.getEmail())
        .withNews(user.getNews())
        .withUiMetadata(user.getUiMetadata());
  }

}
