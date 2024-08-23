/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.AuthProvider;
import io.airbyte.config.User;
import io.airbyte.config.UserInfo;

public class UserInfoConverter {

  public static UserInfo userInfoFromUser(final User user) {
    return new UserInfo()
        .withUserId(user.getUserId())
        .withName(user.getName())
        .withDefaultWorkspaceId(user.getDefaultWorkspaceId())
        .withStatus(user.getStatus())
        .withCompanyName(user.getCompanyName())
        .withEmail(user.getEmail())
        .withNews(user.getNews())
        .withUiMetadata(user.getUiMetadata());
  }

  public static User userFromUserInfo(final UserInfo userInfo, final String authUserId, final AuthProvider authProvider) {
    return new User()
        .withUserId(userInfo.getUserId())
        .withAuthUserId(authUserId)
        .withAuthProvider(authProvider)
        .withName(userInfo.getName())
        .withDefaultWorkspaceId(userInfo.getDefaultWorkspaceId())
        .withStatus(userInfo.getStatus())
        .withCompanyName(userInfo.getCompanyName())
        .withEmail(userInfo.getEmail())
        .withNews(userInfo.getNews())
        .withUiMetadata(userInfo.getUiMetadata());
  }

}
