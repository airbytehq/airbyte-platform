/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

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

}
