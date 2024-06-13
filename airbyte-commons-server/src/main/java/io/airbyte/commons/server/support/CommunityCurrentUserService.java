/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.User;
import io.airbyte.config.persistence.UserPersistence;
import io.micronaut.runtime.http.scope.RequestScope;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link CurrentUserService} that uses the default user from the
 * {@link UserPersistence}. Community edition of Airbyte doesn't surface the concept of real users,
 * so this implementation simply returns the default user that ships with the application.
 * `@RequestScope` means one bean is created per request, so the default user is cached for any
 * subsequent calls to getCurrentUser() within the same request.
 */
@Slf4j
@RequestScope
public class CommunityCurrentUserService implements CurrentUserService {

  private final UserPersistence userPersistence;
  private User retrievedDefaultUser;

  public CommunityCurrentUserService(final UserPersistence userPersistence) {
    this.userPersistence = userPersistence;
  }

  @Override
  public User getCurrentUser() {
    if (this.retrievedDefaultUser == null) {
      try {
        this.retrievedDefaultUser = userPersistence.getDefaultUser().orElseThrow();
        log.debug("Setting current user for request to retrieved default user: {}", retrievedDefaultUser);
      } catch (final Exception e) {
        throw new RuntimeException("Could not get the current user due to an internal error.", e);
      }
    }
    return this.retrievedDefaultUser;
  }

}
