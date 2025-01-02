/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.persistence.UserPersistence;
import io.micronaut.runtime.http.scope.RequestScope;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link CurrentUserService} that uses the default user from the
 * {@link UserPersistence}. Community edition of Airbyte doesn't surface the concept of real users,
 * so this implementation simply returns the default user that ships with the application.
 * `@RequestScope` means one bean is created per request, so the default user is cached for any
 * subsequent calls to getCurrentUser() within the same request.
 */
@RequestScope
public class CommunityCurrentUserService implements CurrentUserService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final UserPersistence userPersistence;
  private AuthenticatedUser retrievedDefaultUser;

  public CommunityCurrentUserService(final UserPersistence userPersistence) {
    this.userPersistence = userPersistence;
  }

  @Override
  public AuthenticatedUser getCurrentUser() {
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
