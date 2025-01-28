/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.commons.server.errors.AuthException;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.persistence.UserPersistence;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.http.scope.RequestScope;
import io.micronaut.security.utils.SecurityService;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for retrieving the current Airbyte User associated with the current request. Replaces
 * the {@link CommunityCurrentUserService} when micronaut.security is enabled, ie in Enterprise and
 * Cloud. `@RequestScope` means one bean is created per request, so the current user is cached for
 * any subsequent calls to getCurrentUser() within the same request.
 */
@RequestScope
@Requires(property = "micronaut.security.enabled",
          value = "true")
@Replaces(CommunityCurrentUserService.class)
public class SecurityAwareCurrentUserService implements CurrentUserService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final UserPersistence userPersistence;
  private final SecurityService securityService;
  private AuthenticatedUser retrievedCurrentUser;

  public SecurityAwareCurrentUserService(final UserPersistence userPersistence,
                                         final SecurityService securityService) {
    this.userPersistence = userPersistence;
    this.securityService = securityService;
  }

  @Override
  public AuthenticatedUser getCurrentUser() {
    if (this.retrievedCurrentUser == null) {
      try {
        final String authUserId = securityService.username().orElseThrow();
        this.retrievedCurrentUser = userPersistence.getUserByAuthId(authUserId).orElseThrow();
        log.debug("Setting current user for request to: {}", retrievedCurrentUser);
      } catch (final Exception e) {
        throw new AuthException("Could not get the current Airbyte user due to an internal error.", e);
      }
    }
    return this.retrievedCurrentUser;
  }

}
