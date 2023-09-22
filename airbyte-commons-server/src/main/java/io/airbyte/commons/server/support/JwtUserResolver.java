/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.JwtTokenParser.JWT_AUTH_PROVIDER;
import static io.airbyte.commons.server.support.JwtTokenParser.JWT_SSO_REALM;
import static io.airbyte.commons.server.support.JwtTokenParser.JWT_USER_EMAIL;
import static io.airbyte.commons.server.support.JwtTokenParser.JWT_USER_NAME;

import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves JWT into UserRead object.
 */
@Singleton
@Slf4j
public class JwtUserResolver {

  private final Optional<SecurityService> securityService;

  public JwtUserResolver(final Optional<SecurityService> securityService) {
    this.securityService = securityService;
  }

  /**
   * Resolves JWT token into a UserRead object with user metadata.
   */
  public User resolveUser() {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty user.");
      return new User();
    }
    final String authUserId = securityService.get().username().get();

    final var jwtMap = securityService.get().getAuthentication().get().getAttributes();
    final String email = (String) jwtMap.get(JWT_USER_EMAIL);
    // Default name to email address if name is not found
    final String name = (String) jwtMap.getOrDefault(JWT_USER_NAME, email);
    final AuthProvider authProvider = (AuthProvider) jwtMap.getOrDefault(JWT_AUTH_PROVIDER, null);
    return new User().withName(name).withEmail(email).withAuthUserId(authUserId).withAuthProvider(authProvider);
  }

  /**
   * Resolves JWT token to SsoRealm. If Sso realm does not exist, it will return null.
   */
  public String resolveSsoRealm() {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty user.");
      return null;
    }

    final var jwtMap = securityService.get().getAuthentication().get().getAttributes();
    return (String) jwtMap.getOrDefault(JWT_SSO_REALM, null);
  }

  public boolean isJwtUserResolverEnabled() {
    return securityService.isPresent();
  }

}
