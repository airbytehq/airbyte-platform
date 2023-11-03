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
import io.micronaut.context.annotation.Requires;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves JWT into UserRead object.
 */
@Singleton
@Slf4j
@Requires(notEnv = "local-test")
public class JwtUserAuthenticationResolver implements UserAuthenticationResolver {

  private final Optional<SecurityService> securityService;

  public JwtUserAuthenticationResolver(final Optional<SecurityService> securityService) {
    this.securityService = securityService;
  }

  /**
   * Resolves JWT token into a UserRead object with user metadata.
   */
  @Override
  public User resolveUser(final String expectedAuthUserId) {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty user.");
      return new User();
    }
    final String authUserId = securityService.get().username().get();
    if (!expectedAuthUserId.equals(authUserId)) {
      throw new IllegalArgumentException("JWT token doesn't match the expected auth user id.");
    }

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
  @Override
  public String resolveSsoRealm() {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty user.");
      return null;
    }

    final var jwtMap = securityService.get().getAuthentication().get().getAttributes();
    return (String) jwtMap.getOrDefault(JWT_SSO_REALM, null);
  }

}
