/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support;

import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_AUTH_PROVIDER;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME;

import io.airbyte.config.AuthProvider;
import io.airbyte.config.AuthenticatedUser;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves JWT into UserRead object.
 */
@Singleton
public class JwtUserAuthenticationResolver implements UserAuthenticationResolver {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Optional<SecurityService> securityService;

  public JwtUserAuthenticationResolver(final Optional<SecurityService> securityService) {
    this.securityService = securityService;
  }

  /**
   * Resolves JWT token into a UserRead object with user metadata.
   */
  @Override
  public AuthenticatedUser resolveUser(final String expectedAuthUserId) {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty user.");
      return new AuthenticatedUser();
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
    return new AuthenticatedUser().withName(name).withEmail(email).withAuthUserId(authUserId).withAuthProvider(authProvider);
  }

  /**
   * Resolves JWT token to realm. If a realm is not set, it will return null.
   */
  @Override
  public Optional<String> resolveRealm() {
    if (securityService.isEmpty()) {
      log.warn("Security service is not available. Returning empty realm.");
      return Optional.empty();
    }

    final var jwtMap = securityService.get().getAuthentication().get().getAttributes();
    final String realm = (String) jwtMap.getOrDefault(JWT_SSO_REALM, null);
    return Optional.ofNullable(realm);
  }

}
