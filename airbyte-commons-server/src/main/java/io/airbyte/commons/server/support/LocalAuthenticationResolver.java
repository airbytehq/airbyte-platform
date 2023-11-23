/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Authentication resolver for local development. Always returns a user with the expected
 * authUserId, regardless of the actual signup/sign-in process.
 */
@Singleton
@Requires(env = "local-test")
@Slf4j
public class LocalAuthenticationResolver implements UserAuthenticationResolver {

  @Override
  public User resolveUser(final String expectedAuthUserId) {
    try {
      return new User()
          .withAuthUserId(expectedAuthUserId)
          .withAuthProvider(AuthProvider.AIRBYTE)
          .withName("local")
          .withEmail("local@airbyte.io");
    } catch (final Exception e) {
      throw new RuntimeException("Could not resolve local default user.", e);
    }
  }

  @Override
  public String resolveSsoRealm() {
    return null;
  }

}
