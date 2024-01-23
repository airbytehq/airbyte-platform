/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.config.AuthProvider;
import io.airbyte.config.User;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Authentication resolver for local development. Always returns a user with the expected
 * authUserId, regardless of the actual signup/sign-in process.
 * <p>
 * This resolver should only ever be used for local development. Specifically to bypass our normal
 * authentication flow by hard-coding an instance-admin `X-Endpoint-API-UserInfo` header on all
 * incoming requests.
 * <p>
 * For running things like acceptance tests locally, real auth logic is needed so this resolver
 * should not be used. (it can be turned off by setting
 * `airbyte.auth.local.use-hardcoded-admin-header` to `false`)
 */
@Singleton
@Requires(property = "airbyte.auth.local.use-hardcoded-admin-header",
          value = "true")
@Requires(env = "local-test")
@Replaces(JwtUserAuthenticationResolver.class)
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
