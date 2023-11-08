/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.config.User;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalAuthenticationResolverTest {

  private LocalAuthenticationResolver localAuthenticationResolver;

  private static final String AUTH_USER_ID = UUID.randomUUID().toString();

  @BeforeEach
  void setup() {
    localAuthenticationResolver = new LocalAuthenticationResolver();
  }

  @Test
  void testResolveUserAlwaysUsesProvidedAuthUserId() {
    final User userRead = localAuthenticationResolver.resolveUser(AUTH_USER_ID);
    assertEquals(AUTH_USER_ID, userRead.getAuthUserId());
  }

  @Test
  void testResolveSsoRealmAlwaysNull() {
    final String ssoRealm = localAuthenticationResolver.resolveSsoRealm();
    assertNull(ssoRealm);
  }

}
