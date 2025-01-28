/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_AUTH_PROVIDER;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL;
import static io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver;
import io.airbyte.config.AuthProvider;
import io.airbyte.config.AuthenticatedUser;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JwtUserAuthenticationResolverTest {

  private SecurityService securityService;

  private JwtUserAuthenticationResolver jwtUserAuthenticationResolver;

  private static final String EMAIL = "email@email.com";
  private static final String USER_NAME = "userName";
  private static final String AUTH_USER_ID = "auth_user_id";

  @BeforeEach
  void setup() {
    securityService = mock(SecurityService.class);
    jwtUserAuthenticationResolver = new JwtUserAuthenticationResolver(Optional.of(securityService));
  }

  @Test
  void testResolveUser_firebase() {
    when(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID));

    final Optional<Authentication> authentication =
        Optional.of(Authentication.build(AUTH_USER_ID,
            Map.of(JWT_USER_EMAIL, EMAIL, JWT_USER_NAME, USER_NAME, JWT_AUTH_PROVIDER, AuthProvider.GOOGLE_IDENTITY_PLATFORM)));
    when(securityService.getAuthentication()).thenReturn(authentication);

    final AuthenticatedUser userRead = jwtUserAuthenticationResolver.resolveUser(AUTH_USER_ID);

    final AuthenticatedUser expectedUserRead =
        new AuthenticatedUser().withAuthUserId(AUTH_USER_ID).withEmail(EMAIL).withName(USER_NAME).withAuthProvider(
            AuthProvider.GOOGLE_IDENTITY_PLATFORM);
    assertEquals(expectedUserRead, userRead);

    // In this case we do not have ssoRealm in the attributes; expecting not throw and treat it as a
    // request without realm.
    final Optional<String> realm = jwtUserAuthenticationResolver.resolveRealm();
    assertTrue(realm.isEmpty());
  }

  @Test
  void testResolveRealm_firebase() {
    when(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID));
    final Optional<Authentication> authentication =
        Optional.of(Authentication.build(AUTH_USER_ID, Map.of(JWT_AUTH_PROVIDER, AuthProvider.GOOGLE_IDENTITY_PLATFORM)));
    when(securityService.getAuthentication()).thenReturn(authentication);

    final Optional<String> realm = jwtUserAuthenticationResolver.resolveRealm();
    assertTrue(realm.isEmpty());
  }

  @Test
  void testResolveRealm_keycloak() {
    when(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID));
    final Optional<Authentication> authentication =
        Optional.of(Authentication.build(AUTH_USER_ID, Map.of(JWT_SSO_REALM, "airbyte")));
    when(securityService.getAuthentication()).thenReturn(authentication);
    final String realm = jwtUserAuthenticationResolver.resolveRealm().orElseThrow();
    assertEquals("airbyte", realm);
  }

}
