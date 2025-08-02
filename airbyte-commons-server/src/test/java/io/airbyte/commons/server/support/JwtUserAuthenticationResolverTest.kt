/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.commons.auth.support.JwtTokenParser.JWT_AUTH_PROVIDER
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.Optional

internal class JwtUserAuthenticationResolverTest {
  private lateinit var securityService: SecurityService

  private lateinit var jwtUserAuthenticationResolver: JwtUserAuthenticationResolver

  @BeforeEach
  fun setup() {
    securityService = Mockito.mock(SecurityService::class.java)
    jwtUserAuthenticationResolver = JwtUserAuthenticationResolver(securityService)
  }

  @Test
  fun testResolveUser_firebase() {
    Mockito.`when`(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID))

    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_USER_EMAIL to EMAIL, JWT_USER_NAME to USER_NAME, JWT_AUTH_PROVIDER to AuthProvider.GOOGLE_IDENTITY_PLATFORM),
        ),
      )
    Mockito.`when`(securityService.getAuthentication()).thenReturn(authentication)

    val userRead = jwtUserAuthenticationResolver.resolveUser(AUTH_USER_ID)

    val expectedUserRead =
      AuthenticatedUser().withAuthUserId(AUTH_USER_ID).withEmail(EMAIL).withName(USER_NAME).withAuthProvider(
        AuthProvider.GOOGLE_IDENTITY_PLATFORM,
      )
    Assertions.assertEquals(expectedUserRead, userRead)

    // In this case we do not have ssoRealm in the attributes; expecting not throw and treat it as a
    // request without realm.
    Assertions.assertNull(jwtUserAuthenticationResolver.resolveRealm())
  }

  @Test
  fun testResolveRealm_firebase() {
    Mockito.`when`(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID))
    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_AUTH_PROVIDER to AuthProvider.GOOGLE_IDENTITY_PLATFORM),
        ),
      )
    Mockito.`when`(securityService.getAuthentication()).thenReturn(authentication)

    Assertions.assertNull(jwtUserAuthenticationResolver.resolveRealm())
  }

  @Test
  fun testResolveRealm_keycloak() {
    Mockito.`when`(securityService.username()).thenReturn(Optional.of(AUTH_USER_ID))
    val authentication: Optional<Authentication> =
      Optional.of(Authentication.build(AUTH_USER_ID, mapOf(JWT_SSO_REALM to "airbyte")))
    Mockito.`when`(securityService.getAuthentication()).thenReturn(authentication)
    val realm = jwtUserAuthenticationResolver.resolveRealm()
    Assertions.assertEquals("airbyte", realm)
  }

  companion object {
    private const val EMAIL = "email@email.com"
    private const val USER_NAME = "userName"
    private const val AUTH_USER_ID = "auth_user_id"
  }
}
