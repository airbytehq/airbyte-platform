/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.commons.auth.support.JwtTokenParser.JWT_AUTH_PROVIDER
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_SSO_REALM
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL_VERIFIED
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional

internal class JwtUserAuthenticationResolverTest {
  private lateinit var securityService: SecurityService

  private lateinit var jwtUserAuthenticationResolver: JwtUserAuthenticationResolver

  @BeforeEach
  fun setup() {
    securityService = mockk()
    jwtUserAuthenticationResolver = JwtUserAuthenticationResolver(securityService)
  }

  @Test
  fun testResolveUser_firebase() {
    every { securityService.username() } returns Optional.of(AUTH_USER_ID)

    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_USER_EMAIL to EMAIL, JWT_USER_NAME to USER_NAME, JWT_AUTH_PROVIDER to AuthProvider.GOOGLE_IDENTITY_PLATFORM),
        ),
      )
    every { securityService.getAuthentication() } returns authentication

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
  fun `resolves a verified email claim only when it is boolean true`() {
    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_USER_EMAIL to EMAIL, JWT_USER_EMAIL_VERIFIED to true),
        ),
      )
    every { securityService.getAuthentication() } returns authentication

    Assertions.assertEquals(EMAIL, jwtUserAuthenticationResolver.resolveVerifiedEmail())
  }

  @Test
  fun `does not treat a missing email verification claim as verified`() {
    val authentication: Optional<Authentication> =
      Optional.of(Authentication.build(AUTH_USER_ID, emptyMap()))
    every { securityService.getAuthentication() } returns authentication

    Assertions.assertNull(jwtUserAuthenticationResolver.resolveVerifiedEmail())
  }

  @Test
  fun `does not treat a string email verification claim as verified`() {
    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_USER_EMAIL_VERIFIED to "true"),
        ),
      )
    every { securityService.getAuthentication() } returns authentication

    Assertions.assertNull(jwtUserAuthenticationResolver.resolveVerifiedEmail())
  }

  @Test
  fun testResolveRealm_firebase() {
    every { securityService.username() } returns Optional.of(AUTH_USER_ID)
    val authentication: Optional<Authentication> =
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_AUTH_PROVIDER to AuthProvider.GOOGLE_IDENTITY_PLATFORM),
        ),
      )
    every { securityService.getAuthentication() } returns authentication

    Assertions.assertNull(jwtUserAuthenticationResolver.resolveRealm())
  }

  @Test
  fun testResolveRealm_keycloak() {
    every { securityService.username() } returns Optional.of(AUTH_USER_ID)
    val authentication: Optional<Authentication> =
      Optional.of(Authentication.build(AUTH_USER_ID, mapOf(JWT_SSO_REALM to "airbyte")))
    every { securityService.getAuthentication() } returns authentication
    val realm = jwtUserAuthenticationResolver.resolveRealm()
    Assertions.assertEquals("airbyte", realm)
  }

  companion object {
    private const val EMAIL = "email@email.com"
    private const val USER_NAME = "userName"
    private const val AUTH_USER_ID = "auth_user_id"
  }
}
