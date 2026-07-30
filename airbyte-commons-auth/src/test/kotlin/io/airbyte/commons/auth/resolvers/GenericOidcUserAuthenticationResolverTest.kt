/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.resolvers

import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL_VERIFIED
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig.GenericOidcFieldMappingConfig
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.Optional

class GenericOidcUserAuthenticationResolverTest {
  private val securityService = mockk<SecurityService>()
  private val resolver = GenericOidcUserAuthenticationResolver(securityService, AirbyteAuthConfig())

  @Test
  fun `resolves a verified email claim only when it is boolean true`() {
    every { securityService.authentication } returns
      Optional.of(Authentication.build(AUTH_USER_ID, mapOf("email" to EMAIL, JWT_USER_EMAIL_VERIFIED to true)))

    assertEquals(EMAIL, resolver.resolveVerifiedEmail())
  }

  @Test
  fun `does not treat an absent email verification claim as verified`() {
    every { securityService.authentication } returns
      Optional.of(Authentication.build(AUTH_USER_ID, emptyMap()))

    assertNull(resolver.resolveVerifiedEmail())
  }

  @Test
  fun `resolves the standard verified email when a custom login email field matches it`() {
    val customResolver =
      GenericOidcUserAuthenticationResolver(
        securityService,
        AirbyteAuthConfig(
          identityProvider =
            AirbyteAuthIdentityProviderConfig(
              oidc =
                OidcIdentityProviderConfig(
                  fields = GenericOidcFieldMappingConfig(email = "upn"),
                ),
            ),
        ),
      )
    every { securityService.authentication } returns
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(
            "email" to EMAIL,
            "upn" to EMAIL,
            JWT_USER_EMAIL_VERIFIED to true,
          ),
        ),
      )

    assertEquals(EMAIL, customResolver.resolveVerifiedEmail())
  }

  @Test
  fun `resolves the standard verified email when a custom login email field differs`() {
    val customResolver =
      GenericOidcUserAuthenticationResolver(
        securityService,
        AirbyteAuthConfig(
          identityProvider =
            AirbyteAuthIdentityProviderConfig(
              oidc =
                OidcIdentityProviderConfig(
                  fields = GenericOidcFieldMappingConfig(email = "upn"),
                ),
            ),
        ),
      )
    every { securityService.authentication } returns
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(
            "email" to "verified@example.com",
            "upn" to "unverified@example.com",
            JWT_USER_EMAIL_VERIFIED to true,
          ),
        ),
      )

    assertEquals("verified@example.com", customResolver.resolveVerifiedEmail())
  }

  @Test
  fun `does not verify the auth user id fallback when the selected email claim is missing`() {
    every { securityService.authentication } returns
      Optional.of(
        Authentication.build(
          AUTH_USER_ID,
          mapOf(JWT_USER_EMAIL_VERIFIED to true),
        ),
      )

    assertNull(resolver.resolveVerifiedEmail())
  }

  companion object {
    private const val AUTH_USER_ID = "auth-user-id"
    private const val EMAIL = "verified@example.com"
  }
}
