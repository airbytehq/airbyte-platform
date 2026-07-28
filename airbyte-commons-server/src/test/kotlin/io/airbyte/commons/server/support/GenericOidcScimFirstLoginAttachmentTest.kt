/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.commons.auth.resolvers.GenericOidcUserAuthenticationResolver
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL_VERIFIED
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.ScimFirstLoginUserRow
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.ScimAuthUser
import io.airbyte.domain.services.scim.ScimFirstLoginAttachmentResult
import io.airbyte.domain.services.scim.ScimFirstLoginService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig
import io.airbyte.micronaut.runtime.AirbyteAuthConfig.AirbyteAuthIdentityProviderConfig.OidcIdentityProviderConfig.GenericOidcFieldMappingConfig
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class GenericOidcScimFirstLoginAttachmentTest {
  private val securityService = mockk<SecurityService>()
  private val mappingRepository = mockk<ScimResourceMappingRepository>()
  private val userRepository = mockk<ScimAirbyteUserRepository>()
  private val authUserRepository = mockk<ScimAuthUserRepository>()
  private val resolver =
    GenericOidcUserAuthenticationResolver(
      securityService,
      AirbyteAuthConfig(
        identityProvider =
          AirbyteAuthIdentityProviderConfig(
            oidc =
              OidcIdentityProviderConfig(
                fields = GenericOidcFieldMappingConfig(email = CUSTOM_EMAIL_CLAIM),
              ),
          ),
      ),
    )
  private val service = ScimFirstLoginService(mappingRepository, userRepository, authUserRepository)

  @BeforeEach
  fun setUp() {
    every { securityService.username() } returns Optional.of(AUTH_USER_ID)
    every { userRepository.acquireGlobalEmailLock(LOGIN_EMAIL) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(LOGIN_EMAIL) } returns
      listOf(ScimFirstLoginUserRow(USER_ID))
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()
  }

  @Test
  fun `custom login claim matching the verified standard email attaches`() {
    val savedIdentity = slot<ScimAuthUser>()
    every { authUserRepository.save(capture(savedIdentity)) } answers { savedIdentity.captured }

    val result =
      attach(
        mapOf(
          CUSTOM_EMAIL_CLAIM to LOGIN_EMAIL,
          STANDARD_EMAIL_CLAIM to LOGIN_EMAIL,
          JWT_USER_EMAIL_VERIFIED to true,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Attached(USER_ID))
    assertThat(savedIdentity.captured.userId).isEqualTo(USER_ID)
    assertThat(savedIdentity.captured.authUserId).isEqualTo(AUTH_USER_ID)
  }

  @Test
  fun `verified standard email attaches when the configured login claim differs`() {
    val savedIdentity = slot<ScimAuthUser>()
    every { userRepository.acquireGlobalEmailLock(CUSTOM_LOGIN_EMAIL) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(CUSTOM_LOGIN_EMAIL) } returns emptyList()
    every { authUserRepository.save(capture(savedIdentity)) } answers { savedIdentity.captured }

    val result =
      attach(
        mapOf(
          CUSTOM_EMAIL_CLAIM to CUSTOM_LOGIN_EMAIL,
          STANDARD_EMAIL_CLAIM to LOGIN_EMAIL,
          JWT_USER_EMAIL_VERIFIED to true,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Attached(USER_ID))
    assertThat(savedIdentity.captured.userId).isEqualTo(USER_ID)
    assertThat(savedIdentity.captured.authUserId).isEqualTo(AUTH_USER_ID)
  }

  @Test
  fun `verified standard email attaches when the configured login claim is missing`() {
    val savedIdentity = slot<ScimAuthUser>()
    every { userRepository.acquireGlobalEmailLock(AUTH_USER_ID) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(AUTH_USER_ID) } returns emptyList()
    every { authUserRepository.save(capture(savedIdentity)) } answers { savedIdentity.captured }

    val result =
      attach(
        mapOf(
          STANDARD_EMAIL_CLAIM to LOGIN_EMAIL,
          JWT_USER_EMAIL_VERIFIED to true,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Attached(USER_ID))
    assertThat(savedIdentity.captured.userId).isEqualTo(USER_ID)
    assertThat(savedIdentity.captured.authUserId).isEqualTo(AUTH_USER_ID)
  }

  @Test
  fun `configured login claim matching a mapping fails closed when the verified standard email differs`() {
    every { userRepository.acquireGlobalEmailLock(DIFFERENT_VERIFIED_EMAIL) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(DIFFERENT_VERIFIED_EMAIL) } returns emptyList()

    val result =
      attach(
        mapOf(
          CUSTOM_EMAIL_CLAIM to LOGIN_EMAIL,
          STANDARD_EMAIL_CLAIM to DIFFERENT_VERIFIED_EMAIL,
          JWT_USER_EMAIL_VERIFIED to true,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `custom login claim without a standard email fails closed`() {
    val result =
      attach(
        mapOf(
          CUSTOM_EMAIL_CLAIM to LOGIN_EMAIL,
          JWT_USER_EMAIL_VERIFIED to true,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `custom login claim with an unverified standard email fails closed`() {
    val result =
      attach(
        mapOf(
          CUSTOM_EMAIL_CLAIM to LOGIN_EMAIL,
          STANDARD_EMAIL_CLAIM to LOGIN_EMAIL,
          JWT_USER_EMAIL_VERIFIED to false,
        ),
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  private fun attach(attributes: Map<String, Any>): ScimFirstLoginAttachmentResult {
    every { securityService.authentication } returns Optional.of(Authentication.build(AUTH_USER_ID, attributes))
    val authenticatedUser = resolver.resolveUser(AUTH_USER_ID)
    return service.attachIfPreProvisioned(
      authenticatedUser.email,
      resolver.resolveVerifiedEmail(),
      authenticatedUser.authUserId,
      authenticatedUser.authProvider,
    )
  }

  private companion object {
    const val AUTH_USER_ID = "auth-user-id"
    const val CUSTOM_EMAIL_CLAIM = "upn"
    const val CUSTOM_LOGIN_EMAIL = "custom@example.com"
    const val DIFFERENT_VERIFIED_EMAIL = "different@example.com"
    const val LOGIN_EMAIL = "alice@example.com"
    const val STANDARD_EMAIL_CLAIM = "email"
    val USER_ID: UUID = UUID.randomUUID()
  }
}
