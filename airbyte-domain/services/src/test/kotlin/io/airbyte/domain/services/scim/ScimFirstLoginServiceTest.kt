/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.config.AuthProvider
import io.airbyte.data.repositories.ScimAirbyteUserRepository
import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.ScimFirstLoginUserRow
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.ScimAuthUser
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as DbAuthProvider

class ScimFirstLoginServiceTest {
  private val mappingRepository = mockk<ScimResourceMappingRepository>()
  private val userRepository = mockk<ScimAirbyteUserRepository>()
  private val authUserRepository = mockk<ScimAuthUserRepository>()
  private val service = ScimFirstLoginService(mappingRepository, userRepository, authUserRepository)

  @BeforeEach
  fun setUp() {
    every { userRepository.acquireGlobalEmailLock(EMAIL) } returns true
  }

  @Test
  fun `returns no match only after confirming the authentication identity is absent`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns emptyList()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result = service.attachIfPreProvisioned(EMAIL, EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.NoMatch)
    verify(exactly = 1) { authUserRepository.acquireIdentityLock(AUTH_USER_ID) }
    verify(exactly = 1) { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) }
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `fails closed when the expected unmapped User gained SCIM ownership under another email`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns emptyList()
    every { mappingRepository.findUsersByUserIdForUpdate(USER_ID) } returns listOf(ScimFirstLoginUserRow(USER_ID))
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result =
      service.attachIfPreProvisioned(
        EMAIL,
        EMAIL,
        AUTH_USER_ID,
        AuthProvider.KEYCLOAK,
        expectedUnmappedUserId = USER_ID,
      )

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    verify(exactly = 1) { mappingRepository.findUsersByUserIdForUpdate(USER_ID) }
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `returns the unique existing identity owner when no SCIM mapping matches`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns emptyList()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every {
      authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID)
    } returns
      listOf(
        ScimAuthUser(
          userId = USER_ID,
          authUserId = AUTH_USER_ID,
          authProvider = DbAuthProvider.keycloak,
        ),
      )

    val result = service.attachIfPreProvisioned(EMAIL, EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.ExistingIdentity(USER_ID))
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `fails closed when no SCIM mapping matches an authentication identity with multiple owners`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns emptyList()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every {
      authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID)
    } returns
      listOf(
        ScimAuthUser(
          userId = USER_ID,
          authUserId = AUTH_USER_ID,
          authProvider = DbAuthProvider.keycloak,
        ),
        ScimAuthUser(
          userId = OTHER_USER_ID,
          authUserId = AUTH_USER_ID,
          authProvider = DbAuthProvider.google_identity_platform,
        ),
      )

    val result = service.attachIfPreProvisioned(EMAIL, EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.AmbiguousIdentity)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `attaches one identity when active and inactive mappings across organizations identify the same User`() {
    val saved = slot<ScimAuthUser>()
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns
      listOf(ScimFirstLoginUserRow(USER_ID), ScimFirstLoginUserRow(USER_ID))
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()
    every { authUserRepository.save(capture(saved)) } answers { saved.captured }

    val result = service.attachIfPreProvisioned(EMAIL.uppercase(), EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Attached(USER_ID))
    assertThat(saved.captured.userId).isEqualTo(USER_ID)
    assertThat(saved.captured.authUserId).isEqualTo(AUTH_USER_ID)
    assertThat(saved.captured.authProvider).isEqualTo(DbAuthProvider.keycloak)
  }

  @Test
  fun `fails closed without identity writes when matching mappings identify different Users`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns
      listOf(ScimFirstLoginUserRow(USER_ID), ScimFirstLoginUserRow(OTHER_USER_ID))
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result = service.attachIfPreProvisioned(EMAIL, EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `returns idempotently when the identity is already attached to the mapped User`() {
    mappedUser()
    val existing =
      ScimAuthUser(
        userId = USER_ID,
        authUserId = AUTH_USER_ID,
        authProvider = DbAuthProvider.keycloak,
      )
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns listOf(existing)

    val result = service.attachIfPreProvisioned(EMAIL, null, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.AlreadyAttached(USER_ID))
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `returns idempotently when the raw identity is attached to the mapped User under another provider`() {
    mappedUser()
    val existing =
      ScimAuthUser(
        userId = USER_ID,
        authUserId = AUTH_USER_ID,
        authProvider = DbAuthProvider.google_identity_platform,
      )
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns listOf(existing)

    val result = service.attachIfPreProvisioned(EMAIL, null, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.AlreadyAttached(USER_ID))
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `fails closed when the authentication id is attached to another User under another provider`() {
    mappedUser()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every {
      authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID)
    } returns
      listOf(
        ScimAuthUser(
          userId = OTHER_USER_ID,
          authUserId = AUTH_USER_ID,
          authProvider = DbAuthProvider.google_identity_platform,
        ),
      )

    val result = service.attachIfPreProvisioned(EMAIL, EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `does not attach when the matching login email is not verified`() {
    mappedUser()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result = service.attachIfPreProvisioned(EMAIL, null, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `does not attach when the verified standard email differs from the custom login email`() {
    mappedUser()
    every { userRepository.acquireGlobalEmailLock(DIFFERENT_EMAIL) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(DIFFERENT_EMAIL) } returns emptyList()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result = service.attachIfPreProvisioned(EMAIL, DIFFERENT_EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `fails closed when verified and configured login emails map to different Users`() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns listOf(ScimFirstLoginUserRow(USER_ID))
    every { userRepository.acquireGlobalEmailLock(DIFFERENT_EMAIL) } returns true
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(DIFFERENT_EMAIL) } returns
      listOf(ScimFirstLoginUserRow(OTHER_USER_ID))
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()
    every { authUserRepository.save(any()) } answers { firstArg() }

    val result = service.attachIfPreProvisioned(EMAIL, DIFFERENT_EMAIL, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.Conflict)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  @Test
  fun `does not attach when the matching login email has no verification claim`() {
    mappedUser()
    every { authUserRepository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { authUserRepository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns emptyList()

    val result = service.attachIfPreProvisioned(EMAIL, null, AUTH_USER_ID, AuthProvider.KEYCLOAK)

    assertThat(result).isEqualTo(ScimFirstLoginAttachmentResult.EmailNotVerified)
    verify(exactly = 0) { authUserRepository.save(any()) }
  }

  private fun mappedUser() {
    every { mappingRepository.findUsersByPrimaryEmailForUpdate(EMAIL) } returns listOf(ScimFirstLoginUserRow(USER_ID))
  }

  companion object {
    private const val EMAIL = "current@example.com"
    private const val DIFFERENT_EMAIL = "different@example.com"
    private const val AUTH_USER_ID = "auth-user-id"
    private val USER_ID = UUID.randomUUID()
    private val OTHER_USER_ID = UUID.randomUUID()
  }
}
