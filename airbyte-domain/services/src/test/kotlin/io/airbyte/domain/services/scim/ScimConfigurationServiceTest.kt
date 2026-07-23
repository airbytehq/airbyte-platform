/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimIdpProvider
import io.airbyte.domain.models.scim.ScimOrganizationNotFoundException
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class ScimConfigurationServiceTest {
  private val scimAccessGate = mockk<ScimAccessGate>()
  private val organizationRepository = mockk<OrganizationRepository>()
  private val scimConfigurationRepository = mockk<ScimConfigurationRepository>()
  private val tokenService = mockk<ScimTokenService>()
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val userId = UserId(UUID.randomUUID())
  private val service =
    ScimConfigurationService(
      scimAccessGate,
      organizationRepository,
      scimConfigurationRepository,
      tokenService,
      ImmediateConfigTransactionOperations(),
    )

  @BeforeEach
  fun setUp() {
    clearMocks(scimAccessGate, organizationRepository, scimConfigurationRepository, tokenService)
  }

  @Test
  fun `status reports not configured without taking locks or evaluating gates`() {
    every { scimConfigurationRepository.findByOrganizationId(organizationId.value) } returns null

    val result = service.getConfiguration(organizationId)

    assertEquals(ScimConfigurationStatus.NOT_CONFIGURED, result.status)
    assertNull(result.idpProvider)
    assertNull(result.createdAt)
    assertNull(result.updatedAt)
    assertNull(result.token)
    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
    verify(exactly = 0) { organizationRepository.findByIdForUpdate(any()) }
    verify(exactly = 0) { scimConfigurationRepository.findByOrganizationIdForUpdate(any()) }
  }

  @Test
  fun `status maps an enabled configuration without exposing its token hash`() {
    val createdAt = OffsetDateTime.now().minusDays(1)
    val updatedAt = OffsetDateTime.now()
    every { scimConfigurationRepository.findByOrganizationId(organizationId.value) } returns
      enabledConfiguration(
        idpProvider = ScimIdpProvider.OKTA.storageValue,
        createdAt = createdAt,
        updatedAt = updatedAt,
      )

    val result = service.getConfiguration(organizationId)

    assertEquals(ScimConfigurationStatus.ENABLED, result.status)
    assertEquals(ScimIdpProvider.OKTA, result.idpProvider)
    assertEquals(createdAt, result.createdAt)
    assertEquals(updatedAt, result.updatedAt)
    assertNull(result.token)
  }

  @Test
  fun `status maps a disabled configuration`() {
    every { scimConfigurationRepository.findByOrganizationId(organizationId.value) } returns
      ScimConfiguration(
        organizationId = organizationId.value,
        idpProvider = ScimIdpProvider.MICROSOFT_ENTRA_ID.storageValue,
        enabled = false,
        disabledAt = OffsetDateTime.now(),
      )

    val result = service.getConfiguration(organizationId)

    assertEquals(ScimConfigurationStatus.DISABLED, result.status)
    assertEquals(ScimIdpProvider.MICROSOFT_ENTRA_ID, result.idpProvider)
    assertNull(result.token)
  }

  @Test
  fun `initial enable denies access before locking or generating token material`() {
    every { scimAccessGate.isAllowed(organizationId) } returns false

    assertThrows<ScimAccessDeniedException> {
      service.enable(organizationId, ScimIdpProvider.OKTA, userId)
    }

    verify(exactly = 0) { organizationRepository.findByIdForUpdate(any()) }
    verify(exactly = 0) { scimConfigurationRepository.findByOrganizationIdForUpdate(any()) }
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.save(any()) }
  }

  @Test
  fun `initial enable locks the organization and configuration before issuing one token`() {
    val rawToken = "airbyte_scim_${"a".repeat(64)}"
    val tokenHash = "b".repeat(64)
    every { scimAccessGate.isAllowed(organizationId) } returns true
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns null
    every { tokenService.generateToken() } returns rawToken
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { scimConfigurationRepository.save(any()) } answers {
      firstArg<ScimConfiguration>().apply {
        createdAt = OffsetDateTime.now()
        updatedAt = createdAt
      }
    }

    val result = service.enable(organizationId, ScimIdpProvider.OKTA, userId)

    assertEquals(ScimConfigurationStatus.ENABLED, result.status)
    assertEquals(ScimIdpProvider.OKTA, result.idpProvider)
    assertEquals(rawToken, result.token)
    assertTrue(result.createdAt != null)
    assertEquals(result.createdAt, result.updatedAt)
    verifyOrder {
      scimAccessGate.isAllowed(organizationId)
      organizationRepository.findByIdForUpdate(organizationId.value)
      scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
      tokenService.generateToken()
      tokenService.hashToken(rawToken)
      scimConfigurationRepository.save(
        withArg { configuration ->
          assertEquals(organizationId.value, configuration.organizationId)
          assertEquals(tokenHash, configuration.tokenHash)
          assertEquals(ScimIdpProvider.OKTA.storageValue, configuration.idpProvider)
          assertTrue(configuration.enabled)
          assertEquals(userId.value, configuration.createdByUserId)
          assertEquals(userId.value, configuration.tokenIssuedByUserId)
          assertTrue(configuration.tokenIssuedAt != null)
          assertNull(configuration.disabledAt)
          assertNull(configuration.disabledByUserId)
        },
      )
    }
  }

  @Test
  fun `initial enable propagates repository failures`() {
    val failure = IllegalStateException("database unavailable")
    val rawToken = "airbyte_scim_${"a".repeat(64)}"
    every { scimAccessGate.isAllowed(organizationId) } returns true
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns null
    every { tokenService.generateToken() } returns rawToken
    every { tokenService.hashToken(rawToken) } returns "token-hash"
    every { scimConfigurationRepository.save(any()) } throws failure

    val thrown =
      assertThrows<IllegalStateException> {
        service.enable(organizationId, ScimIdpProvider.OKTA, userId)
      }

    assertSame(failure, thrown)
  }

  @Test
  fun `enable is a tokenless no-op when already enabled with the same provider`() {
    val existing = enabledConfiguration()
    stubLockedConfiguration(existing)

    val result = service.enable(organizationId, ScimIdpProvider.OKTA, userId)

    assertEquals(ScimConfigurationStatus.ENABLED, result.status)
    assertEquals(ScimIdpProvider.OKTA, result.idpProvider)
    assertNull(result.token)
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { tokenService.hashToken(any()) }
    verify(exactly = 0) { scimConfigurationRepository.save(any()) }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
    verify(exactly = 0) { scimConfigurationRepository.disableByIdAndOrganizationId(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `enable rejects changing the provider on an enabled configuration`() {
    stubLockedConfiguration(enabledConfiguration())

    val exception =
      assertThrows<ScimConfigurationConflictException> {
        service.enable(organizationId, ScimIdpProvider.MICROSOFT_ENTRA_ID, userId)
      }

    assertEquals("SCIM is already enabled with a different identity provider", exception.message)
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
    verify(exactly = 0) { scimConfigurationRepository.disableByIdAndOrganizationId(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `enable rejects a disabled configuration for PLAT-940 reconciliation`() {
    stubLockedConfiguration(
      ScimConfiguration(
        organizationId = organizationId.value,
        idpProvider = ScimIdpProvider.OKTA.storageValue,
        enabled = false,
        disabledAt = OffsetDateTime.now(),
      ),
    )

    val exception =
      assertThrows<ScimConfigurationConflictException> {
        service.enable(organizationId, ScimIdpProvider.OKTA, userId)
      }

    assertEquals("Disabled SCIM configurations cannot be re-enabled by this operation", exception.message)
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
    verify(exactly = 0) { scimConfigurationRepository.disableByIdAndOrganizationId(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `enable rejects a missing organization before reading configuration state`() {
    every { scimAccessGate.isAllowed(organizationId) } returns true
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns Optional.empty()

    assertThrows<ScimOrganizationNotFoundException> {
      service.enable(organizationId, ScimIdpProvider.OKTA, userId)
    }

    verify(exactly = 0) { scimConfigurationRepository.findByOrganizationIdForUpdate(any()) }
    verify(exactly = 0) { tokenService.generateToken() }
  }

  @Test
  fun `rotation denies access before locking or generating token material`() {
    every { scimAccessGate.isAllowed(organizationId) } returns false

    assertThrows<ScimAccessDeniedException> { service.rotateToken(organizationId, userId) }

    verify(exactly = 0) { organizationRepository.findByIdForUpdate(any()) }
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `rotation rejects an absent configuration without generating a token`() {
    every { scimAccessGate.isAllowed(organizationId) } returns true
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns null

    val exception = assertThrows<ScimConfigurationConflictException> { service.rotateToken(organizationId, userId) }

    assertEquals("SCIM must be enabled before its token can be rotated", exception.message)
    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `rotation rejects a disabled configuration without generating a token`() {
    stubLockedConfiguration(
      ScimConfiguration(
        organizationId = organizationId.value,
        idpProvider = ScimIdpProvider.OKTA.storageValue,
        enabled = false,
        disabledAt = OffsetDateTime.now(),
      ),
    )

    assertThrows<ScimConfigurationConflictException> { service.rotateToken(organizationId, userId) }

    verify(exactly = 0) { tokenService.generateToken() }
    verify(exactly = 0) { scimConfigurationRepository.rotateTokenByIdAndOrganizationId(any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `rotation replaces the hash and issuance metadata and returns the raw token once`() {
    val originalIssuedAt = OffsetDateTime.now().minusDays(1)
    val configuration = enabledConfiguration().apply { tokenIssuedAt = originalIssuedAt }
    val rawToken = "airbyte_scim_${"c".repeat(64)}"
    val replacementHash = "d".repeat(64)
    stubLockedConfiguration(configuration)
    every { tokenService.generateToken() } returns rawToken
    every { tokenService.hashToken(rawToken) } returns replacementHash
    every {
      scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        replacementHash,
        any(),
        userId.value,
        any(),
      )
    } returns 1

    val result = service.rotateToken(organizationId, userId)

    assertEquals(rawToken, result.token)
    assertEquals(ScimConfigurationStatus.ENABLED, result.status)
    assertEquals(ScimIdpProvider.OKTA, result.idpProvider)
    assertEquals(replacementHash, configuration.tokenHash)
    assertEquals(userId.value, configuration.tokenIssuedByUserId)
    assertTrue(configuration.tokenIssuedAt!!.isAfter(originalIssuedAt))
    verifyOrder {
      scimAccessGate.isAllowed(organizationId)
      organizationRepository.findByIdForUpdate(organizationId.value)
      scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
      tokenService.generateToken()
      tokenService.hashToken(rawToken)
      scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        replacementHash,
        any(),
        userId.value,
        any(),
      )
    }
  }

  @Test
  fun `rotation requires exactly one organization scoped row to be updated`() {
    val configuration = enabledConfiguration()
    stubLockedConfiguration(configuration)
    every { tokenService.generateToken() } returns "raw-token"
    every { tokenService.hashToken("raw-token") } returns "replacement-hash"
    every {
      scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        "replacement-hash",
        any(),
        userId.value,
        any(),
      )
    } returns 0

    assertThrows<IllegalStateException> { service.rotateToken(organizationId, userId) }
  }

  @Test
  fun `rotation propagates repository failures`() {
    val configuration = enabledConfiguration()
    val failure = IllegalStateException("database unavailable")
    stubLockedConfiguration(configuration)
    every { tokenService.generateToken() } returns "raw-token"
    every { tokenService.hashToken("raw-token") } returns "replacement-hash"
    every {
      scimConfigurationRepository.rotateTokenByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        "replacement-hash",
        any(),
        userId.value,
        any(),
      )
    } throws failure

    val thrown = assertThrows<IllegalStateException> { service.rotateToken(organizationId, userId) }

    assertSame(failure, thrown)
  }

  @Test
  fun `disable is a gate-free no-op when configuration is absent`() {
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns null

    service.disable(organizationId, userId)

    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
    verify(exactly = 0) { scimConfigurationRepository.disableByIdAndOrganizationId(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `disable is a gate-free no-op when configuration is already disabled`() {
    val configuration =
      ScimConfiguration(
        organizationId = organizationId.value,
        idpProvider = ScimIdpProvider.OKTA.storageValue,
        enabled = false,
        disabledAt = OffsetDateTime.now(),
      )
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns configuration

    service.disable(organizationId, userId)

    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
    verify(exactly = 0) { scimConfigurationRepository.disableByIdAndOrganizationId(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `disable clears token state and preserves provider and creation metadata`() {
    val createdBy = UUID.randomUUID()
    val createdAt = OffsetDateTime.now().minusDays(5)
    val configuration =
      enabledConfiguration(createdAt = createdAt).apply {
        createdByUserId = createdBy
        tokenIssuedByUserId = UUID.randomUUID()
      }
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns configuration
    every {
      scimConfigurationRepository.disableByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        any(),
        userId.value,
        any(),
      )
    } returns 1

    service.disable(organizationId, userId)

    assertFalse(configuration.enabled)
    assertNull(configuration.tokenHash)
    assertNull(configuration.tokenIssuedAt)
    assertNull(configuration.tokenIssuedByUserId)
    assertEquals(ScimIdpProvider.OKTA.storageValue, configuration.idpProvider)
    assertEquals(createdBy, configuration.createdByUserId)
    assertEquals(createdAt, configuration.createdAt)
    assertEquals(userId.value, configuration.disabledByUserId)
    assertTrue(configuration.disabledAt != null)
    verify(exactly = 0) { scimAccessGate.isAllowed(any()) }
    verifyOrder {
      organizationRepository.findByIdForUpdate(organizationId.value)
      scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value)
      scimConfigurationRepository.disableByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        any(),
        userId.value,
        any(),
      )
    }
  }

  @Test
  fun `disable requires exactly one organization scoped row to be updated`() {
    val configuration = enabledConfiguration()
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns configuration
    every {
      scimConfigurationRepository.disableByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        any(),
        userId.value,
        any(),
      )
    } returns 0

    assertThrows<IllegalStateException> { service.disable(organizationId, userId) }
  }

  @Test
  fun `disable propagates repository failures`() {
    val configuration = enabledConfiguration()
    val failure = IllegalStateException("database unavailable")
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns configuration
    every {
      scimConfigurationRepository.disableByIdAndOrganizationId(
        configuration.id!!,
        organizationId.value,
        any(),
        userId.value,
        any(),
      )
    } throws failure

    val thrown = assertThrows<IllegalStateException> { service.disable(organizationId, userId) }

    assertSame(failure, thrown)
  }

  private fun enabledConfiguration(
    idpProvider: String = ScimIdpProvider.OKTA.storageValue,
    createdAt: OffsetDateTime? = null,
    updatedAt: OffsetDateTime? = null,
  ): ScimConfiguration =
    ScimConfiguration(
      id = UUID.randomUUID(),
      organizationId = organizationId.value,
      tokenHash = "hash",
      idpProvider = idpProvider,
      enabled = true,
      createdAt = createdAt,
      updatedAt = updatedAt,
      tokenIssuedAt = OffsetDateTime.now(),
    )

  private fun stubLockedConfiguration(configuration: ScimConfiguration) {
    every { scimAccessGate.isAllowed(organizationId) } returns true
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(Organization(id = organizationId.value, name = "org", email = "org@example.com"))
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(organizationId.value) } returns configuration
  }
}

private class ImmediateConfigTransactionOperations : TransactionOperations<Connection> {
  override fun getConnection(): Connection = mockk(relaxed = true)

  override fun hasConnection(): Boolean = true

  override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

  override fun <R : Any?> execute(
    definition: TransactionDefinition,
    callback: TransactionCallback<Connection, R>,
  ): R = callback.call(mockk(relaxed = true))
}
