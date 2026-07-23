/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class ScimAuthenticationServiceTest {
  private val accessGate = mockk<ScimAccessGate>()
  private val repository = mockk<ScimConfigurationRepository>()
  private val tokenService = mockk<ScimTokenService>()
  private val service = ScimAuthenticationService(accessGate, repository, tokenService)
  private val rawToken = "airbyte_scim_${"a".repeat(64)}"
  private val tokenHash = "b".repeat(64)

  @Test
  fun `authenticates an enabled token for its resolved organization`() {
    val configuration = enabledConfiguration()
    every { tokenService.isValidToken(rawToken) } returns true
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { repository.findEnabledByTokenHash(tokenHash) } returns configuration
    every { accessGate.isAllowed(OrganizationId(configuration.organizationId)) } returns true

    val result = service.authenticate(rawToken)

    assertEquals(configuration.id, result.configurationId)
    assertEquals(configuration.organizationId, result.organizationId.value)
    assertTrue(result.matchesTokenHash(tokenHash))
  }

  @Test
  fun `malformed tokens fail without hashing or repository access`() {
    listOf("", "not-a-scim-token", "airbyte_scim_${"A".repeat(64)}").forEach { malformedToken ->
      every { tokenService.isValidToken(malformedToken) } returns false

      val exception = assertThrows<ScimAuthenticationException> { service.authenticate(malformedToken) }

      assertEquals("SCIM authentication failed", exception.message)
    }

    verify(exactly = 0) { tokenService.hashToken(any()) }
    verify(exactly = 0) { repository.findEnabledByTokenHash(any()) }
    verify(exactly = 0) { accessGate.isAllowed(any()) }
  }

  @Test
  fun `unknown rotated disabled or revoked token hash fails generically`() {
    every { tokenService.isValidToken(rawToken) } returns true
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { repository.findEnabledByTokenHash(tokenHash) } returns null

    val exception = assertThrows<ScimAuthenticationException> { service.authenticate(rawToken) }

    assertEquals("SCIM authentication failed", exception.message)
    verify(exactly = 0) { accessGate.isAllowed(any()) }
  }

  @Test
  fun `missing persisted configuration id fails generically`() {
    val configuration = enabledConfiguration(id = null)
    stubLookup(configuration)
    every { accessGate.isAllowed(OrganizationId(configuration.organizationId)) } returns true

    val exception = assertThrows<ScimAuthenticationException> { service.authenticate(rawToken) }

    assertEquals("SCIM authentication failed", exception.message)
  }

  @Test
  fun `gate denial preserves forbidden semantics`() {
    val configuration = enabledConfiguration()
    stubLookup(configuration)
    every { accessGate.isAllowed(OrganizationId(configuration.organizationId)) } returns false

    assertThrows<ScimAccessDeniedException> { service.authenticate(rawToken) }
  }

  @Test
  fun `distinct token hashes resolve distinct organization contexts`() {
    val otherToken = "airbyte_scim_${"c".repeat(64)}"
    val otherHash = "d".repeat(64)
    val first = enabledConfiguration()
    val second = enabledConfiguration()
    every { tokenService.isValidToken(rawToken) } returns true
    every { tokenService.isValidToken(otherToken) } returns true
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { tokenService.hashToken(otherToken) } returns otherHash
    every { repository.findEnabledByTokenHash(tokenHash) } returns first
    every { repository.findEnabledByTokenHash(otherHash) } returns second
    every { accessGate.isAllowed(OrganizationId(first.organizationId)) } returns true
    every { accessGate.isAllowed(OrganizationId(second.organizationId)) } returns true

    val firstContext = service.authenticate(rawToken)
    val secondContext = service.authenticate(otherToken)

    assertEquals(first.organizationId, firstContext.organizationId.value)
    assertEquals(second.organizationId, secondContext.organizationId.value)
    assertNotEquals(firstContext.organizationId, secondContext.organizationId)
  }

  @Test
  fun `repository and gate failures propagate unchanged`() {
    val repositoryFailure = IllegalStateException("database unavailable")
    every { tokenService.isValidToken(rawToken) } returns true
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { repository.findEnabledByTokenHash(tokenHash) } throws repositoryFailure

    assertSame(repositoryFailure, assertThrows<IllegalStateException> { service.authenticate(rawToken) })

    val configuration = enabledConfiguration()
    val gateFailure = IllegalStateException("gate unavailable")
    every { repository.findEnabledByTokenHash(tokenHash) } returns configuration
    every { accessGate.isAllowed(OrganizationId(configuration.organizationId)) } throws gateFailure

    assertSame(gateFailure, assertThrows<IllegalStateException> { service.authenticate(rawToken) })
  }

  private fun stubLookup(configuration: ScimConfiguration) {
    every { tokenService.isValidToken(rawToken) } returns true
    every { tokenService.hashToken(rawToken) } returns tokenHash
    every { repository.findEnabledByTokenHash(tokenHash) } returns configuration
  }

  private fun enabledConfiguration(id: UUID? = UUID.randomUUID()): ScimConfiguration =
    ScimConfiguration(
      id = id,
      organizationId = UUID.randomUUID(),
      tokenHash = tokenHash,
      idpProvider = "okta",
      enabled = true,
      tokenIssuedAt = OffsetDateTime.now(),
    )
}
