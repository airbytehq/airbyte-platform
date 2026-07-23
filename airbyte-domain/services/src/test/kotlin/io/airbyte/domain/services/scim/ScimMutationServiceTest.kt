/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.micronaut.transaction.TransactionCallback
import io.micronaut.transaction.TransactionDefinition
import io.micronaut.transaction.TransactionOperations
import io.micronaut.transaction.TransactionStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class ScimMutationServiceTest {
  private val organizationRepository = mockk<OrganizationRepository>()
  private val repository = mockk<ScimConfigurationRepository>()
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val configurationId = UUID.randomUUID()
  private val tokenHash = "authenticated-token-hash"
  private val context = ScimAuthenticationContext(configurationId, organizationId, tokenHash)
  private val service =
    ScimMutationService(
      organizationRepository,
      repository,
      ImmediateMutationTransactionOperations(),
    )

  @Test
  fun `locks organization then tenant scoped configuration before invoking mutation`() {
    stubOrganization()
    every {
      repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    } returns enabledConfiguration()

    val result = service.execute(context) { "mutation-result" }

    assertEquals("mutation-result", result)
    verifyOrder {
      organizationRepository.findByIdForUpdate(organizationId.value)
      repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    }
  }

  @Test
  fun `does not invoke mutation when organization is missing`() {
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns Optional.empty()

    assertMutationRejected()

    verify(exactly = 0) { repository.findByIdAndOrganizationIdForUpdate(any(), any()) }
  }

  @Test
  fun `does not invoke mutation when configuration is missing`() {
    stubOrganization()
    every {
      repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    } returns null

    assertMutationRejected()
  }

  @Test
  fun `does not invoke mutation when configuration is disabled`() {
    stubOrganization()
    every {
      repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    } returns enabledConfiguration().apply { enabled = false }

    assertMutationRejected()
  }

  @Test
  fun `does not invoke mutation when token hash was cleared or rotated`() {
    listOf(null, "replacement-token-hash").forEach { currentHash ->
      stubOrganization()
      every {
        repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
      } returns enabledConfiguration().apply { tokenHash = currentHash }

      assertMutationRejected()
    }
  }

  @Test
  fun `callback failures propagate unchanged`() {
    val failure = IllegalStateException("mutation failed")
    stubOrganization()
    every {
      repository.findByIdAndOrganizationIdForUpdate(configurationId, organizationId.value)
    } returns enabledConfiguration()

    val thrown = assertThrows<IllegalStateException> { service.execute(context) { throw failure } }

    assertSame(failure, thrown)
  }

  private fun assertMutationRejected() {
    var invoked = false

    assertThrows<ScimAuthenticationException> {
      service.execute(context) { invoked = true }
    }

    assertFalse(invoked)
  }

  private fun stubOrganization() {
    every { organizationRepository.findByIdForUpdate(organizationId.value) } returns
      Optional.of(
        Organization(
          id = organizationId.value,
          name = "organization",
          email = "organization@example.com",
        ),
      )
  }

  private fun enabledConfiguration(): ScimConfiguration =
    ScimConfiguration(
      id = configurationId,
      organizationId = organizationId.value,
      tokenHash = tokenHash,
      idpProvider = "okta",
      enabled = true,
      tokenIssuedAt = OffsetDateTime.now(),
    )
}

private class ImmediateMutationTransactionOperations : TransactionOperations<Connection> {
  override fun getConnection(): Connection = mockk(relaxed = true)

  override fun hasConnection(): Boolean = true

  override fun findTransactionStatus(): Optional<out TransactionStatus<*>> = Optional.empty()

  override fun <R : Any?> execute(
    definition: TransactionDefinition,
    callback: TransactionCallback<Connection, R>,
  ): R = callback.call(mockk(relaxed = true))
}
