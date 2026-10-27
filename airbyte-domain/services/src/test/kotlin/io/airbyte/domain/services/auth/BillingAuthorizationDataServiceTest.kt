/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class BillingAuthorizationDataServiceTest {
  private val data = mockk<AuthorizationDataService>()
  private val payments = mockk<OrganizationPaymentConfigService>()
  private val service = BillingAuthorizationDataService(data, payments)
  private val organization = UUID(0, 2)

  @Test
  fun `payment metadata distinguishes missing config foreign rows and dependency errors`() {
    every { payments.findByOrganizationId(organization) } returns null
    assertFalse(service.hasPaymentConfiguration(organization))
    every { payments.findByOrganizationId(organization) } returns OrganizationPaymentConfig().withOrganizationId(UUID(0, 99))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.hasPaymentConfiguration(organization) }
    val failure = IOException("payment config unavailable")
    every { payments.findByOrganizationId(organization) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.hasPaymentConfiguration(organization) })
  }

  @Test
  fun `backfill uses exact saved historical connection and requires its organization and payment row`() {
    val connection = UUID(0, 3)
    val owner = WorkspaceAndOrganization(UUID(0, 1), organization)
    every { data.replicationJobConnectionId(41) } returns connection
    every { data.billingConnectionWorkspaceAndOrganization(connection) } returns owner
    every { data.organization(organization) } returns organization
    every { payments.findByOrganizationId(organization) } returns OrganizationPaymentConfig().withOrganizationId(organization)
    assertEquals(owner, service.backfillJobOwner(41))
    every { payments.findByOrganizationId(organization) } returns null
    assertThrows(IllegalStateException::class.java) { service.backfillJobOwner(41) }
    val failure = IOException("job unavailable")
    every { data.replicationJobConnectionId(41) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.backfillJobOwner(41) })
  }
}
