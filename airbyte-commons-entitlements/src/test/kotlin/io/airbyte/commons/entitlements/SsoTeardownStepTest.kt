/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.SsoConfig
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.RealmDeletionException
import io.airbyte.domain.models.OrganizationId
import io.micronaut.context.BeanProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class SsoTeardownStepTest {
  private val ssoConfigService = mockk<SsoConfigService>(relaxed = true)
  private val organizationEmailDomainService = mockk<OrganizationEmailDomainService>(relaxed = true)
  private val scimConfigurationRepository = mockk<ScimConfigurationRepository>(relaxed = true)
  private val keycloakClient = mockk<AirbyteKeycloakClient>(relaxed = true)
  private val keycloakClientProvider = mockk<BeanProvider<AirbyteKeycloakClient>>()
  private val step = SsoTeardownStep(ssoConfigService, organizationEmailDomainService, scimConfigurationRepository, keycloakClientProvider)

  private val orgId = OrganizationId(UUID.randomUUID())
  private val realm = "acme"
  private val ssoConfig = SsoConfig().withOrganizationId(orgId.value).withKeycloakRealm(realm)

  @BeforeEach
  fun setup() {
    every { keycloakClientProvider.get() } returns keycloakClient
    every { ssoConfigService.getSsoConfig(orgId.value) } returns ssoConfig
    every { keycloakClient.realmExists(realm) } returns true
    every { scimConfigurationRepository.findByOrganizationId(orgId.value) } returns null
  }

  @Test
  fun `does nothing when the organization has no SSO config`() {
    every { ssoConfigService.getSsoConfig(orgId.value) } returns null

    val result = step.run(orgId)

    assertEquals(PlanLimitStepResult.NotNeeded, result)
    verify(exactly = 0) { keycloakClientProvider.get() }
    verify(exactly = 0) { ssoConfigService.deleteSsoConfig(any()) }
    verify(exactly = 0) { organizationEmailDomainService.deleteAllEmailDomains(any()) }
  }

  @Test
  fun `deletes the realm before the database rows`() {
    val scimConfiguration = ScimConfiguration(organizationId = orgId.value)
    every { scimConfigurationRepository.findByOrganizationId(orgId.value) } returns scimConfiguration

    val result = step.run(orgId)

    assertEquals(PlanLimitStepResult.Completed("removed SSO realm=$realm"), result)
    verifyOrder {
      keycloakClient.realmExists(realm)
      keycloakClient.deleteRealm(realm)
      ssoConfigService.deleteSsoConfig(orgId.value)
      organizationEmailDomainService.deleteAllEmailDomains(orgId.value)
      scimConfigurationRepository.delete(scimConfiguration)
    }
  }

  @Test
  fun `finishes the database cleanup when the realm is already gone`() {
    every { keycloakClient.realmExists(realm) } returns false

    val result = step.run(orgId)

    assertEquals(PlanLimitStepResult.Completed("removed SSO realm=$realm"), result)
    verify(exactly = 0) { keycloakClient.deleteRealm(any()) }
    verify(exactly = 1) { ssoConfigService.deleteSsoConfig(orgId.value) }
    verify(exactly = 1) { organizationEmailDomainService.deleteAllEmailDomains(orgId.value) }
    verify(exactly = 0) { scimConfigurationRepository.delete(any()) }
  }

  @Test
  fun `leaves the database untouched when Keycloak fails`() {
    every { keycloakClient.deleteRealm(realm) } throws RealmDeletionException("boom")

    assertThrows(RealmDeletionException::class.java) { step.run(orgId) }

    verify(exactly = 0) { ssoConfigService.deleteSsoConfig(any()) }
    verify(exactly = 0) { organizationEmailDomainService.deleteAllEmailDomains(any()) }
    verify(exactly = 0) { scimConfigurationRepository.delete(any()) }
  }
}
