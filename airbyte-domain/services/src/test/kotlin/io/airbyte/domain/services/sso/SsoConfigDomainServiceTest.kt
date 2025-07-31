/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.config.Organization
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.domain.models.SsoConfig
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class SsoConfigDomainServiceTest {
  private lateinit var ssoConfigService: SsoConfigService
  private lateinit var organizationEmailDomainService: OrganizationEmailDomainService
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var organizationService: OrganizationService

  private lateinit var ssoConfigDomainService: SsoConfigDomainService

  @BeforeEach
  fun setup() {
    ssoConfigService = mockk()
    organizationEmailDomainService = mockk()
    airbyteKeycloakClient = mockk()
    organizationService = mockk()
    ssoConfigDomainService =
      SsoConfigDomainService(
        ssoConfigService,
        organizationEmailDomainService,
        airbyteKeycloakClient,
        organizationService,
      )
  }

  @Test
  fun `retrieveSsoConfig should return SsoConfig successfully`() {
    val organizationId = UUID.randomUUID()

    every { ssoConfigService.getSsoConfig(organizationId) } returns mockk(relaxed = true)
    every { airbyteKeycloakClient.getSsoConfigData(organizationId, any()) } returns mockk(relaxed = true)
    every { organizationEmailDomainService.findByOrganizationId(organizationId) } returns listOf(mockk(relaxed = true))

    ssoConfigDomainService.retrieveSsoConfig(organizationId)

    verify(exactly = 1) { ssoConfigService.getSsoConfig(organizationId) }
    verify(exactly = 1) { airbyteKeycloakClient.getSsoConfigData(organizationId, any()) }
    verify(exactly = 1) { organizationEmailDomainService.findByOrganizationId(organizationId) }
  }

  @Test
  fun `should throw SSOConfigRetrievalProblem if config is null`() {
    val organizationId = UUID.randomUUID()
    every { ssoConfigService.getSsoConfig(organizationId) } returns null

    assertThrows<ResourceNotFoundProblem> {
      ssoConfigDomainService.retrieveSsoConfig(organizationId)
    }
  }

  @Test
  fun `createAndStoreSsoConfig should create SSO config and save email domain successfully`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { airbyteKeycloakClient.createOidcSsoConfig(config) } just Runs
    every { ssoConfigService.createSsoConfig(any()) } returns mockk()
    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.createEmailDomain(any()) } returns mockk()
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    ssoConfigDomainService.createAndStoreSsoConfig(config)

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 1) {
      ssoConfigService.createSsoConfig(
        withArg {
          assertEquals(config, it)
        },
      )
    }
    verify(exactly = 1) {
      organizationEmailDomainService.createEmailDomain(
        withArg {
          assertEquals(config.organizationId, it.organizationId)
          assertEquals(config.emailDomain, it.emailDomain)
        },
      )
    }
  }

  @Test
  fun `createAndStoreSsoConfig should not save anything if keycloak fails`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { airbyteKeycloakClient.createOidcSsoConfig(config) } throws RuntimeException("Keycloak failed")

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 0) { ssoConfigService.createSsoConfig(any()) }
    verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
  }

  @Test
  fun `deleteSsoConfig should remove the config and domain email in the db and keycloak`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "test-company-identifier"

    every { airbyteKeycloakClient.deleteRealm(companyIdentifier) } just Runs
    every { ssoConfigService.deleteSsoConfig(orgId) } just Runs
    every { organizationEmailDomainService.deleteAllEmailDomains(orgId) } just Runs

    ssoConfigDomainService.deleteSsoConfig(orgId, companyIdentifier)

    verify(exactly = 1) { ssoConfigService.deleteSsoConfig(orgId) }
    verify(exactly = 1) { organizationEmailDomainService.deleteAllEmailDomains(orgId) }
    verify(exactly = 1) { airbyteKeycloakClient.deleteRealm(companyIdentifier) }
  }

  @Test
  fun `createAndStoreSsoConfig throws when email domain already exists`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns listOf(mockk())
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    val exception =
      assertThrows<SSOSetupProblem> {
        ssoConfigDomainService.createAndStoreSsoConfig(config)
      }
    assert(
      exception.problem
        .getData()
        .toString()
        .contains("Email domain already exists: ${config.emailDomain}"),
    )
  }

  @Test
  fun `createAndStoreSsoConfig throws when sso config already exists`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"
    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns mockk()
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    val exception =
      assertThrows<SSOSetupProblem> {
        ssoConfigDomainService.createAndStoreSsoConfig(config)
      }
    assert(
      exception.problem
        .getData()
        .toString()
        .contains("SSO Config already exists for organization ${config.organizationId}"),
    )
  }

  @Test
  fun `createAndStoreSsoConfig throws when the email domain is invalid`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.io" // org email does not match domain

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns mockk()
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    val exception =
      assertThrows<SSOSetupProblem> {
        ssoConfigDomainService.createAndStoreSsoConfig(config)
      }
    assert(
      exception.problem
        .getData()
        .toString()
        .contains("Domain must match the organization"),
    )
  }

  @Test
  fun `validateEmailDomain works even if the org email is incorrectly formatted`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "just-a-string.com"

    // get org with invalid email
    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    val exception =
      assertThrows<SSOSetupProblem> {
        ssoConfigDomainService.validateEmailDomain(config)
      }
    assert(
      exception.problem
        .getData()
        .toString()
        .contains("Domain must match the organization"),
    )
  }

  private fun buildTestOrganization(
    orgId: UUID,
    email: String,
  ): Organization =
    Organization()
      .withOrganizationId(orgId)
      .withEmail(email)

  private fun buildTestSsoConfig(
    orgId: UUID,
    emailDomain: String,
  ): SsoConfig =
    SsoConfig(
      organizationId = orgId,
      companyIdentifier = "airbyte",
      clientId = "client-id",
      clientSecret = "client-secret",
      discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
      emailDomain = emailDomain,
    )
}
