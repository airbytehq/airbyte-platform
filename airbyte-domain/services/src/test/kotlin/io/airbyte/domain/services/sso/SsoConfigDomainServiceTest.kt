/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.SSOActivationProblem
import io.airbyte.api.problems.throwable.generated.SSODeletionProblem
import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.config.Organization
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.data.services.OrganizationEmailDomainService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SsoConfigService
import io.airbyte.data.services.impls.keycloak.AirbyteKeycloakClient
import io.airbyte.data.services.impls.keycloak.IdpNotFoundException
import io.airbyte.data.services.impls.keycloak.InvalidOidcDiscoveryDocumentException
import io.airbyte.data.services.impls.keycloak.RealmDeletionException
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseVerifiedDomainsForSsoActivate
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.config.SsoConfig as ConfigSsoConfig
import io.airbyte.config.SsoConfigStatus as ConfigSsoConfigStatus

class SsoConfigDomainServiceTest {
  private lateinit var ssoConfigService: SsoConfigService
  private lateinit var organizationEmailDomainService: OrganizationEmailDomainService
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var organizationService: OrganizationService
  private lateinit var userPersistence: UserPersistence
  private lateinit var organizationDomainVerificationService: OrganizationDomainVerificationService
  private lateinit var featureFlagClient: FeatureFlagClient

  private lateinit var ssoConfigDomainService: SsoConfigDomainService

  @BeforeEach
  fun setup() {
    ssoConfigService = mockk()
    organizationEmailDomainService = mockk()
    airbyteKeycloakClient = mockk()
    organizationService = mockk()
    userPersistence = mockk()
    organizationDomainVerificationService = mockk()
    featureFlagClient = mockk()
    ssoConfigDomainService =
      SsoConfigDomainService(
        ssoConfigService,
        organizationEmailDomainService,
        airbyteKeycloakClient,
        organizationService,
        userPersistence,
        organizationDomainVerificationService,
        featureFlagClient,
      )
  }

  @Test
  fun `retrieveSsoConfig should return SsoConfig successfully`() {
    val organizationId = UUID.randomUUID()

    every { ssoConfigService.getSsoConfig(organizationId) } returns mockk(relaxed = true)
    every { airbyteKeycloakClient.realmExists(any()) } returns true
    every { airbyteKeycloakClient.getSsoConfigData(organizationId, any()) } returns mockk(relaxed = true)
    every { organizationEmailDomainService.findByOrganizationId(organizationId) } returns listOf(mockk(relaxed = true))

    ssoConfigDomainService.retrieveSsoConfig(organizationId)

    verify(exactly = 1) { ssoConfigService.getSsoConfig(organizationId) }
    verify(exactly = 1) { airbyteKeycloakClient.realmExists(any()) }
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
  fun `retrieveSsoConfig should return empty credentials when IDP not found`() {
    val organizationId = UUID.randomUUID()
    val realmName = "test-realm"
    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(organizationId)
        .withKeycloakRealm(realmName)
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    every { ssoConfigService.getSsoConfig(organizationId) } returns existingConfig
    every { airbyteKeycloakClient.realmExists(realmName) } returns true
    every { airbyteKeycloakClient.getSsoConfigData(organizationId, realmName) } throws IdpNotFoundException("IDP not found")
    every { organizationEmailDomainService.findByOrganizationId(organizationId) } returns emptyList()

    val result = ssoConfigDomainService.retrieveSsoConfig(organizationId)

    assertEquals("", result.clientId)
    assertEquals("", result.clientSecret)
    assertEquals(realmName, result.companyIdentifier)
    assertEquals(SsoConfigStatus.DRAFT, result.status)
    verify(exactly = 1) { airbyteKeycloakClient.getSsoConfigData(organizationId, realmName) }
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
    every { ssoConfigService.getSsoConfigByCompanyIdentifier(any()) } returns null
    every { organizationEmailDomainService.createEmailDomain(any()) } returns mockk()
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()

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
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()
    every { airbyteKeycloakClient.createOidcSsoConfig(config) } throws RuntimeException("Keycloak failed")

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 0) { ssoConfigService.createSsoConfig(any()) }
    verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
  }

  @Test
  fun `createAndStoreSsoConfig should cleanup keycloak realm if database fails`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { ssoConfigService.getSsoConfigByCompanyIdentifier(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()
    every { airbyteKeycloakClient.createOidcSsoConfig(config) } just Runs
    every { ssoConfigService.createSsoConfig(any()) } throws RuntimeException("Database failed")
    every { airbyteKeycloakClient.deleteRealm(config.companyIdentifier) } just Runs

    assertThrows<RuntimeException> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 1) { ssoConfigService.createSsoConfig(config) }
    verify(exactly = 1) { airbyteKeycloakClient.deleteRealm(config.companyIdentifier) }
    verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
  }

  @Test
  fun `deleteSsoConfig should remove the config and domain email in the db and keycloak`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "test-company-identifier"

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(companyIdentifier)
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { airbyteKeycloakClient.deleteRealm(companyIdentifier) } just Runs
    every { ssoConfigService.deleteSsoConfig(orgId) } just Runs
    every { organizationEmailDomainService.deleteAllEmailDomains(orgId) } just Runs

    ssoConfigDomainService.deleteSsoConfig(orgId, companyIdentifier)

    verify(exactly = 1) { ssoConfigService.deleteSsoConfig(orgId) }
    verify(exactly = 1) { organizationEmailDomainService.deleteAllEmailDomains(orgId) }
    verify(exactly = 1) { airbyteKeycloakClient.deleteRealm(companyIdentifier) }
  }

  @Test
  fun `deleteSsoConfig should still remove the config and domain email in the db even if keycloak realm deletion fails`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "test-company-identifier"

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(companyIdentifier)
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { airbyteKeycloakClient.deleteRealm(companyIdentifier) } throws RealmDeletionException("Realm deletion failed")
    every { ssoConfigService.deleteSsoConfig(orgId) } just Runs
    every { organizationEmailDomainService.deleteAllEmailDomains(orgId) } just Runs

    assertDoesNotThrow { ssoConfigDomainService.deleteSsoConfig(orgId, companyIdentifier) }

    verify(exactly = 1) { ssoConfigService.deleteSsoConfig(orgId) }
    verify(exactly = 1) { organizationEmailDomainService.deleteAllEmailDomains(orgId) }
    verify(exactly = 1) { airbyteKeycloakClient.deleteRealm(companyIdentifier) }
  }

  @Test
  fun `deleteSsoConfig should throw when company identifier does not match`() {
    val orgId = UUID.randomUUID()
    val providedCompanyIdentifier = "wrong-company-identifier"
    val actualCompanyIdentifier = "correct-company-identifier"

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(actualCompanyIdentifier)
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig

    val exception =
      assertThrows<SSODeletionProblem> {
        ssoConfigDomainService.deleteSsoConfig(orgId, providedCompanyIdentifier)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("Company identifier mismatch"),
    )
  }

  @Test
  fun `deleteSsoConfig should throw when no config exists`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "test-company-identifier"

    every { ssoConfigService.getSsoConfig(orgId) } returns null

    val exception =
      assertThrows<SSODeletionProblem> {
        ssoConfigDomainService.deleteSsoConfig(orgId, companyIdentifier)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("No SSO config found for organization"),
    )
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

    assertThrows<SSOActivationProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `createAndStoreSsoConfig throws when active sso config already exists`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"
    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    val existingActiveConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("existing-realm")
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(any()) } returns existingActiveConfig
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `createAndStoreSsoConfig should throw when trying to create active config with existing draft`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"
    val org = buildTestOrganization(orgId, orgEmail)
    val newConfig = buildTestSsoConfig(orgId, emailDomain)

    val existingDraftConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("old-draft-realm")
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingDraftConfig
    every { organizationEmailDomainService.findByEmailDomain(emailDomain) } returns emptyList()
    every { organizationService.getOrganization(orgId) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(newConfig)
    }
  }

  @Test
  fun `createAndStoreSsoConfig should throw when trying to create active config with existing active`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"
    val org = buildTestOrganization(orgId, orgEmail)
    val newConfig = buildTestSsoConfig(orgId, emailDomain)

    val existingActiveConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("existing-realm")
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingActiveConfig
    every { organizationEmailDomainService.findByEmailDomain(emailDomain) } returns emptyList()
    every { organizationService.getOrganization(orgId) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(newConfig)
    }
  }

  @Test
  fun `createAndStoreSsoConfig throws when the email domain is invalid`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.io" // org email does not match domain

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    assertThrows<SSOActivationProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `createAndStoreSsoConfig throws when the discoveryUrl is invalid`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"
    val invalidDiscoveryUrl = "not-a-url-at-all"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain, invalidDiscoveryUrl)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `createAndStoreSsoConfig throws when OIDC discovery document is missing required fields`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { ssoConfigService.getSsoConfigByCompanyIdentifier(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(any(), any()) } returns emptyList()
    every { airbyteKeycloakClient.createOidcSsoConfig(config) } throws
      InvalidOidcDiscoveryDocumentException("Missing required fields", listOf("authorizationUrl", "tokenUrl"))

    val exception =
      assertThrows<SSOSetupProblem> {
        ssoConfigDomainService.createAndStoreSsoConfig(config)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("discovery URL did not return valid SSO configuration"),
    )
  }

  @Test
  fun `validateEmailDomain works even if the org email is incorrectly formatted`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "just-a-string.com"
    val companyIdentifier = "test-company"

    // get org with invalid email
    val org = buildTestOrganization(orgId, orgEmail)

    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    assertThrows<SSOActivationProblem> {
      ssoConfigDomainService.validateEmailDomainMatchesOrganization(orgId, emailDomain, companyIdentifier)
    }
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
    emailDomain: String? = "airbyte.com",
    discoveryUrl: String = "https://auth.airbyte.com/.well-known/openid-configuration",
    status: SsoConfigStatus = SsoConfigStatus.ACTIVE,
  ): SsoConfig =
    SsoConfig(
      organizationId = orgId,
      companyIdentifier = "airbyte",
      clientId = "client-id",
      clientSecret = "client-secret",
      discoveryUrl = discoveryUrl,
      emailDomain = emailDomain,
      status = status,
    )

  @Test
  fun `createAndStoreSsoConfig should create draft SSO config without email domain`() {
    val orgId = UUID.randomUUID()
    val config = buildTestSsoConfig(orgId, emailDomain = null, status = SsoConfigStatus.DRAFT)

    every { airbyteKeycloakClient.createOidcSsoConfig(config) } just Runs
    every { ssoConfigService.createSsoConfig(any()) } returns mockk()
    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { ssoConfigService.getSsoConfigByCompanyIdentifier(any()) } returns null

    ssoConfigDomainService.createAndStoreSsoConfig(config)

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 1) { ssoConfigService.createSsoConfig(config) }
    verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
  }

  @Test
  fun `createAndStoreSsoConfig throws when creating draft with email domain`() {
    val orgId = UUID.randomUUID()
    val config = buildTestSsoConfig(orgId, emailDomain = "airbyte.com", status = SsoConfigStatus.DRAFT)

    every { ssoConfigService.getSsoConfig(any()) } returns null

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `createAndStoreSsoConfig throws when creating active without email domain`() {
    val orgId = UUID.randomUUID()
    val config = buildTestSsoConfig(orgId, emailDomain = null, status = SsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(any()) } returns null

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }
  }

  @Test
  fun `activateSsoConfig should activate draft and create email domain successfully for all verified domains -- FeatureFlag on`() {
    val orgId = UUID.randomUUID()
    val domain1 = "test1.com"
    val domain2 = "test1.io"

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("test1")
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    val verifiedDomain1 =
      OrganizationDomainVerification(
        id = UUID.randomUUID(),
        organizationId = orgId,
        domain = domain1,
        verificationMethod = DomainVerificationMethod.DNS_TXT,
        status = DomainVerificationStatus.VERIFIED,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.airbyte.com",
        dnsRecordPrefix = "_airbyte-verification",
        attempts = 1,
        lastCheckedAt = OffsetDateTime.now(),
        expiresAt = OffsetDateTime.now().plusDays(14),
        createdBy = null,
        verifiedAt = OffsetDateTime.now(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    val verifiedDomain2 =
      OrganizationDomainVerification(
        id = UUID.randomUUID(),
        organizationId = orgId,
        domain = domain2,
        verificationMethod = DomainVerificationMethod.DNS_TXT,
        status = DomainVerificationStatus.VERIFIED,
        verificationToken = "token2",
        dnsRecordName = "_airbyte-verification.airbyte.io",
        dnsRecordPrefix = "_airbyte-verification",
        attempts = 1,
        lastCheckedAt = OffsetDateTime.now(),
        expiresAt = OffsetDateTime.now().plusDays(14),
        createdBy = null,
        verifiedAt = OffsetDateTime.now(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    every { featureFlagClient.boolVariation(UseVerifiedDomainsForSsoActivate, any()) } returns true
    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { organizationDomainVerificationService.findByOrganizationId(orgId) } returns listOf(verifiedDomain1, verifiedDomain2)

    every { ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE) } just Runs
    every { organizationEmailDomainService.createEmailDomain(any()) } returns mockk()

    ssoConfigDomainService.activateSsoConfig(orgId)

    verify(exactly = 1) { ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE) }
    verify(exactly = 2) { organizationEmailDomainService.createEmailDomain(any()) }
    verify {
      organizationEmailDomainService.createEmailDomain(
        withArg {
          assertEquals(orgId, it.organizationId)
          assertEquals(domain1, it.emailDomain)
        },
      )
    }
    verify {
      organizationEmailDomainService.createEmailDomain(
        withArg {
          assertEquals(orgId, it.organizationId)
          assertEquals(domain2, it.emailDomain)
        },
      )
    }
  }

  @Test
  fun `activateSsoConfig throws when SSO config does not exist`() {
    val orgId = UUID.randomUUID()

    every { ssoConfigService.getSsoConfig(orgId) } returns null

    assertThrows<SSOActivationProblem> {
      ssoConfigDomainService.activateSsoConfig(orgId)
    }
  }

  @Test
  fun `activateSsoConfig throws when SSO config is already active`() {
    val orgId = UUID.randomUUID()

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("airbyte")
        .withStatus(ConfigSsoConfigStatus.ACTIVE)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig

    assertThrows<SSOActivationProblem> {
      ssoConfigDomainService.activateSsoConfig(orgId)
    }
  }

  @Test
  fun `activateSsoConfig throws when no verified domains exist -- FeatureFlag on`() {
    val orgId = UUID.randomUUID()

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("airbyte")
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    every { featureFlagClient.boolVariation(UseVerifiedDomainsForSsoActivate, any()) } returns true
    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { organizationDomainVerificationService.findByOrganizationId(orgId) } returns emptyList()

    val exception =
      assertThrows<SSOActivationProblem> {
        ssoConfigDomainService.activateSsoConfig(orgId)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("No verified domains found"),
    )
  }

  @Test
  fun `activateSsoConfig throws when only pending (non-verified) domains exist -- FeatureFlag on`() {
    val orgId = UUID.randomUUID()

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("airbyte")
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    val pendingDomain =
      OrganizationDomainVerification(
        id = UUID.randomUUID(),
        organizationId = orgId,
        domain = "airbyte.com",
        verificationMethod = DomainVerificationMethod.DNS_TXT,
        status = DomainVerificationStatus.PENDING,
        verificationToken = "token1",
        dnsRecordName = "_airbyte-verification.airbyte.com",
        dnsRecordPrefix = "_airbyte-verification",
        attempts = 0,
        lastCheckedAt = null,
        expiresAt = OffsetDateTime.now().plusDays(14),
        createdBy = null,
        verifiedAt = null,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    every { featureFlagClient.boolVariation(UseVerifiedDomainsForSsoActivate, any()) } returns true
    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { organizationDomainVerificationService.findByOrganizationId(orgId) } returns listOf(pendingDomain)

    val exception =
      assertThrows<SSOActivationProblem> {
        ssoConfigDomainService.activateSsoConfig(orgId)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("No verified domains found"),
    )
  }

  @Test
  fun `createAndStoreSsoConfig should update IDP config when draft exists with same company identifier and realm exists`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "airbyte"
    val newConfig = buildTestSsoConfig(orgId, emailDomain = null, status = SsoConfigStatus.DRAFT)

    val existingDraftConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(companyIdentifier)
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingDraftConfig
    every { airbyteKeycloakClient.realmExists(companyIdentifier) } returns true
    every { airbyteKeycloakClient.replaceOidcIdpConfig(newConfig) } just Runs

    ssoConfigDomainService.createAndStoreSsoConfig(newConfig)

    verify(exactly = 1) { airbyteKeycloakClient.realmExists(companyIdentifier) }
    verify(exactly = 1) { airbyteKeycloakClient.replaceOidcIdpConfig(newConfig) }
    verify(exactly = 0) { ssoConfigService.createSsoConfig(any()) }
    verify(exactly = 0) { airbyteKeycloakClient.createOidcSsoConfig(any()) }
    verify(exactly = 0) { airbyteKeycloakClient.deleteRealm(any()) }
  }

  @Test
  fun `createAndStoreSsoConfig should recreate realm when draft exists with same company identifier but realm does not exist`() {
    val orgId = UUID.randomUUID()
    val companyIdentifier = "airbyte"
    val newConfig = buildTestSsoConfig(orgId, emailDomain = null, status = SsoConfigStatus.DRAFT)

    val existingDraftConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(companyIdentifier)
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    every { ssoConfigService.getSsoConfig(orgId) } returns existingDraftConfig
    every { airbyteKeycloakClient.realmExists(companyIdentifier) } returns false
    every { airbyteKeycloakClient.createOidcSsoConfig(newConfig) } just Runs

    ssoConfigDomainService.createAndStoreSsoConfig(newConfig)

    verify(exactly = 1) { airbyteKeycloakClient.realmExists(companyIdentifier) }
    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(newConfig) }
    verify(exactly = 0) { ssoConfigService.createSsoConfig(any()) }
    verify(exactly = 0) { airbyteKeycloakClient.replaceOidcIdpConfig(any()) }
    verify(exactly = 0) { airbyteKeycloakClient.deleteRealm(any()) }
  }

  @Test
  fun `createAndStoreSsoConfig should delete old realm and create new when draft exists with different company identifier`() {
    val orgId = UUID.randomUUID()
    val oldCompanyIdentifier = "old-company"
    val newCompanyIdentifier = "new-company"
    val newConfig =
      buildTestSsoConfig(orgId, emailDomain = null, status = SsoConfigStatus.DRAFT).copy(
        companyIdentifier = newCompanyIdentifier,
      )

    val existingDraftConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm(oldCompanyIdentifier)
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    // First call: check for existing config in createDraftSsoConfig
    // Second call: check for existing config in deleteSsoConfig (still returns existing)
    // Third call: after deletion, check for existing config in createNewDraftSsoConfig (returns null)
    every { ssoConfigService.getSsoConfig(orgId) } returns existingDraftConfig andThen existingDraftConfig andThen null
    every { ssoConfigService.getSsoConfigByCompanyIdentifier(any()) } returns null
    every { ssoConfigService.deleteSsoConfig(orgId) } just Runs
    every { organizationEmailDomainService.deleteAllEmailDomains(orgId) } just Runs
    every { airbyteKeycloakClient.deleteRealm(oldCompanyIdentifier) } just Runs
    every { airbyteKeycloakClient.createOidcSsoConfig(newConfig) } just Runs
    every { ssoConfigService.createSsoConfig(newConfig) } returns mockk()

    ssoConfigDomainService.createAndStoreSsoConfig(newConfig)

    verify(exactly = 1) { ssoConfigService.deleteSsoConfig(orgId) }
    verify(exactly = 1) { organizationEmailDomainService.deleteAllEmailDomains(orgId) }
    verify(exactly = 1) { airbyteKeycloakClient.deleteRealm(oldCompanyIdentifier) }
    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(newConfig) }
    verify(exactly = 1) { ssoConfigService.createSsoConfig(newConfig) }
    verify(exactly = 0) { airbyteKeycloakClient.replaceOidcIdpConfig(any()) }
  }

  @Test
  fun `activateSsoConfig with flag OFF - requires emailDomain parameter`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val existingConfig =
      ConfigSsoConfig()
        .withOrganizationId(orgId)
        .withKeycloakRealm("test-realm")
        .withStatus(ConfigSsoConfigStatus.DRAFT)

    val org = buildTestOrganization(orgId, orgEmail)

    // Feature flag OFF - use old behavior
    every { featureFlagClient.boolVariation(UseVerifiedDomainsForSsoActivate, any()) } returns false
    every { ssoConfigService.getSsoConfig(orgId) } returns existingConfig
    every { organizationService.getOrganization(orgId) } returns Optional.of(org)
    every { organizationEmailDomainService.findByEmailDomain(emailDomain) } returns emptyList()
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(emailDomain, orgId) } returns emptyList()
    every { ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE) } just Runs
    every { organizationEmailDomainService.createEmailDomain(any()) } returns mockk()

    ssoConfigDomainService.activateSsoConfig(orgId, emailDomain)

    verify(exactly = 1) { organizationService.getOrganization(orgId) }
    verify(exactly = 1) { organizationEmailDomainService.findByEmailDomain(emailDomain) }
    verify(exactly = 1) { userPersistence.findUsersWithEmailDomainOutsideOrganization(emailDomain, orgId) }
    verify(exactly = 1) { ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE) }
    verify(exactly = 1) {
      organizationEmailDomainService.createEmailDomain(
        withArg {
          assertEquals(orgId, it.organizationId)
          assertEquals(emailDomain, it.emailDomain)
        },
      )
    }
    verify(exactly = 0) { organizationDomainVerificationService.findByOrganizationId(any()) }
  }

  @Test
  fun `createAndStoreSsoConfig with ACTIVE status throws when users from other organizations already use the domain`() {
    val orgId = UUID.randomUUID()
    val emailDomain = "airbyte.com"
    val orgEmail = "test@airbyte.com"

    val org = buildTestOrganization(orgId, orgEmail)
    val config = buildTestSsoConfig(orgId, emailDomain)

    val otherUserId = UUID.randomUUID()

    every { ssoConfigService.getSsoConfig(any()) } returns null
    every { organizationEmailDomainService.findByEmailDomain(any()) } returns emptyList()
    every { organizationService.getOrganization(any()) } returns Optional.of(org)
    every { userPersistence.findUsersWithEmailDomainOutsideOrganization(emailDomain, orgId) } returns listOf(otherUserId)

    val exception =
      assertThrows<SSOActivationProblem> {
        ssoConfigDomainService.createAndStoreSsoConfig(config)
      }

    assert(
      exception.problem
        .getData()
        .toString()
        .contains("1 user(s) from other organizations"),
    )
  }
}
