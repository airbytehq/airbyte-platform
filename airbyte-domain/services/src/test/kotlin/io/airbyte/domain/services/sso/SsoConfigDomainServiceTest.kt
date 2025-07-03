/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.api.problems.throwable.generated.SSOSetupProblem
import io.airbyte.data.services.OrganizationEmailDomainService
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
import java.util.UUID

class SsoConfigDomainServiceTest {
  private lateinit var ssoConfigService: SsoConfigService
  private lateinit var organizationEmailDomainService: OrganizationEmailDomainService
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient
  private lateinit var ssoConfigDomainService: SsoConfigDomainService

  @BeforeEach
  fun setup() {
    ssoConfigService = mockk()
    organizationEmailDomainService = mockk()
    airbyteKeycloakClient = mockk()
    ssoConfigDomainService =
      SsoConfigDomainService(
        ssoConfigService,
        organizationEmailDomainService,
        airbyteKeycloakClient,
      )
  }

  @Test
  fun `createAndStoreSsoConfig should create SSO config and save email domain successfully`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        emailDomain = "airbyte.com",
      )

    every { airbyteKeycloakClient.createOidcSsoConfig(config) } just Runs
    every { ssoConfigService.createSsoConfig(any()) } returns mockk()
    every { organizationEmailDomainService.createEmailDomain(any()) } returns mockk()

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
  fun `createSsoConfig should not save anything if keycloak fails`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        emailDomain = "airbyte.com",
      )

    every { airbyteKeycloakClient.createOidcSsoConfig(config) } throws RuntimeException("Keycloak failed")

    assertThrows<SSOSetupProblem> {
      ssoConfigDomainService.createAndStoreSsoConfig(config)
    }

    verify(exactly = 1) { airbyteKeycloakClient.createOidcSsoConfig(config) }
    verify(exactly = 0) { ssoConfigService.createSsoConfig(any()) }
    verify(exactly = 0) { organizationEmailDomainService.createEmailDomain(any()) }
  }
}
