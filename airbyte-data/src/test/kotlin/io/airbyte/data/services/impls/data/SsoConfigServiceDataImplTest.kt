/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.domain.models.SsoConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class SsoConfigServiceDataImplTest {
  private lateinit var ssoConfigRepository: SsoConfigRepository
  private lateinit var ssoConfigService: SsoConfigServiceDataImpl

  @BeforeEach
  fun setup() {
    ssoConfigRepository = mockk()
    ssoConfigService =
      SsoConfigServiceDataImpl(
        ssoConfigRepository,
      )
  }

  @Test
  fun `createSsoConfig should create SSO config successfully`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
        emailDomain = "airbyte.com",
      )

    every { ssoConfigRepository.save(any()) } returns mockk()

    ssoConfigService.createSsoConfig(config)

    verify(exactly = 1) {
      ssoConfigRepository.save(
        withArg {
          assertEquals(config.organizationId, it.organizationId)
          assertEquals(config.companyIdentifier, it.keycloakRealm)
        },
      )
    }
  }

  @Test
  fun `deleteSsoConfig should remove SSO config successfully`() {
    val orgId = UUID.randomUUID()
    every { ssoConfigRepository.deleteByOrganizationId(orgId) } returns mockk()

    ssoConfigService.deleteSsoConfig(orgId)

    verify(exactly = 1) { ssoConfigRepository.deleteByOrganizationId(orgId) }
  }
}
