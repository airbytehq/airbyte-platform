/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoConfigStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import io.airbyte.data.repositories.entities.SsoConfig as SsoConfigEntity
import io.airbyte.db.instance.configs.jooq.generated.enums.SsoConfigStatus as JooqSsoConfigStatus

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
        status = SsoConfigStatus.ACTIVE,
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

  @Test
  fun `updateSsoConfigStatus should update status successfully`() {
    val orgId = UUID.randomUUID()
    val entity =
      SsoConfigEntity(
        id = UUID.randomUUID(),
        organizationId = orgId,
        keycloakRealm = "airbyte",
        status = JooqSsoConfigStatus.draft,
      )

    every { ssoConfigRepository.findByOrganizationId(orgId) } returns entity
    every { ssoConfigRepository.update(any()) } returns entity

    ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE)

    verify(exactly = 1) { ssoConfigRepository.findByOrganizationId(orgId) }
    verify(exactly = 1) {
      ssoConfigRepository.update(
        withArg {
          assertEquals(JooqSsoConfigStatus.active, it.status)
          assertEquals(orgId, it.organizationId)
        },
      )
    }
  }

  @Test
  fun `updateSsoConfigStatus throws when config not found`() {
    val orgId = UUID.randomUUID()

    every { ssoConfigRepository.findByOrganizationId(orgId) } returns null

    assertThrows<ConfigNotFoundException> {
      ssoConfigService.updateSsoConfigStatus(orgId, SsoConfigStatus.ACTIVE)
    }

    verify(exactly = 1) { ssoConfigRepository.findByOrganizationId(orgId) }
    verify(exactly = 0) { ssoConfigRepository.update(any()) }
  }

  @Test
  fun `getSsoConfigByCompanyIdentifier should return config when it exists`() {
    val orgId = UUID.randomUUID()
    val entity =
      SsoConfigEntity(
        id = UUID.randomUUID(),
        organizationId = orgId,
        keycloakRealm = "airbyte",
        status = JooqSsoConfigStatus.active,
      )

    every { ssoConfigRepository.findByKeycloakRealm("airbyte") } returns entity

    val result = ssoConfigService.getSsoConfigByCompanyIdentifier("airbyte")

    assertEquals("airbyte", result?.keycloakRealm)
    assertEquals(orgId, result?.organizationId)
    verify(exactly = 1) { ssoConfigRepository.findByKeycloakRealm("airbyte") }
  }

  @Test
  fun `getSsoConfigByCompanyIdentifier should return null when not found`() {
    every { ssoConfigRepository.findByKeycloakRealm("non-existent") } returns null

    val result = ssoConfigService.getSsoConfigByCompanyIdentifier("non-existent")

    assertEquals(null, result)
    verify(exactly = 1) { ssoConfigRepository.findByKeycloakRealm("non-existent") }
  }
}
