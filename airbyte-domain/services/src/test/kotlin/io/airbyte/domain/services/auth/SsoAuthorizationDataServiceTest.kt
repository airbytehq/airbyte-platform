/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.SsoConfig
import io.airbyte.data.services.SsoConfigService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class SsoAuthorizationDataServiceTest {
  private val organization = UUID(0, 4)
  private val configurations = mockk<SsoConfigService>()
  private val service = SsoAuthorizationDataService(configurations)

  @Test
  fun `saved realm comes from matching organization metadata without provider credentials`() {
    every { configurations.getSsoConfig(organization) } returns SsoConfig().withOrganizationId(organization).withKeycloakRealm("fixture")
    assertEquals("fixture", service.savedRealm(organization).orElseThrow())
    every { configurations.getSsoConfig(organization) } returns null
    assertTrue(service.savedRealm(organization).isEmpty)
  }

  @Test
  fun `wrong organization missing realm and dependency errors cannot approve a realm`() {
    every { configurations.getSsoConfig(organization) } returns SsoConfig().withOrganizationId(UUID(0, 99)).withKeycloakRealm("fixture")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.savedRealm(organization) }
    every { configurations.getSsoConfig(organization) } returns SsoConfig().withOrganizationId(organization)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.savedRealm(organization) }
    every { configurations.getSsoConfig(organization) } returns SsoConfig().withOrganizationId(organization).withKeycloakRealm("")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.savedRealm(organization) }
    val failure = IOException("saved realm unavailable")
    every { configurations.getSsoConfig(organization) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.savedRealm(organization) })
  }
}
