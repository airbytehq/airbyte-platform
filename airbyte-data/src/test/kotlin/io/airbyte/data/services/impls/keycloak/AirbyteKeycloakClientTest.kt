/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.domain.models.SsoConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import java.util.UUID

class AirbyteKeycloakClientTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private lateinit var keycloakAdminClient: Keycloak
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient

  @BeforeEach
  fun setup() {
    keycloakAdminClient = mockk<Keycloak>(relaxed = true)
    airbyteKeycloakClient = AirbyteKeycloakClient(keycloakAdminClient, airbyteUrl)
  }

  @Test
  fun `createOidcSsoConfig should create realm successfully`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
      )

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakAdminClient.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.create(any()) } returns mockk()

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockk()

    airbyteKeycloakClient.createOidcSsoConfig(config)

    verify(exactly = 4) { keycloakAdminClient.realms() }
    verify(exactly = 1) { realmsMock.create(any()) }
    verify(exactly = 2) { realmMock.identityProviders() }
    verify(exactly = 1) { idpMock.create(any()) }
    verify(exactly = 1) { realmMock.clients() }
    verify(exactly = 1) { clientsMock.create(any()) }
  }

  @Test
  fun `createOidcSsoConfig should throw RealmCreationException when Keycloak call fails`() {
    val config =
      SsoConfig(
        organizationId = UUID.randomUUID(),
        emailDomain = "testdomain",
        companyIdentifier = "airbyte",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://auth.airbyte.com/.well-known/openid-configuration",
      )

    every { keycloakAdminClient.realms().create(any()) } throws RuntimeException("Internal Server Error")

    val exception = assertThrows<RealmCreationException> { airbyteKeycloakClient.createOidcSsoConfig(config) }

    assertTrue(exception.message!!.contains("Create SSO config request failed"))
  }
}
