/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.IdentityProviderResource
import org.keycloak.admin.client.resource.IdentityProvidersResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.RealmsResource
import org.keycloak.representations.idm.IdentityProviderRepresentation
import java.util.UUID

class AirbyteKeycloakClientTest {
  private val airbyteUrl: String = "https://cloud.airbyte.com"
  private lateinit var airbyteKeycloakAdminClientProvider: AirbyteKeycloakAdminClientProvider
  private lateinit var airbyteKeycloakClient: AirbyteKeycloakClient

  private var keycloakClientMock = mockk<Keycloak>(relaxed = true)

  @BeforeEach
  fun setup() {
    airbyteKeycloakAdminClientProvider = mockk<AirbyteKeycloakAdminClientProvider>(relaxed = true)
    every { airbyteKeycloakAdminClientProvider.createKeycloakAdminClient() } returns keycloakClientMock
    airbyteKeycloakClient = AirbyteKeycloakClient(airbyteKeycloakAdminClientProvider, airbyteUrl)
  }

  @AfterEach
  fun tearDown() {
    clearMocks(keycloakClientMock)
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

    val mockResponse = mockk<Response>(relaxed = true)
    every { mockResponse.statusInfo } returns Response.Status.OK

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val clientsMock = mockk<ClientsResource>(relaxed = true)
    every { realmMock.clients() } returns clientsMock
    every { clientsMock.create(any()) } returns mockResponse

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { realmMock.identityProviders() } returns idpMock
    every { idpMock.create(any()) } returns mockResponse

    airbyteKeycloakClient.createOidcSsoConfig(config)

    verify(exactly = 4) { keycloakClientMock.realms() }
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

    every { keycloakClientMock.realms().create(any()) } throws RuntimeException("Internal Server Error")

    val exception = assertThrows<RealmCreationException> { airbyteKeycloakClient.createOidcSsoConfig(config) }

    assertTrue(exception.message!!.contains("Create SSO config request failed"))
  }

  @Test
  fun `updateIdpClientCredentials successfully changes the idp`() {
    val config =
      SsoKeycloakIdpCredentials(
        organizationId = UUID.randomUUID(),
        clientId = "client-id",
        clientSecret = "client-secret",
      )

    val realmsMock = mockk<RealmsResource>(relaxed = true)
    every { keycloakClientMock.realms() } returns realmsMock

    val realmMock = mockk<RealmResource>(relaxed = true)
    every { realmsMock.realm(any()) } returns realmMock

    val idpRepresentationMock = mockk<IdentityProviderRepresentation>(relaxed = true)
    every { idpRepresentationMock.alias } returns "default"

    val idpMock = mockk<IdentityProvidersResource>(relaxed = true)
    every { idpMock.findAll() } returns listOf(idpRepresentationMock)
    every { realmMock.identityProviders() } returns idpMock

    // note that this is a IdentityProviderResource, not an IdentityProvidersResource
    // as above (singular Provider, not plural Providers)!
    val idpProviderMock = mockk<IdentityProviderResource>(relaxed = true)
    every { idpMock.get(any()) } returns idpProviderMock
    every { idpProviderMock.update(any()) } returns Unit

    airbyteKeycloakClient.updateIdpClientCredentials(config, "testrealm")
    verify(exactly = 1) { keycloakClientMock.realms() }
    verify(exactly = 2) { realmMock.identityProviders() }
    verify(exactly = 1) { idpMock.findAll() }
    verify(exactly = 1) { idpMock.get(any()) }
    verify(exactly = 1) { idpProviderMock.update(any()) }
  }
}
