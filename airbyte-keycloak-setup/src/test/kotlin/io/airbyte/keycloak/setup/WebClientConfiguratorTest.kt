/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.ClientRepresentation

@ExtendWith(MockKExtension::class)
internal class WebClientConfiguratorTest {
  @MockK
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig

  @MockK
  private lateinit var realmResource: RealmResource

  @MockK
  private lateinit var clientsResource: ClientsResource

  @MockK
  private lateinit var response: Response

  private lateinit var webClientConfigurator: WebClientConfigurator

  @BeforeEach
  fun setUp() {
    every { keycloakConfiguration.webClientId } returns WEB_CLIENT_ID
    webClientConfigurator = WebClientConfigurator(AirbyteConfig(airbyteUrl = WEBAPP_URL), keycloakConfiguration)
  }

  @Test
  fun testCreateWebClient() {
    every { realmResource.clients() } returns clientsResource
    every { clientsResource.findByClientId(WEB_CLIENT_ID) } returns emptyList()
    every { clientsResource.create(any()) } returns response
    every { response.status } returns 201
    every { response.close() } returns Unit

    webClientConfigurator.configureWebClient(realmResource)

    verify(exactly = 1) { clientsResource.create(any<ClientRepresentation>()) }
  }

  @Test
  fun testCreateClientRepresentation() {
    every { keycloakConfiguration.webClientId } returns WEB_CLIENT_ID

    val clientRepresentation = webClientConfigurator.clientRepresentationFromConfig

    Assertions.assertEquals(WEB_CLIENT_ID, clientRepresentation.clientId)
    Assertions.assertTrue(clientRepresentation.isPublicClient)
    Assertions.assertTrue(clientRepresentation.isDirectAccessGrantsEnabled)
    Assertions.assertEquals("180", clientRepresentation.attributes["access.token.lifespan"])
  }

  companion object {
    private const val WEBAPP_URL = "http://localhost:8000"
    private const val WEB_CLIENT_ID = "airbyte-okta"
  }
}
