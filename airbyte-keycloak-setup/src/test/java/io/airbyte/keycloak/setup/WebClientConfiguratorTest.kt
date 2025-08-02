/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.keycloak.admin.client.resource.ClientsResource
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.representations.idm.ClientRepresentation
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension

@ExtendWith(MockitoExtension::class)
internal class WebClientConfiguratorTest {
  @Mock
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration

  @Mock
  private lateinit var realmResource: RealmResource

  @Mock
  private lateinit var clientsResource: ClientsResource

  @Mock
  private lateinit var response: Response

  private lateinit var webClientConfigurator: WebClientConfigurator

  @BeforeEach
  fun setUp() {
    Mockito.`when`(keycloakConfiguration.webClientId).thenReturn(WEB_CLIENT_ID)
    webClientConfigurator = WebClientConfigurator(WEBAPP_URL, keycloakConfiguration)
  }

  @Test
  fun testCreateWebClient() {
    Mockito.`when`(realmResource.clients()).thenReturn(clientsResource)
    Mockito
      .`when`(clientsResource.create(ArgumentMatchers.any(ClientRepresentation::class.java)))
      .thenReturn(response)
    Mockito.`when`(response.status).thenReturn(201)

    webClientConfigurator.configureWebClient(realmResource)

    Mockito.verify(clientsResource).create(ArgumentMatchers.any(ClientRepresentation::class.java))
  }

  @Test
  fun testCreateClientRepresentation() {
    Mockito.`when`(keycloakConfiguration.webClientId).thenReturn(WEB_CLIENT_ID)

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
