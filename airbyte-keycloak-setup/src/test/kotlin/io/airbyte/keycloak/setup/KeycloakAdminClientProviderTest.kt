/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension

@ExtendWith(MockitoExtension::class)
internal class KeycloakAdminClientProviderTest {
  @Mock
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration

  @InjectMocks
  private lateinit var keycloakAdminClientProvider: KeycloakAdminClientProvider

  @BeforeEach
  fun setUp() {
    Mockito.`when`(keycloakConfiguration.realm).thenReturn(REALM)
    Mockito.`when`(keycloakConfiguration.clientId).thenReturn(CLIENT_ID)
    Mockito.`when`(keycloakConfiguration.username).thenReturn(USERNAME)
    Mockito.`when`(keycloakConfiguration.password).thenReturn(PASSWORD)
  }

  @Test
  fun testCreateKeycloakAdminClient() {
    val keycloak = keycloakAdminClientProvider.createKeycloakAdminClient(KEYCLOAK_URL)

    Assertions.assertNotNull(keycloak)
    Mockito.verify(keycloakConfiguration, Mockito.times(1)).realm
    Mockito.verify(keycloakConfiguration, Mockito.times(1)).clientId
    Mockito.verify(keycloakConfiguration, Mockito.times(1)).username
    Mockito.verify(keycloakConfiguration, Mockito.times(1)).password
  }

  companion object {
    private const val KEYCLOAK_URL = "http://localhost:8180/auth"
    private const val REALM = "airbyte"
    private const val CLIENT_ID = "admin-cli"
    private const val USERNAME = "admin"
    private const val PASSWORD = "admin"
  }
}
