/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
internal class KeycloakAdminClientProviderTest {
  @MockK
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig

  @InjectMockKs
  private lateinit var keycloakAdminClientProvider: KeycloakAdminClientProvider

  @BeforeEach
  fun setUp() {
    every { keycloakConfiguration.realm } returns REALM
    every { keycloakConfiguration.clientId } returns CLIENT_ID
    every { keycloakConfiguration.username } returns USERNAME
    every { keycloakConfiguration.password } returns PASSWORD
  }

  @Test
  fun testCreateKeycloakAdminClient() {
    val keycloak = keycloakAdminClientProvider.createKeycloakAdminClient(KEYCLOAK_URL)

    Assertions.assertNotNull(keycloak)
    verify(exactly = 1) { keycloakConfiguration.realm }
    verify(exactly = 1) { keycloakConfiguration.clientId }
    verify(exactly = 1) { keycloakConfiguration.username }
    verify(exactly = 1) { keycloakConfiguration.password }
  }

  companion object {
    private const val KEYCLOAK_URL = "http://localhost:8180/auth"
    private const val REALM = "airbyte"
    private const val CLIENT_ID = "admin-cli"
    private const val USERNAME = "admin"
    private const val PASSWORD = "admin"
  }
}
