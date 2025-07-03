/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import jakarta.inject.Singleton
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder

/**
 * This class provides a Keycloak administration client. It can create and configure the client
 * based on specified parameters.
 */
@Singleton
class KeycloakAdminClientProvider(
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
) {
  @Synchronized
  fun createKeycloakAdminClient(keycloakUrl: String?): Keycloak =
    KeycloakBuilder
      .builder()
      .serverUrl(keycloakUrl)
      .realm(keycloakConfiguration.realm)
      .clientId(keycloakConfiguration.clientId)
      .username(keycloakConfiguration.username)
      .password(keycloakConfiguration.password)
      .build()
}
