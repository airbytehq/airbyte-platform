/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import jakarta.inject.Singleton;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;

/**
 * This class provides a Keycloak administration client. It can create and configure the client
 * based on specified parameters.
 */
@Singleton
public class KeycloakAdminClientProvider {

  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public KeycloakAdminClientProvider(AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.keycloakConfiguration = keycloakConfiguration;
  }

  public synchronized Keycloak createKeycloakAdminClient(final String keycloakUrl) {
    return KeycloakBuilder.builder()
        .serverUrl(keycloakUrl)
        .realm(keycloakConfiguration.getRealm())
        .clientId(keycloakConfiguration.getClientId())
        .username(keycloakConfiguration.getUsername())
        .password(keycloakConfiguration.getPassword())
        .build();
  }

}
