/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth;

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;

@Factory
public class KeycloakBeanFactory {

  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public KeycloakBeanFactory(AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.keycloakConfiguration = keycloakConfiguration;
  }

  @Singleton
  public Keycloak createKeycloakAdminClient() {
    return KeycloakBuilder.builder()
        .serverUrl(keycloakConfiguration.getServerUrl())
        .realm(keycloakConfiguration.getRealm())
        .clientId(keycloakConfiguration.getClientId())
        .username(keycloakConfiguration.getUsername())
        .password(keycloakConfiguration.getPassword())
        .build();
  }

}
