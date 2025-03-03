/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder

@Factory
class KeycloakBeanFactory(
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
) {
  @Singleton
  fun createKeycloakAdminClient(): Keycloak =
    KeycloakBuilder
      .builder()
      .serverUrl(keycloakConfiguration.getServerUrl())
      .realm(keycloakConfiguration.realm)
      .clientId(keycloakConfiguration.clientId)
      .username(keycloakConfiguration.username)
      .password(keycloakConfiguration.password)
      .build()
}
