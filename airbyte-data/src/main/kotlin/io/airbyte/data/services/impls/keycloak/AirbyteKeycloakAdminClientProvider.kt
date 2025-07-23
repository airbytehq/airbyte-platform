/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import jakarta.inject.Singleton
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder

/**
 * This class creates an instance of a keycloak admin client based on the config
 * variables provided in [AirbyteKeycloakConfiguration]. This is a copy of the logic located in
 * [KeycloakAdminClientProvider], in the airbyte-keycloak-setup package. We don't import that here,
 * as it is considered standalone and only runs on startup.
 */
@Singleton
class AirbyteKeycloakAdminClientProvider(
  private val keycloakConfiguration: AirbyteKeycloakConfiguration,
) {
  @Synchronized
  fun createKeycloakAdminClient(): Keycloak =
    KeycloakBuilder
      .builder()
      .serverUrl(getServerUrl())
      .realm(keycloakConfiguration.realm)
      .clientId(keycloakConfiguration.clientId)
      .username(keycloakConfiguration.username)
      .password(keycloakConfiguration.password)
      .build()

  private fun getServerUrl(): String {
    val basePath = keycloakConfiguration.basePath
    val basePathWithLeadingSlash = if (basePath.startsWith("/")) basePath else "/$basePath"
    return keycloakConfiguration.protocol + "://" + keycloakConfiguration.host + basePathWithLeadingSlash
  }
}
