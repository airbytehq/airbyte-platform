/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder
import org.keycloak.admin.client.ClientBuilderWrapper
import org.keycloak.admin.client.JacksonProvider
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder
import java.util.concurrent.TimeUnit

@Factory
class KeycloakBeanFactory(
  private val keycloakConfiguration: AirbyteKeycloakConfig,
) {
  @Singleton
  fun createKeycloakAdminClient(): Keycloak {
    val clientBuilder = ClientBuilderWrapper.create(null, false) as ResteasyClientBuilder
    clientBuilder.register(JacksonProvider::class.java, 100)
    val httpClient =
      clientBuilder
        .connectTimeout(keycloakConfiguration.connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .readTimeout(keycloakConfiguration.readTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .connectionCheckoutTimeout(keycloakConfiguration.connectionCheckoutTimeout.toMillis(), TimeUnit.MILLISECONDS)
        .build()

    return KeycloakBuilder
      .builder()
      .serverUrl(keycloakConfiguration.getServerUrl())
      .realm(keycloakConfiguration.realm)
      .clientId(keycloakConfiguration.clientId)
      .username(keycloakConfiguration.username)
      .password(keycloakConfiguration.password)
      .resteasyClient(httpClient)
      .build()
  }
}
