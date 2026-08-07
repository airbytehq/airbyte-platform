/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.jboss.resteasy.client.jaxrs.ResteasyClient
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.ClientBuilderWrapper
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.KeycloakBuilder
import java.time.Duration
import java.util.concurrent.TimeUnit

class KeycloakBeanFactoryTest {
  @Test
  fun `admin client uses configured connect, read, and checkout timeouts`() {
    val configuration =
      AirbyteKeycloakConfig(
        host = "keycloak",
        connectTimeout = Duration.ofMillis(125),
        connectionCheckoutTimeout = Duration.ofMillis(225),
        readTimeout = Duration.ofMillis(375),
      )
    val httpClientBuilder = mockk<ResteasyClientBuilder>()
    val httpClient = mockk<ResteasyClient>()
    val keycloakBuilder = mockk<KeycloakBuilder>()
    val keycloak = mockk<Keycloak>()

    mockkStatic(ClientBuilderWrapper::class)
    mockkStatic(KeycloakBuilder::class)
    every { ClientBuilderWrapper.create(null, false) } returns httpClientBuilder
    every { httpClientBuilder.register(any<Class<*>>(), any<Int>()) } returns httpClientBuilder
    every { httpClientBuilder.connectTimeout(any(), any()) } returns httpClientBuilder
    every { httpClientBuilder.readTimeout(any(), any()) } returns httpClientBuilder
    every { httpClientBuilder.connectionCheckoutTimeout(any(), any()) } returns httpClientBuilder
    every { httpClientBuilder.build() } returns httpClient
    every { KeycloakBuilder.builder() } returns keycloakBuilder
    every { keycloakBuilder.serverUrl(any()) } returns keycloakBuilder
    every { keycloakBuilder.realm(any()) } returns keycloakBuilder
    every { keycloakBuilder.clientId(any()) } returns keycloakBuilder
    every { keycloakBuilder.username(any()) } returns keycloakBuilder
    every { keycloakBuilder.password(any()) } returns keycloakBuilder
    every { keycloakBuilder.resteasyClient(any()) } returns keycloakBuilder
    every { keycloakBuilder.build() } returns keycloak

    try {
      KeycloakBeanFactory(configuration).createKeycloakAdminClient()

      verify(exactly = 1) { httpClientBuilder.connectTimeout(125, TimeUnit.MILLISECONDS) }
      verify(exactly = 1) { httpClientBuilder.readTimeout(375, TimeUnit.MILLISECONDS) }
      verify(exactly = 1) { httpClientBuilder.connectionCheckoutTimeout(225, TimeUnit.MILLISECONDS) }
      verify(exactly = 1) { keycloakBuilder.resteasyClient(httpClient) }
    } finally {
      unmockkStatic(KeycloakBuilder::class)
      unmockkStatic(ClientBuilderWrapper::class)
    }
  }
}
