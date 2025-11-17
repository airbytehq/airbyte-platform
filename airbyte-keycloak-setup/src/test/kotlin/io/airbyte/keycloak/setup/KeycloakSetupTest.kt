/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockKExtension::class)
internal class KeycloakSetupTest {
  @MockK
  private lateinit var httpClient: HttpClient

  @MockK
  private lateinit var blockingHttpClient: BlockingHttpClient

  @MockK
  private lateinit var keycloakServer: KeycloakServer

  @MockK
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfig

  @MockK
  private lateinit var configDbResetHelper: ConfigDbResetHelper

  private lateinit var keycloakSetup: KeycloakSetup

  @BeforeEach
  fun setup() {
    every { keycloakServer.keycloakServerUrl } returns "http://localhost:8180/auth"
    every { httpClient.toBlocking() } returns blockingHttpClient
    every { blockingHttpClient.exchange(any<HttpRequest<*>>(), eq(String::class.java)) } returns HttpResponse.ok()

    keycloakSetup = KeycloakSetup(httpClient, keycloakServer, keycloakConfiguration, configDbResetHelper)
  }

  @Test
  fun testRun() {
    every { keycloakServer.setupAirbyteRealm() } returns Unit
    every { keycloakServer.closeKeycloakAdminClient() } returns Unit
    every { keycloakConfiguration.resetRealm } returns false

    keycloakSetup.run()

    verify(exactly = 1) { httpClient.toBlocking() }
    verify(exactly = 1) { blockingHttpClient.exchange(any<HttpRequest<*>>(), eq(String::class.java)) }
    verify(exactly = 1) { keycloakServer.setupAirbyteRealm() }
    verify(exactly = 1) { keycloakServer.closeKeycloakAdminClient() }
    verify(exactly = 0) { configDbResetHelper.deleteConfigDbUsers() }
  }

  @Test
  fun testRunThrowsException() {
    every { httpClient.toBlocking().exchange(any<HttpRequest<*>>(), eq(String::class.java)) } throws
      HttpClientResponseException("Error", HttpResponse.serverError<String>())
    every { keycloakServer.closeKeycloakAdminClient() } returns Unit

    Assertions.assertThrows(HttpClientResponseException::class.java) { keycloakSetup.run() }

    verify(exactly = 1) { keycloakServer.keycloakServerUrl }
    verify(exactly = 1) { httpClient.toBlocking().exchange(any<HttpRequest<*>>(), eq(String::class.java)) }
    verify(exactly = 0) { keycloakServer.setupAirbyteRealm() }
    verify(exactly = 1) { keycloakServer.closeKeycloakAdminClient() }
  }

  @Test
  fun testResetRealm() {
    every { keycloakConfiguration.resetRealm } returns true
    every { keycloakServer.destroyAndRecreateAirbyteRealm() } returns Unit
    every { configDbResetHelper.deleteConfigDbUsers() } returns Unit
    every { keycloakServer.closeKeycloakAdminClient() } returns Unit

    keycloakSetup.run()

    verify(exactly = 0) { keycloakServer.setupAirbyteRealm() }
    verify(exactly = 1) { keycloakServer.destroyAndRecreateAirbyteRealm() }
    verify(exactly = 1) { configDbResetHelper.deleteConfigDbUsers() }
  }
}
