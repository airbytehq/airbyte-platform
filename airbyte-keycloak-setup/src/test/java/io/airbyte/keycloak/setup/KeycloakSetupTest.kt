/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.client.BlockingHttpClient
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.MockitoAnnotations

internal class KeycloakSetupTest {
  @Mock
  private lateinit var httpClient: HttpClient

  @Mock
  private lateinit var blockingHttpClient: BlockingHttpClient

  @Mock
  private lateinit var keycloakServer: KeycloakServer

  @Mock
  private lateinit var keycloakConfiguration: AirbyteKeycloakConfiguration

  @Mock
  private lateinit var configDbResetHelper: ConfigDbResetHelper

  private lateinit var keycloakSetup: KeycloakSetup

  @BeforeEach
  fun setup() {
    MockitoAnnotations.openMocks(this)

    Mockito.`when`(keycloakServer.keycloakServerUrl).thenReturn("http://localhost:8180/auth")
    Mockito.`when`(httpClient.toBlocking()).thenReturn(blockingHttpClient)
    Mockito
      .`when`(
        blockingHttpClient.exchange(
          ArgumentMatchers.any(HttpRequest::class.java),
          ArgumentMatchers.eq(String::class.java),
        ),
      ).thenReturn(HttpResponse.ok())

    keycloakSetup = KeycloakSetup(httpClient, keycloakServer, keycloakConfiguration, configDbResetHelper)
  }

  @Test
  @Throws(Exception::class)
  fun testRun() {
    keycloakSetup.run()

    Mockito.verify(httpClient).toBlocking()
    Mockito.verify(blockingHttpClient).exchange(
      ArgumentMatchers.any(HttpRequest::class.java),
      ArgumentMatchers.eq(String::class.java),
    )
    Mockito.verify(keycloakServer).setupAirbyteRealm()
    Mockito.verify(keycloakServer).closeKeycloakAdminClient()
    Mockito.verify(configDbResetHelper, Mockito.never()).deleteConfigDbUsers()
  }

  @Test
  fun testRunThrowsException() {
    Mockito
      .`when`(
        httpClient.toBlocking().exchange(
          ArgumentMatchers.any(HttpRequest::class.java),
          ArgumentMatchers.eq(String::class.java),
        ),
      ).thenThrow(HttpClientResponseException("Error", HttpResponse.serverError<String>()))

    Assertions.assertThrows(HttpClientResponseException::class.java) { keycloakSetup.run() }

    Mockito.verify(keycloakServer).keycloakServerUrl
    Mockito.verify(httpClient.toBlocking()).exchange(
      ArgumentMatchers.any(HttpRequest::class.java),
      ArgumentMatchers.eq(String::class.java),
    )
    Mockito.verify(keycloakServer, Mockito.never()).setupAirbyteRealm() // Should not be called if exception is thrown
    Mockito.verify(keycloakServer).closeKeycloakAdminClient()
  }

  @Test
  @Throws(Exception::class)
  fun testResetRealm() {
    Mockito.`when`(keycloakConfiguration.resetRealm).thenReturn(true)

    keycloakSetup.run()

    Mockito.verify(keycloakServer, Mockito.times(0)).setupAirbyteRealm()
    Mockito.verify(keycloakServer, Mockito.times(1)).destroyAndRecreateAirbyteRealm()
    Mockito.verify(configDbResetHelper, Mockito.times(1)).deleteConfigDbUsers()
  }
}
