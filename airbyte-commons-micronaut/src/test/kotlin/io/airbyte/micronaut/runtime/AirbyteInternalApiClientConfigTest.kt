/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteInternalApiClientConfigDefaultTest {
  @Inject
  private lateinit var airbyteInternalApiClientConfig: AirbyteInternalApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteInternalApiClientConfig.basePath)
    assertEquals(30, airbyteInternalApiClientConfig.connectTimeoutSeconds)
    assertEquals(600, airbyteInternalApiClientConfig.readTimeoutSeconds)
    assertEquals(2, airbyteInternalApiClientConfig.retries.delaySeconds)
    assertEquals(.25, airbyteInternalApiClientConfig.retries.jitterFactor)
    assertEquals(5, airbyteInternalApiClientConfig.retries.max)
    assertEquals("", airbyteInternalApiClientConfig.auth.clientId)
    assertEquals("", airbyteInternalApiClientConfig.auth.clientSecret)
    assertEquals("", airbyteInternalApiClientConfig.auth.token)
    assertEquals("", airbyteInternalApiClientConfig.auth.tokenEndpoint)
    assertEquals(AirbyteInternalApiClientConfig.AuthType.INTERNAL_CLIENT_TOKEN, airbyteInternalApiClientConfig.auth.type)
  }
}

@MicronautTest(propertySources = ["classpath:application-internal-api-dataplane-access-token.yml"])
internal class AirbyteInternalApiClientConfigDataplaneAccessTokenTest {
  @Inject
  private lateinit var airbyteInternalApiClientConfig: AirbyteInternalApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://localhost:8001/api", airbyteInternalApiClientConfig.basePath)
    assertEquals(60, airbyteInternalApiClientConfig.connectTimeoutSeconds)
    assertEquals(300, airbyteInternalApiClientConfig.readTimeoutSeconds)
    assertEquals(10, airbyteInternalApiClientConfig.retries.delaySeconds)
    assertEquals(.75, airbyteInternalApiClientConfig.retries.jitterFactor)
    assertEquals(2, airbyteInternalApiClientConfig.retries.max)
    assertEquals("test-client-id", airbyteInternalApiClientConfig.auth.clientId)
    assertEquals("test-client-secret", airbyteInternalApiClientConfig.auth.clientSecret)
    assertEquals("test-token", airbyteInternalApiClientConfig.auth.token)
    assertEquals("test-token-endpoint", airbyteInternalApiClientConfig.auth.tokenEndpoint)
    assertEquals(AirbyteInternalApiClientConfig.AuthType.DATAPLANE_ACCESS_TOKEN, airbyteInternalApiClientConfig.auth.type)
  }
}

@MicronautTest(propertySources = ["classpath:application-internal-api-internal-client-token.yml"])
internal class AirbyteInternalApiClientConfigInternalClientTokenTest {
  @Inject
  private lateinit var airbyteInternalApiClientConfig: AirbyteInternalApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://localhost:8001/api", airbyteInternalApiClientConfig.basePath)
    assertEquals(60, airbyteInternalApiClientConfig.connectTimeoutSeconds)
    assertEquals(300, airbyteInternalApiClientConfig.readTimeoutSeconds)
    assertEquals(10, airbyteInternalApiClientConfig.retries.delaySeconds)
    assertEquals(.75, airbyteInternalApiClientConfig.retries.jitterFactor)
    assertEquals(2, airbyteInternalApiClientConfig.retries.max)
    assertEquals("test-client-id", airbyteInternalApiClientConfig.auth.clientId)
    assertEquals("test-client-secret", airbyteInternalApiClientConfig.auth.clientSecret)
    assertEquals("test-token", airbyteInternalApiClientConfig.auth.token)
    assertEquals("test-token-endpoint", airbyteInternalApiClientConfig.auth.tokenEndpoint)
    assertEquals(AirbyteInternalApiClientConfig.AuthType.INTERNAL_CLIENT_TOKEN, airbyteInternalApiClientConfig.auth.type)
  }
}
