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
internal class AirbyteManifestServerApiClientConfigDefaultTest {
  @Inject
  private lateinit var airbyteManifestServerApiClientConfig: AirbyteManifestServerApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteManifestServerApiClientConfig.basePath)
    assertEquals(DEFAULT_MANIFEST_SERVER_API_CONNECT_TIMEOUT_SECONDS, airbyteManifestServerApiClientConfig.connectTimeoutSeconds)
    assertEquals(DEFAULT_MANIFEST_SERVER_API_READ_TIMEOUT_SECONDS, airbyteManifestServerApiClientConfig.readTimeoutSeconds)
    assertEquals("", airbyteManifestServerApiClientConfig.signatureSecret)
  }
}

@MicronautTest(propertySources = ["classpath:application-manifest-server-api.yml"])
internal class AirbyteManifestServerApiClientConfigTest {
  @Inject
  private lateinit var airbyteManifestServerApiClientConfig: AirbyteManifestServerApiClientConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://localhost:8003", airbyteManifestServerApiClientConfig.basePath)
    assertEquals(90, airbyteManifestServerApiClientConfig.connectTimeoutSeconds)
    assertEquals(150, airbyteManifestServerApiClientConfig.readTimeoutSeconds)
    assertEquals("test-signature-secret", airbyteManifestServerApiClientConfig.signatureSecret)
  }
}
