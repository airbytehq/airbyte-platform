/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteConnectorBuilderApiConfigDefaultTest {
  @Inject
  private lateinit var airbyteConnectorBuilderApiConfig: AirbyteConnectorBuilderApiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteConnectorBuilderApiConfig.basePath)
    assertEquals("", airbyteConnectorBuilderApiConfig.signatureSecret)
    assertEquals(DEFAULT_CONNECTOR_BUILDER_API_CONNECT_TIMEOUT_SECONDS, airbyteConnectorBuilderApiConfig.connectTimeoutSeconds)
    assertEquals(DEFAULT_CONNECTOR_BUILDER_API_READ_TIMEOUT_SECONDS, airbyteConnectorBuilderApiConfig.readTimeoutSeconds)
  }
}

@MicronautTest(propertySources = ["classpath:application-connector-builder-api.yml"])
internal class AirbyteConnectorBuilderApiConfigOverridesTest {
  @Inject
  private lateinit var airbyteConnectorBuilderApiConfig: AirbyteConnectorBuilderApiConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-base-path", airbyteConnectorBuilderApiConfig.basePath)
    assertEquals("test-signature-secret", airbyteConnectorBuilderApiConfig.signatureSecret)
    assertEquals(1, airbyteConnectorBuilderApiConfig.connectTimeoutSeconds)
    assertEquals(2, airbyteConnectorBuilderApiConfig.readTimeoutSeconds)
  }
}
