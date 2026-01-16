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
internal class AirbyteConnectorBuilderConfigDefaultTest {
  @Inject
  private lateinit var airbyteConnectorBuilderConfig: AirbyteConnectorBuilderConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteConnectorBuilderConfig.aiAssist.urlBase)
    assertEquals(false, airbyteConnectorBuilderConfig.capabilities.enableUnsafeCode)
    assertEquals("", airbyteConnectorBuilderConfig.github.airbytePatToken)
  }
}

@MicronautTest(propertySources = ["classpath:application-connector-builder.yml"])
internal class AirbyteConnectorBuilderConfigOverridesTest {
  @Inject
  private lateinit var airbyteConnectorBuilderConfig: AirbyteConnectorBuilderConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("http://test-ai-assist", airbyteConnectorBuilderConfig.aiAssist.urlBase)
    assertEquals(true, airbyteConnectorBuilderConfig.capabilities.enableUnsafeCode)
    assertEquals("test-token", airbyteConnectorBuilderConfig.github.airbytePatToken)
  }
}
