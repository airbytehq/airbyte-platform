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
internal class AirbyteDataDogConfigDefaultTest {
  @Inject
  private lateinit var airbyteDataDogConfig: AirbyteDataDogConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteDataDogConfig.agent.host)
    assertEquals("", airbyteDataDogConfig.agent.port)
    assertEquals(DEFAULT_DATA_DOG_ORCHESTRATOR_DISABLED_INTEGRATIONS, airbyteDataDogConfig.orchestratorDisabledIntegrations)
    assertEquals("", airbyteDataDogConfig.env)
    assertEquals("", airbyteDataDogConfig.version)
  }
}

@MicronautTest(propertySources = ["classpath:application-datadog.yml"])
internal class AirbyteDataDogConfigOverridesTest {
  @Inject
  private lateinit var airbyteDataDogConfig: AirbyteDataDogConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-datadog-agent-host", airbyteDataDogConfig.agent.host)
    assertEquals("1", airbyteDataDogConfig.agent.port)
    assertEquals("test-datadog-orchestrator-disabled-integrations", airbyteDataDogConfig.orchestratorDisabledIntegrations)
    assertEquals("test-datadog-env", airbyteDataDogConfig.env)
    assertEquals("test-datadog-version", airbyteDataDogConfig.version)
  }
}
