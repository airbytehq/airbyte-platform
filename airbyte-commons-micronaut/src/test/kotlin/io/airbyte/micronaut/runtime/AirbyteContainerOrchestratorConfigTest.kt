/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.runtime

import io.airbyte.micronaut.runtime.AirbyteContainerOrchestratorConfig
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteContainerOrchestratorConfigDefaultTest {
  @Inject
  private lateinit var airbyteContainerOrchestratorConfig: AirbyteContainerOrchestratorConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteContainerOrchestratorConfig.enableUnsafeCode)
    assertEquals("", airbyteContainerOrchestratorConfig.javaOpts)
  }
}

@MicronautTest(propertySources = ["classpath:application-container-orchestrator.yml"])
internal class AirbyteContainerOrchestratorConfigOverridesTest {
  @Inject
  private lateinit var airbyteContainerOrchestratorConfig: AirbyteContainerOrchestratorConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteContainerOrchestratorConfig.enableUnsafeCode)
    assertEquals("test-java-opts", airbyteContainerOrchestratorConfig.javaOpts)
  }
}
