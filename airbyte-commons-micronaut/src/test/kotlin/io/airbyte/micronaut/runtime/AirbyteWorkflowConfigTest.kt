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
internal class AirbyteWorkflowConfigDefaultTest {
  @Inject
  private lateinit var airbyteWorkflowConfig: AirbyteWorkflowConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_WORKFLOW_FAILURE_RESTART_DELAY, airbyteWorkflowConfig.failure.restartDelay)
  }
}

@MicronautTest(propertySources = ["classpath:application-workflow.yml"])
internal class AirbyteWorkflowConfigOverridesTest {
  @Inject
  private lateinit var airbyteWorkflowConfig: AirbyteWorkflowConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(100L, airbyteWorkflowConfig.failure.restartDelay)
  }
}
