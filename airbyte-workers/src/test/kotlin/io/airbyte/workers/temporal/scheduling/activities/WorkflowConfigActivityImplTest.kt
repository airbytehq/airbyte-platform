/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.micronaut.runtime.AirbyteWorkflowConfig
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Test suite for the [WorkflowConfigActivityImpl] class.
 */
@MicronautTest(environments = [Environment.TEST])
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
@Property(name = "STORAGE_TYPE", value = "LOCAL")
internal class WorkflowConfigActivityImplTest {
  @Inject
  private lateinit var airbyteWorkflowConfig: AirbyteWorkflowConfig

  @Test
  @Property(name = "airbyte.workflow.failure.restart-delay", value = "30")
  fun testFetchingWorkflowRestartDelayInSeconds() {
    val workflowRestartDelaySeconds = 30L
    val activity = WorkflowConfigActivityImpl(airbyteWorkflowConfig)
    Assertions.assertEquals(workflowRestartDelaySeconds, activity.getWorkflowRestartDelaySeconds().seconds)
  }
}
