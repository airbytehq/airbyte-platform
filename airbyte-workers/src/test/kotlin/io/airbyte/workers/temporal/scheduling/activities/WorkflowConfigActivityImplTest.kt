/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Test suite for the [WorkflowConfigActivityImpl] class.
 */
internal class WorkflowConfigActivityImplTest {
  @Test
  fun testFetchingWorkflowRestartDelayInSeconds() {
    val workflowRestartDelaySeconds = 30L
    val activity = WorkflowConfigActivityImpl(workflowRestartDelaySeconds)
    Assertions.assertEquals(workflowRestartDelaySeconds, activity.getWorkflowRestartDelaySeconds().getSeconds())
  }
}
