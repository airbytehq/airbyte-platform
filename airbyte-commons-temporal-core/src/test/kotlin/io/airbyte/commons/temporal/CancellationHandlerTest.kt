/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.CancellationHandler.TemporalCancellationHandler
import io.airbyte.commons.temporal.stubs.TestWorkflow
import io.temporal.activity.Activity
import io.temporal.client.WorkflowOptions
import io.temporal.testing.TestWorkflowEnvironment
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class CancellationHandlerTest {
  @Test
  fun testCancellationHandler() {
    val testEnv = TestWorkflowEnvironment.newInstance()

    val worker = testEnv.newWorker("task-queue")

    worker.registerWorkflowImplementationTypes(TestWorkflow.TestWorkflowImpl::class.java)
    val client = testEnv.workflowClient

    worker.registerActivitiesImplementations(
      TestWorkflow.TestActivityImplTest {
        val context = Activity.getExecutionContext()
        TemporalCancellationHandler(context).checkAndHandleCancellation {}
      },
    )

    testEnv.start()

    val testWorkflow =
      client.newWorkflowStub(
        TestWorkflow::class.java,
        WorkflowOptions
          .newBuilder()
          .setTaskQueue("task-queue")
          .build(),
      )

    Assertions.assertDoesNotThrow { testWorkflow.execute() }
  }
}
