package io.airbyte.commons.server.handlers

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.SignalInput
import io.airbyte.config.SignalInput.Companion.SYNC_WORKFLOW
import io.mockk.every
import io.mockk.mockk
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import org.junit.jupiter.api.Test

class SignalHandlerTest {
  private val mockWorkflowClient = mockk<WorkflowClient>()

  private val signalHandler = SignalHandler(mockWorkflowClient)

  private val workflowId = "workflowId"
  private val taskQueue = "taskQueue"

  @Test
  fun `test sync signal send`() {
    val signalInput =
      SignalInput(
        workflowType = SYNC_WORKFLOW,
        workflowId = workflowId,
        taskQueue = taskQueue,
      )

    mockWorkflowClient(workflowId, taskQueue, SyncWorkflow::class.java)

    signalHandler.signal(signalInput)
  }

  fun <T> mockWorkflowClient(
    workflowId: String,
    taskQueue: String,
    workflowClass: Class<T>,
  ) {
    val workflowOption =
      WorkflowOptions.newBuilder()
        .setWorkflowId(workflowId)
        .setTaskQueue(taskQueue)
        .build()
    when (workflowClass) {
      SyncWorkflow::class.java -> every { mockWorkflowClient.newWorkflowStub(workflowClass, workflowOption) } returns mockk<SyncWorkflow>()
      else -> throw IllegalArgumentException("Unknown workflow class: $workflowClass")
    }
  }
}
