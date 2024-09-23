package io.airbyte.commons.server.handlers

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.SignalInput
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import jakarta.inject.Singleton

@Singleton
class SignalHandler(
  private val workflowClient: WorkflowClient,
) {
  fun signal(signalInput: SignalInput) {
    if (signalInput.workflowType == SignalInput.SYNC_WORKFLOW) {
      signalSync(signalInput)
    }
  }

  private fun signalSync(signalInput: SignalInput) {
    // TODO: Send signal.
    val workflow = getWorkflowStub(signalInput.workflowId, signalInput.taskQueue, SyncWorkflow::class.java)
  }

  fun <T> getWorkflowStub(
    workflowId: String,
    taskQueue: String,
    workflowType: Class<T>,
  ): T {
    val workflowOption =
      WorkflowOptions.newBuilder()
        .setWorkflowId(workflowId)
        .setTaskQueue(taskQueue)
        .build()

    return workflowClient.newWorkflowStub(workflowType, workflowOption)
  }
}
