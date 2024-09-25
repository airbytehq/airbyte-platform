package io.airbyte.commons.server.handlers

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.SignalInput
import io.temporal.client.WorkflowClient
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
    val workflow = getWorkflowStub(signalInput.workflowId, SyncWorkflow::class.java)
    workflow.checkAsyncActivityStatus()
  }

  fun <T> getWorkflowStub(
    workflowId: String,
    workflowType: Class<T>,
  ): T {
    return workflowClient.newWorkflowStub(workflowType, workflowId)
  }
}
