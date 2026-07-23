/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.SignalInput
import io.temporal.client.WorkflowClient
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class SignalHandler(
  @Named("workerWorkflowClient") private val workflowClient: WorkflowClient,
) {
  fun signal(signalInput: SignalInput) {
    when (signalInput.workflowType) {
      SignalInput.CONNECTOR_COMMAND_WORKFLOW -> signalConnectorCommand(signalInput)
      SignalInput.SYNC_WORKFLOW -> signalSync(signalInput)
    }
  }

  private fun signalConnectorCommand(signalInput: SignalInput) {
    val workflow = getWorkflowStub(signalInput.workflowId, ConnectorCommandWorkflow::class.java)
    workflow.checkTerminalStatus()
  }

  private fun signalSync(signalInput: SignalInput) {
    val workflow = getWorkflowStub(signalInput.workflowId, SyncWorkflow::class.java)
    workflow.checkAsyncActivityStatus()
  }

  fun <T> getWorkflowStub(
    workflowId: String,
    workflowType: Class<T>,
  ): T = workflowClient.newWorkflowStub(workflowType, workflowId)
}
