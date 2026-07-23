/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.SignalInput
import io.airbyte.config.SignalInput.Companion.SYNC_WORKFLOW
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.client.WorkflowClient
import org.junit.jupiter.api.Test

class SignalHandlerTest {
  private val mockWorkflowClient = mockk<WorkflowClient>()

  private val signalHandler = SignalHandler(mockWorkflowClient)

  private val workflowId = "workflowId"

  @Test
  fun `test sync signal send`() {
    val signalInput =
      SignalInput(
        workflowType = SYNC_WORKFLOW,
        workflowId = workflowId,
      )

    val syncWorkflow = mockk<SyncWorkflow>(relaxed = true)
    every { mockWorkflowClient.newWorkflowStub(SyncWorkflow::class.java, workflowId) } returns syncWorkflow

    signalHandler.signal(signalInput)
    verify(exactly = 1) { syncWorkflow.checkAsyncActivityStatus() }
  }
}
