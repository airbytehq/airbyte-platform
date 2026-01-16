/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.client.BatchRequest
import io.temporal.client.WorkflowOptions
import io.temporal.workflow.Functions
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConnectionManageUtilsTest {
  @Test
  fun signalAndRepairIfNecessaryWhenNoWorkflowWillCreate() {
    val mWorkflow = mockk<WorkflowClientWrapped>()
    val mMetric = mockk<MetricClient>(relaxed = true)
    val cid = UUID.randomUUID()

    every {
      mWorkflow.newWorkflowStub(
        any<Class<ConnectionManagerWorkflow>>(),
        any<WorkflowOptions>(),
      )
    } returns mockk<ConnectionManagerWorkflow>(relaxed = true)
    every { mWorkflow.newSignalWithStartRequest() } returns mockk<BatchRequest>(relaxed = true)
    every { mWorkflow.signalWithStart(any<BatchRequest>()) } returns mockk<WorkflowExecution>(relaxed = true)

    val utils = ConnectionManagerUtils(mWorkflow, mMetric)
    utils.signalWorkflowAndRepairIfNecessary(cid) { _: ConnectionManagerWorkflow? -> Functions.Proc {} }
    // Because we do not mock the getConnectionManagerWorkflow call, the underlying call throws an
    // exception
    // and the logic recreates it.
    verify { mWorkflow.signalWithStart(any<BatchRequest>()) }
  }
}
