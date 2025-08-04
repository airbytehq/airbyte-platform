/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.exception.DeletedWorkflowException
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.metrics.MetricClient
import io.temporal.client.BatchRequest
import io.temporal.client.WorkflowOptions
import io.temporal.workflow.Functions
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.any
import java.util.UUID
import java.util.function.Function

internal class ConnectionManageUtilsTest {
  @Test
  @Throws(DeletedWorkflowException::class)
  fun signalAndRepairIfNeceesaryWhenNoWorkflowWillCreate() {
    val mWorkflow = Mockito.mock(WorkflowClientWrapped::class.java)
    val mMetric = Mockito.mock(MetricClient::class.java)
    val cid = UUID.randomUUID()

    Mockito
      .`when`(
        mWorkflow.newWorkflowStub(
          ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
          ArgumentMatchers.any<WorkflowOptions>(),
        ),
      ).thenReturn(Mockito.mock(ConnectionManagerWorkflow::class.java))
    Mockito.`when`(mWorkflow.newSignalWithStartRequest()).thenReturn(Mockito.mock(BatchRequest::class.java))

    val utils = ConnectionManagerUtils(mWorkflow, mMetric)
    utils.signalWorkflowAndRepairIfNecessary(cid, Function { workflow: ConnectionManagerWorkflow? -> Functions.Proc {} })
    // Because we do not mock the getConnectionManagerWorkflow call, the underlying call throws an
    // exception
    // and the logic recreates it.
    Mockito.verify(mWorkflow).signalWithStart(any<BatchRequest>())
  }
}
