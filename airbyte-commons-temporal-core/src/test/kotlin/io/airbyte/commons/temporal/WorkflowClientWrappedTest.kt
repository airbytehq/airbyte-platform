/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.metrics.MetricClient
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.serviceclient.WorkflowServiceStubs
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.anyVararg

internal class WorkflowClientWrappedTest {
  internal class MyWorkflow

  lateinit var metricClient: MetricClient
  lateinit var temporalWorkflowServiceStubs: WorkflowServiceStubs
  lateinit var temporalWorkflowServiceBlockingStub: WorkflowServiceBlockingStub
  lateinit var temporalWorkflowClient: WorkflowClient
  lateinit var workflowClient: WorkflowClientWrapped

  @BeforeEach
  fun beforeEach() {
    metricClient = Mockito.mock(MetricClient::class.java)
    temporalWorkflowServiceBlockingStub = Mockito.mock(WorkflowServiceBlockingStub::class.java)
    temporalWorkflowServiceStubs = Mockito.mock(WorkflowServiceStubs::class.java)
    Mockito.`when`(temporalWorkflowServiceStubs.blockingStub()).thenReturn(temporalWorkflowServiceBlockingStub)
    temporalWorkflowClient = Mockito.mock(WorkflowClient::class.java)
    Mockito.`when`(temporalWorkflowClient.getWorkflowServiceStubs()).thenReturn(temporalWorkflowServiceStubs)
    workflowClient =
      WorkflowClientWrapped(temporalWorkflowClient, metricClient, MAX_ATTEMPT, BACKOFF_DELAY_IN_MILLIS, BACKOFF_MAX_DELAY_IN_MILLIS)
  }

  @Test
  fun testRetryLogic() {
    Mockito.`when`(temporalWorkflowClient.newWorkflowStub(ArgumentMatchers.any<Class<Any>>(), ArgumentMatchers.anyString())).thenThrow(
      unavailable(),
    )

    Assertions.assertThrows(
      StatusRuntimeException::class.java,
    ) { workflowClient.newWorkflowStub(MyWorkflow::class.java, "fail") }
    Mockito.verify(temporalWorkflowClient, Mockito.times(3)).newWorkflowStub(ArgumentMatchers.any<Class<Any>>(), ArgumentMatchers.anyString())
    Mockito.verify(metricClient, Mockito.times(2)).count(anyOrNull(), anyOrNull(), anyVararg())
  }

  @Test
  fun testNewWorkflowStub() {
    val expected = MyWorkflow()
    Mockito
      .`when`(temporalWorkflowClient.newWorkflowStub(ArgumentMatchers.any<Class<Any>>(), ArgumentMatchers.anyString()))
      .thenThrow(unavailable())
      .thenReturn(expected)

    val actual = workflowClient.newWorkflowStub(MyWorkflow::class.java, "woot")
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testNewWorkflowStubWithOptions() {
    val expected = MyWorkflow()
    Mockito
      .`when`(temporalWorkflowClient.newWorkflowStub(anyOrNull<Class<Any>>(), anyOrNull<WorkflowOptions>()))
      .thenThrow(unavailable())
      .thenReturn(expected)

    val actual = workflowClient.newWorkflowStub(MyWorkflow::class.java, WorkflowOptions.getDefaultInstance())
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testTerminateWorkflow() {
    val workflowStub = Mockito.mock(WorkflowStub::class.java)
    Mockito
      .`when`(temporalWorkflowClient.newUntypedWorkflowStub(ArgumentMatchers.anyString()))
      .thenThrow(unavailable())
      .thenReturn(workflowStub)

    workflowClient.terminateWorkflow("workflow", "test terminate")
    Mockito.verify(temporalWorkflowClient, Mockito.times(2)).newUntypedWorkflowStub("workflow")
    Mockito.verify(workflowStub).terminate("test terminate")
  }

  @Test
  fun testBlockingDescribeWorkflowExecution() {
    val expected = Mockito.mock(DescribeWorkflowExecutionResponse::class.java)
    Mockito
      .`when`(temporalWorkflowServiceBlockingStub.describeWorkflowExecution(ArgumentMatchers.any()))
      .thenThrow(unavailable())
      .thenReturn(expected)

    val actual =
      workflowClient.blockingDescribeWorkflowExecution(
        Mockito.mock(
          DescribeWorkflowExecutionRequest::class.java,
        ),
      )
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testSignalsAreNotRetried() {
    Mockito.`when`(temporalWorkflowClient.signalWithStart(ArgumentMatchers.any())).thenThrow(unavailable())
    Mockito.`when`(temporalWorkflowClient.newSignalWithStartRequest()).thenReturn(Mockito.mock())
    Assertions.assertThrows(StatusRuntimeException::class.java) {
      val request = workflowClient.newSignalWithStartRequest()
      workflowClient.signalWithStart(request)
    }
    Mockito.verify(temporalWorkflowClient, Mockito.times(1)).signalWithStart(ArgumentMatchers.any())
  }

  companion object {
    private const val MAX_ATTEMPT = 3
    private const val BACKOFF_DELAY_IN_MILLIS = 1
    private const val BACKOFF_MAX_DELAY_IN_MILLIS = 10

    private fun unavailable(): StatusRuntimeException = StatusRuntimeException(Status.UNAVAILABLE)
  }
}
