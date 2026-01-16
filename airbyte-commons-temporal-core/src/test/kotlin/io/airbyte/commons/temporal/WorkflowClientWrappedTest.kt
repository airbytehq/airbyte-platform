/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.metrics.MetricClient
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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

internal class WorkflowClientWrappedTest {
  internal class MyWorkflow

  private lateinit var metricClient: MetricClient
  private lateinit var temporalWorkflowServiceStubs: WorkflowServiceStubs
  private lateinit var temporalWorkflowServiceBlockingStub: WorkflowServiceBlockingStub
  private lateinit var temporalWorkflowClient: WorkflowClient
  private lateinit var workflowClient: WorkflowClientWrapped

  @BeforeEach
  fun beforeEach() {
    metricClient = mockk(relaxed = true)
    temporalWorkflowServiceBlockingStub = mockk()
    temporalWorkflowServiceStubs = mockk()
    every { temporalWorkflowServiceStubs.blockingStub() } returns temporalWorkflowServiceBlockingStub
    temporalWorkflowClient = mockk()
    every { temporalWorkflowClient.workflowServiceStubs } returns temporalWorkflowServiceStubs
    workflowClient =
      WorkflowClientWrapped(temporalWorkflowClient, metricClient, MAX_ATTEMPT, BACKOFF_DELAY_IN_MILLIS, BACKOFF_MAX_DELAY_IN_MILLIS)
  }

  @Test
  fun testRetryLogic() {
    every { temporalWorkflowClient.newWorkflowStub(any<Class<Any>>(), any<String>()) } throws unavailable()

    Assertions.assertThrows(
      StatusRuntimeException::class.java,
    ) { workflowClient.newWorkflowStub(MyWorkflow::class.java, "fail") }
    verify(exactly = 3) { temporalWorkflowClient.newWorkflowStub(any<Class<Any>>(), any<String>()) }
    verify(exactly = 2) { metricClient.count(any(), any(), *anyVararg()) }
  }

  @Test
  fun testNewWorkflowStub() {
    val expected = MyWorkflow()
    every { temporalWorkflowClient.newWorkflowStub(any<Class<Any>>(), any<String>()) } throws unavailable() andThen expected

    val actual = workflowClient.newWorkflowStub(MyWorkflow::class.java, "woot")
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testNewWorkflowStubWithOptions() {
    val expected = MyWorkflow()
    every { temporalWorkflowClient.newWorkflowStub(any<Class<Any>>(), any<WorkflowOptions>()) } throws unavailable() andThen expected

    val actual = workflowClient.newWorkflowStub(MyWorkflow::class.java, WorkflowOptions.getDefaultInstance())
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testTerminateWorkflow() {
    val workflowStub = mockk<WorkflowStub>(relaxed = true)
    every { temporalWorkflowClient.newUntypedWorkflowStub(any<String>()) } throws unavailable() andThen workflowStub

    workflowClient.terminateWorkflow("workflow", "test terminate")
    verify(exactly = 2) { temporalWorkflowClient.newUntypedWorkflowStub("workflow") }
    verify { workflowStub.terminate("test terminate") }
  }

  @Test
  fun testBlockingDescribeWorkflowExecution() {
    val expected = mockk<DescribeWorkflowExecutionResponse>()
    every { temporalWorkflowServiceBlockingStub.describeWorkflowExecution(any()) } throws unavailable() andThen expected

    val actual =
      workflowClient.blockingDescribeWorkflowExecution(
        mockk<DescribeWorkflowExecutionRequest>(),
      )
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testSignalsAreNotRetried() {
    every { temporalWorkflowClient.signalWithStart(any()) } throws unavailable()
    every { temporalWorkflowClient.newSignalWithStartRequest() } returns mockk()
    Assertions.assertThrows(StatusRuntimeException::class.java) {
      val request = workflowClient.newSignalWithStartRequest()
      workflowClient.signalWithStart(request)
    }
    verify(exactly = 1) { temporalWorkflowClient.signalWithStart(any()) }
  }

  companion object {
    private const val MAX_ATTEMPT = 3
    private const val BACKOFF_DELAY_IN_MILLIS = 1
    private const val BACKOFF_MAX_DELAY_IN_MILLIS = 10

    private fun unavailable(): StatusRuntimeException = StatusRuntimeException(Status.UNAVAILABLE)
  }
}
