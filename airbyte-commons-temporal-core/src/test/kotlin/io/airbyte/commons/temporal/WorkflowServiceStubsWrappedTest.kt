/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.metrics.MetricClient
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.mockk.every
import io.mockk.mockk
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub
import io.temporal.serviceclient.WorkflowServiceStubs
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class WorkflowServiceStubsWrappedTest {
  private lateinit var metricClient: MetricClient
  private lateinit var temporalWorkflowServiceStubs: WorkflowServiceStubs
  private lateinit var temporalWorkflowServiceBlockingStub: WorkflowServiceBlockingStub
  private lateinit var serviceStubsWrapped: WorkflowServiceStubsWrapped

  @BeforeEach
  fun beforeEach() {
    metricClient = mockk(relaxed = true)
    temporalWorkflowServiceBlockingStub = mockk()
    temporalWorkflowServiceStubs = mockk()
    every { temporalWorkflowServiceStubs.blockingStub() } returns temporalWorkflowServiceBlockingStub
    serviceStubsWrapped =
      WorkflowServiceStubsWrapped(
        temporalWorkflowServiceStubs,
        metricClient,
        MAX_ATTEMPT,
        BACKOFF_DELAY_IN_MILLIS,
        BACKOFF_MAX_DELAY_IN_MILLIS,
      )
  }

  @Test
  fun testListClosedWorkflowExecutions() {
    val request = ListClosedWorkflowExecutionsRequest.newBuilder().build()
    val response = ListClosedWorkflowExecutionsResponse.newBuilder().build()
    every { temporalWorkflowServiceBlockingStub.listClosedWorkflowExecutions(request) } throws unavailable() andThen response

    val actual = serviceStubsWrapped.blockingStubListClosedWorkflowExecutions(request)
    Assertions.assertEquals(response, actual)
  }

  @Test
  fun testListOpenWorkflowExecutions() {
    val request = ListOpenWorkflowExecutionsRequest.newBuilder().build()
    val response = ListOpenWorkflowExecutionsResponse.newBuilder().build()
    every { temporalWorkflowServiceBlockingStub.listOpenWorkflowExecutions(request) } throws unavailable() andThen response

    val actual = serviceStubsWrapped.blockingStubListOpenWorkflowExecutions(request)
    Assertions.assertEquals(response, actual)
  }

  companion object {
    private const val MAX_ATTEMPT = 3
    private const val BACKOFF_DELAY_IN_MILLIS = 1
    private const val BACKOFF_MAX_DELAY_IN_MILLIS = 10

    private fun unavailable(): StatusRuntimeException = StatusRuntimeException(Status.UNAVAILABLE)
  }
}
