/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.metrics.lib.MetricClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowServiceStubsWrappedTest {

  private static final int maxAttempt = 3;
  private static final int backoffDelayInMillis = 1;
  private static final int backoffMaxDelayInMillis = 10;
  private MetricClient metricClient;
  private WorkflowServiceStubs temporalWorkflowServiceStubs;
  private WorkflowServiceBlockingStub temporalWorkflowServiceBlockingStub;
  private WorkflowServiceStubsWrapped serviceStubsWrapped;

  @BeforeEach
  void beforeEach() {
    metricClient = mock(MetricClient.class);
    temporalWorkflowServiceBlockingStub = mock(WorkflowServiceBlockingStub.class);
    temporalWorkflowServiceStubs = mock(WorkflowServiceStubs.class);
    when(temporalWorkflowServiceStubs.blockingStub()).thenReturn(temporalWorkflowServiceBlockingStub);
    serviceStubsWrapped = new WorkflowServiceStubsWrapped(temporalWorkflowServiceStubs, metricClient, maxAttempt, backoffDelayInMillis,
        backoffMaxDelayInMillis);
  }

  @Test
  void testListClosedWorkflowExecutions() {
    final var request = ListClosedWorkflowExecutionsRequest.newBuilder().build();
    final var response = ListClosedWorkflowExecutionsResponse.newBuilder().build();
    when(temporalWorkflowServiceBlockingStub.listClosedWorkflowExecutions(request))
        .thenThrow(unavailable())
        .thenReturn(response);

    final var actual = serviceStubsWrapped.blockingStubListClosedWorkflowExecutions(request);
    assertEquals(response, actual);
  }

  @Test
  void testListOpenWorkflowExecutions() {
    final var request = ListOpenWorkflowExecutionsRequest.newBuilder().build();
    final var response = ListOpenWorkflowExecutionsResponse.newBuilder().build();
    when(temporalWorkflowServiceBlockingStub.listOpenWorkflowExecutions(request))
        .thenThrow(unavailable())
        .thenReturn(response);

    final var actual = serviceStubsWrapped.blockingStubListOpenWorkflowExecutions(request);
    assertEquals(response, actual);
  }

  private static StatusRuntimeException unavailable() {
    return new StatusRuntimeException(Status.UNAVAILABLE);
  }

}
