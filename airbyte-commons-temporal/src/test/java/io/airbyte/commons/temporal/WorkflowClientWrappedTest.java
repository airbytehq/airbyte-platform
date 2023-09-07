/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.metrics.lib.MetricClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowClientWrappedTest {

  static class MyWorkflow {}

  private MetricClient metricClient;
  private WorkflowServiceStubs temporalWorkflowServiceStubs;
  private WorkflowServiceBlockingStub temporalWorkflowServiceBlockingStub;
  private WorkflowClient temporalWorkflowClient;
  private WorkflowClientWrapped workflowClient;

  @BeforeEach
  void beforeEach() {
    metricClient = mock(MetricClient.class);
    temporalWorkflowServiceBlockingStub = mock(WorkflowServiceBlockingStub.class);
    temporalWorkflowServiceStubs = mock(WorkflowServiceStubs.class);
    when(temporalWorkflowServiceStubs.blockingStub()).thenReturn(temporalWorkflowServiceBlockingStub);
    temporalWorkflowClient = mock(WorkflowClient.class);
    when(temporalWorkflowClient.getWorkflowServiceStubs()).thenReturn(temporalWorkflowServiceStubs);
    workflowClient = new WorkflowClientWrapped(temporalWorkflowClient, metricClient);
  }

  @Test
  void testRetryLogic() {
    when(temporalWorkflowClient.newWorkflowStub(any(), anyString()))
        .thenThrow(unavailable());

    assertThrows(StatusRuntimeException.class, () -> workflowClient.newWorkflowStub(MyWorkflow.class, "fail"));
    verify(temporalWorkflowClient, times(3)).newWorkflowStub(any(), anyString());
    verify(metricClient, times(2)).count(any(), anyLong(), any());
  }

  @Test
  void testNewWorkflowStub() {
    final MyWorkflow expected = new MyWorkflow();
    when(temporalWorkflowClient.newWorkflowStub(any(), anyString()))
        .thenThrow(unavailable())
        .thenReturn(expected);

    final MyWorkflow actual = workflowClient.newWorkflowStub(MyWorkflow.class, "woot");
    assertEquals(expected, actual);
  }

  @Test
  void testBlockingDescribeWorkflowExecution() {
    final DescribeWorkflowExecutionResponse expected = mock(DescribeWorkflowExecutionResponse.class);
    when(temporalWorkflowServiceBlockingStub.describeWorkflowExecution(any()))
        .thenThrow(unavailable())
        .thenReturn(expected);

    final DescribeWorkflowExecutionResponse actual = workflowClient.blockingDescribeWorkflowExecution(mock(DescribeWorkflowExecutionRequest.class));
    assertEquals(expected, actual);
  }

  private static StatusRuntimeException unavailable() {
    return new StatusRuntimeException(Status.UNAVAILABLE);
  }

}
