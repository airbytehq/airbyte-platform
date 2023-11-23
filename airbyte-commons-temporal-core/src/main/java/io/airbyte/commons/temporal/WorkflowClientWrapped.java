/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import com.google.common.annotations.VisibleForTesting;
import dev.failsafe.function.CheckedSupplier;
import io.airbyte.metrics.lib.MetricClient;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.workflow.Functions.Proc1;

/**
 * Wrapper around a temporal.client.WorkflowClient. The interface is a subset of the methods that we
 * used wrapped with specific handling for transient errors. The goal being to avoid spreading those
 * error handling across our codebase.
 */
public class WorkflowClientWrapped {

  private final WorkflowClient temporalWorkflowClient;
  private final RetryHelper retryHelper;

  public WorkflowClientWrapped(final WorkflowClient workflowClient, final MetricClient metricClient) {
    this(workflowClient,
        metricClient,
        RetryHelper.DEFAULT_MAX_ATTEMPT,
        RetryHelper.DEFAULT_BACKOFF_DELAY_IN_MILLIS,
        RetryHelper.DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS);
  }

  @VisibleForTesting
  WorkflowClientWrapped(final WorkflowClient workflowClient,
                        final MetricClient metricClient,
                        final int maxAttempt,
                        final int backoffDelayInMillis,
                        final int backoffMaxDelayInMillis) {
    this.temporalWorkflowClient = workflowClient;
    this.retryHelper = new RetryHelper(metricClient, maxAttempt, backoffDelayInMillis, backoffMaxDelayInMillis);
  }

  /**
   * Return the namespace of the temporal client.
   */
  public String getNamespace() {
    return temporalWorkflowClient.getOptions().getNamespace();
  }

  /**
   * Creates workflow client stub for a known execution. Use it to send signals or queries to a
   * running workflow.
   */
  public <T> T newWorkflowStub(final Class<T> workflowInterface, final String workflowId) {
    return withRetries(() -> temporalWorkflowClient.newWorkflowStub(workflowInterface, workflowId), "newWorkflowStub");
  }

  /**
   * Creates workflow client stub for a known execution. Use it to send signals or queries to a
   * running workflow.
   */
  public <T> T newWorkflowStub(final Class<T> workflowInterface, final WorkflowOptions workflowOptions) {
    return withRetries(() -> temporalWorkflowClient.newWorkflowStub(workflowInterface, workflowOptions), "newWorkflowStub");
  }

  /**
   * Returns information about the specified workflow execution.
   */
  public DescribeWorkflowExecutionResponse blockingDescribeWorkflowExecution(final DescribeWorkflowExecutionRequest request) {
    return withRetries(() -> temporalWorkflowClient.getWorkflowServiceStubs().blockingStub().describeWorkflowExecution(request),
        "describeWorkflowExecution");
  }

  /**
   * Terminates a workflow execution. Termination is a hard stop of a workflow execution which doesn't
   * give workflow code any chance to perform cleanup.
   */
  public void terminateWorkflow(final String workflowId, final String reason) {
    withRetries(() -> {
      temporalWorkflowClient.newUntypedWorkflowStub(workflowId).terminate(reason);
      return null;
    },
        "terminate");
  }

  /**
   * Creates BatchRequest that can be used to signal an existing workflow or start a new one if not
   * running. The batch before invocation must contain exactly two operations. One annotated
   * with @WorkflowMethod and another with @SignalMethod.
   */
  public BatchRequest newSignalWithStartRequest() {
    // NOTE we do not retry here because signals are not idempotent
    return temporalWorkflowClient.newSignalWithStartRequest();
  }

  /**
   * Invoke SignalWithStart operation.
   */
  public WorkflowExecution signalWithStart(final BatchRequest batchRequest) {
    // NOTE we do not retry here because signals are not idempotent
    return temporalWorkflowClient.signalWithStart(batchRequest);
  }

  /**
   * Asynchronously start a workflow.
   */
  public <A1> WorkflowExecution start(final Proc1<A1> workflow, final A1 a1) {
    return WorkflowClient.start(workflow, a1);
  }

  /**
   * Where the magic happens.
   * <p>
   * We should only retry errors that are transient GRPC network errors.
   * <p>
   * We should only retry idempotent calls. The caller should be responsible for retrying creates to
   * avoid generating additional noise.
   */
  private <T> T withRetries(final CheckedSupplier<T> call, final String name) {
    return retryHelper.withRetries(call, name);
  }

}
