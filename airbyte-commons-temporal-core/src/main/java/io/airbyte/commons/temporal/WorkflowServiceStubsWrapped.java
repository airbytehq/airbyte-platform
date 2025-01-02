/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import dev.failsafe.function.CheckedSupplier;
import io.airbyte.metrics.lib.MetricClient;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Wrapper around a temporal.client.WorkflowServiceStubs. The interface is a subset of the methods
 * that we used wrapped with specific handling for transient errors. The goal being to avoid
 * spreading those error handling across our codebase.
 */
public class WorkflowServiceStubsWrapped {

  private final WorkflowServiceStubs workflowServiceStubs;
  private final RetryHelper retryHelper;

  public WorkflowServiceStubsWrapped(final WorkflowServiceStubs workflowServiceStubs, final MetricClient metricClient) {
    this(workflowServiceStubs,
        metricClient,
        RetryHelper.DEFAULT_MAX_ATTEMPT,
        RetryHelper.DEFAULT_BACKOFF_DELAY_IN_MILLIS,
        RetryHelper.DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS);
  }

  public WorkflowServiceStubsWrapped(final WorkflowServiceStubs workflowServiceStubs,
                                     final MetricClient metricClient,
                                     final int maxAttempt,
                                     final int backoffDelayInMillis,
                                     final int backoffMaxDelayInMillis) {
    this.workflowServiceStubs = workflowServiceStubs;
    this.retryHelper = new RetryHelper(metricClient, maxAttempt, backoffDelayInMillis, backoffMaxDelayInMillis);
  }

  /**
   * ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific
   * namespace.
   */
  public ListClosedWorkflowExecutionsResponse blockingStubListClosedWorkflowExecutions(final ListClosedWorkflowExecutionsRequest request) {
    return withRetries(() -> workflowServiceStubs.blockingStub().listClosedWorkflowExecutions(request), "listClosedWorkflowExecutions");
  }

  /**
   * ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific
   * namespace.
   */
  public ListOpenWorkflowExecutionsResponse blockingStubListOpenWorkflowExecutions(final ListOpenWorkflowExecutionsRequest request) {
    return withRetries(() -> workflowServiceStubs.blockingStub().listOpenWorkflowExecutions(request), "listOpenWorkflowExecutions");
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
