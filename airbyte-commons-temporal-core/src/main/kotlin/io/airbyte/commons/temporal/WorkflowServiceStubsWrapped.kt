/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricClient
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse
import io.temporal.serviceclient.WorkflowServiceStubs

/**
 * Wrapper around a temporal.client.WorkflowServiceStubs. The interface is a subset of the methods
 * that we used wrapped with specific handling for transient errors. The goal being to avoid
 * spreading those error handling across our codebase.
 */
class WorkflowServiceStubsWrapped
  @JvmOverloads
  constructor(
    private val workflowServiceStubs: WorkflowServiceStubs,
    metricClient: MetricClient,
    maxAttempt: Int = RetryHelper.DEFAULT_MAX_ATTEMPT,
    backoffDelayInMillis: Int = RetryHelper.DEFAULT_BACKOFF_DELAY_IN_MILLIS,
    backoffMaxDelayInMillis: Int = RetryHelper.DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS,
  ) {
    private val retryHelper =
      RetryHelper(metricClient, maxAttempt, backoffDelayInMillis, backoffMaxDelayInMillis)

    /**
     * ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific
     * namespace.
     */
    fun blockingStubListClosedWorkflowExecutions(request: ListClosedWorkflowExecutionsRequest?): ListClosedWorkflowExecutionsResponse =
      withRetries({
        workflowServiceStubs.blockingStub().listClosedWorkflowExecutions(request)
      }, "listClosedWorkflowExecutions")!!

    /**
     * ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific
     * namespace.
     */
    fun blockingStubListOpenWorkflowExecutions(request: ListOpenWorkflowExecutionsRequest?): ListOpenWorkflowExecutionsResponse =
      withRetries({
        workflowServiceStubs.blockingStub().listOpenWorkflowExecutions(request)
      }, "listOpenWorkflowExecutions")!!

    /**
     * Where the magic happens.
     *
     *
     * We should only retry errors that are transient GRPC network errors.
     *
     *
     * We should only retry idempotent calls. The caller should be responsible for retrying creates to
     * avoid generating additional noise.
     */
    private fun <T> withRetries(
      call: CheckedSupplier<T>,
      name: String,
    ): T? = retryHelper.withRetries(call, name)
  }
