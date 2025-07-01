/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import com.google.common.annotations.VisibleForTesting
import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricClient
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.client.BatchRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.workflow.Functions.Proc1

/**
 * Wrapper around a temporal.client.WorkflowClient. The interface is a subset of the methods that we
 * used wrapped with specific handling for transient errors. The goal being to avoid spreading those
 * error handling across our codebase.
 */
class WorkflowClientWrapped
  @VisibleForTesting
  internal constructor(
    private val temporalWorkflowClient: WorkflowClient,
    metricClient: MetricClient,
    maxAttempt: Int,
    backoffDelayInMillis: Int,
    backoffMaxDelayInMillis: Int,
  ) {
    private val retryHelper =
      RetryHelper(metricClient, maxAttempt, backoffDelayInMillis, backoffMaxDelayInMillis)

    constructor(workflowClient: WorkflowClient, metricClient: MetricClient) : this(
      workflowClient,
      metricClient,
      RetryHelper.DEFAULT_MAX_ATTEMPT,
      RetryHelper.DEFAULT_BACKOFF_DELAY_IN_MILLIS,
      RetryHelper.DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS,
    )

    val namespace: String
      /**
       * Return the namespace of the temporal client.
       */
      get() = temporalWorkflowClient.options.namespace

    /**
     * Creates workflow client stub for a known execution. Use it to send signals or queries to a
     * running workflow.
     */
    fun <T> newWorkflowStub(
      workflowInterface: Class<T>?,
      workflowId: String?,
    ): T = withRetries({ temporalWorkflowClient.newWorkflowStub(workflowInterface, workflowId) }, "newWorkflowStub")

    /**
     * Creates workflow client stub for a known execution. Use it to send signals or queries to a
     * running workflow.
     */
    fun <T> newWorkflowStub(
      workflowInterface: Class<T>?,
      workflowOptions: WorkflowOptions?,
    ): T = withRetries({ temporalWorkflowClient.newWorkflowStub(workflowInterface, workflowOptions) }, "newWorkflowStub")

    /**
     * Returns information about the specified workflow execution.
     */
    fun blockingDescribeWorkflowExecution(request: DescribeWorkflowExecutionRequest?): DescribeWorkflowExecutionResponse =
      withRetries(
        { temporalWorkflowClient.workflowServiceStubs.blockingStub().describeWorkflowExecution(request) },
        "describeWorkflowExecution",
      )!!

    /**
     * Terminates a workflow execution. Termination is a hard stop of a workflow execution which doesn't
     * give workflow code any chance to perform cleanup.
     */
    fun terminateWorkflow(
      workflowId: String,
      reason: String,
    ) {
      withRetries<Unit>({ temporalWorkflowClient.newUntypedWorkflowStub(workflowId).terminate(reason) }, "terminate")
    }

    /**
     * Creates BatchRequest that can be used to signal an existing workflow or start a new one if not
     * running. The batch before invocation must contain exactly two operations. One annotated
     * with @WorkflowMethod and another with @SignalMethod.
     */
    fun newSignalWithStartRequest(): BatchRequest {
      // NOTE we do not retry here because signals are not idempotent
      return temporalWorkflowClient.newSignalWithStartRequest()
    }

    /**
     * Invoke SignalWithStart operation.
     */
    fun signalWithStart(batchRequest: BatchRequest): WorkflowExecution {
      // NOTE we do not retry here because signals are not idempotent
      return temporalWorkflowClient.signalWithStart(batchRequest)
    }

    /**
     * Asynchronously start a workflow.
     */
    fun <A1> start(
      workflow: Proc1<A1>?,
      a1: A1,
    ): WorkflowExecution = WorkflowClient.start(workflow, a1)

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
    ): T = retryHelper.withRetries(call, name)
  }
